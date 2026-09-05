-- Database #624: authenticated Next Process/Flow Hybrid V2.
--
-- Public tg/co candidate scans execute as a fixed NOLOGIN, NOBYPASSRLS role
-- whose only table policy is state_code IN (100, 200). Actor-owned and
-- selected-team scans remain under the authenticated-inheriting internal
-- executor and re-check JWT-derived identity/team membership. Selective,
-- indexed populations of at most 2,000 exact versions use exact cosine;
-- broader and unsupported shapes retain strict iterative HNSW.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '10min';

select extensions.vector_dims('[1]'::extensions.vector);

do $next_hybrid_v2_prerequisite_guard$
declare
  v_missing_indexes text[];
begin
  select pg_catalog.array_agg(expected.name order by expected.name)
  into v_missing_indexes
  from (
    values
      ('next_hybrid_public_candidate_type_v2_idx'),
      ('next_hybrid_public_candidate_team_v2_idx'),
      ('next_hybrid_public_candidate_emission_v2_idx'),
      ('next_hybrid_public_candidate_classification_v2_idx'),
      ('next_hybrid_public_candidate_elementary_v2_idx')
  ) as expected(name)
  where not exists (
    select 1
    from pg_catalog.pg_class as relation
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = relation.relnamespace
    join pg_catalog.pg_index as index_catalog
      on index_catalog.indexrelid = relation.oid
    where namespace.nspname = 'private'
      and relation.relname = expected.name
      and index_catalog.indisvalid
      and index_catalog.indisready
      and index_catalog.indislive
  );

  if v_missing_indexes is not null
     or pg_catalog.to_regprocedure(
       'private.next_hybrid_json_codes_v2(jsonb,text)'
     ) is null
     or pg_catalog.to_regclass(
       'private.next_hybrid_public_candidates_v2'
     ) is null
     or not exists (
       select 1 from pg_catalog.pg_roles
       where rolname = 'api_internal_executor'
         and not rolcanlogin
         and not rolbypassrls
         and not rolsuper
     ) then
    raise exception 'Next Hybrid V2 prerequisites drifted: %',
      coalesce(v_missing_indexes::text, 'function-or-role')
      using errcode = '55000';
  end if;
end
$next_hybrid_v2_prerequisite_guard$;

do $next_public_search_executor_role$
begin
  if not exists (
    select 1 from pg_catalog.pg_roles
    where rolname = 'next_public_search_executor'
  ) then
    create role next_public_search_executor
      nologin
      noinherit
      nobypassrls
      nocreatedb
      nocreaterole;
  end if;
end
$next_public_search_executor_role$;

alter role next_public_search_executor
  nologin
  noinherit
  nobypassrls
  nocreatedb
  nocreaterole;

do $next_public_search_executor_attribute_guard$
begin
  if exists (
    select 1 from pg_catalog.pg_roles
    where rolname = 'next_public_search_executor'
      and (rolsuper or rolreplication)
  ) then
    raise exception 'next_public_search_executor has unsafe attributes'
      using errcode = '42501';
  end if;
end
$next_public_search_executor_attribute_guard$;

-- The sync triggers were committed by the projection migration before this
-- backfill starts. Conflict updates only advance older projection rows, so a
-- concurrent newer source write cannot be overwritten by the backfill.
grant api_internal_executor to postgres;
set local role api_internal_executor;

insert into private.next_hybrid_public_candidates_v2(
  dataset_kind, id, version, state_code, team_id, dataset_type,
  is_emission, classification_codes, elementary_codes, source_modified_at
)
select
  'process', source.id, source.version::text, source.state_code,
  source.team_id,
  source.json #>>
    '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}',
  false, '{}', '{}', source.modified_at
from public.processes as source
where source.state_code in (100, 200)
  and source.embedding_ft is not null
on conflict(dataset_kind, id, version) do update set
  state_code = excluded.state_code,
  team_id = excluded.team_id,
  dataset_type = excluded.dataset_type,
  is_emission = excluded.is_emission,
  classification_codes = excluded.classification_codes,
  elementary_codes = excluded.elementary_codes,
  source_modified_at = excluded.source_modified_at
where private.next_hybrid_public_candidates_v2.source_modified_at
  < excluded.source_modified_at;

insert into private.next_hybrid_public_candidates_v2(
  dataset_kind, id, version, state_code, team_id, dataset_type,
  is_emission, classification_codes, elementary_codes, source_modified_at
)
select
  'flow', source.id, source.version::text, source.state_code, source.team_id,
  source.json #>>
    '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}',
  source.json @>
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}',
  coalesce(private.next_hybrid_json_codes_v2(
    source.json #>
      '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
    '@classId'
  ), '{}'),
  coalesce(private.next_hybrid_json_codes_v2(
    source.json #>
      '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
    '@catId'
  ), '{}'),
  source.modified_at
from public.flows as source
where source.state_code in (100, 200)
  and source.embedding_ft is not null
on conflict(dataset_kind, id, version) do update set
  state_code = excluded.state_code,
  team_id = excluded.team_id,
  dataset_type = excluded.dataset_type,
  is_emission = excluded.is_emission,
  classification_codes = excluded.classification_codes,
  elementary_codes = excluded.elementary_codes,
  source_modified_at = excluded.source_modified_at
where private.next_hybrid_public_candidates_v2.source_modified_at
  < excluded.source_modified_at;

delete from private.next_hybrid_public_candidates_v2 as candidate
where not exists (
  select 1
  from public.processes as source
  where candidate.dataset_kind = 'process'
    and source.id = candidate.id
    and source.version::text = candidate.version
    and source.state_code in (100, 200)
    and source.embedding_ft is not null
)
and not exists (
  select 1
  from public.flows as source
  where candidate.dataset_kind = 'flow'
    and source.id = candidate.id
    and source.version::text = candidate.version
    and source.state_code in (100, 200)
    and source.embedding_ft is not null
);

analyze private.next_hybrid_public_candidates_v2;

do $next_hybrid_public_projection_completeness_guard$
declare
  v_kind text;
  v_projected bigint;
  v_expected bigint;
begin
  foreach v_kind in array array['process', 'flow'] loop
    select count(*) into v_projected
    from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = v_kind;

    if v_kind = 'process' then
      select count(*) into v_expected
      from public.processes
      where state_code in (100, 200) and embedding_ft is not null;
    else
      select count(*) into v_expected
      from public.flows
      where state_code in (100, 200) and embedding_ft is not null;
    end if;

    if v_projected <> v_expected then
      raise exception 'Next Hybrid V2 % projection count drifted', v_kind
        using errcode = '55000';
    end if;
  end loop;
end
$next_hybrid_public_projection_completeness_guard$;

reset role;

grant usage on schema public, private, api, extensions
  to next_public_search_executor;

grant select (
  dataset_kind, id, version, state_code, team_id, dataset_type,
  is_emission, classification_codes, elementary_codes, source_modified_at
) on table private.next_hybrid_public_candidates_v2
  to next_public_search_executor;

grant select (
  id, json, state_code, version, modified_at, team_id,
  embedding_ft, search_text, tableoid, ctid
) on table public.flows to next_public_search_executor;

grant select (
  id, json, state_code, version, modified_at, team_id,
  embedding_ft, search_text, tableoid, ctid
) on table public.processes to next_public_search_executor;

create policy next_public_search_executor_select_flows_v2
on public.flows
for select
to next_public_search_executor
using (state_code in (100, 200));

create policy next_public_search_executor_select_processes_v2
on public.processes
for select
to next_public_search_executor
using (state_code in (100, 200));

grant execute on function private.next_hybrid_json_codes_v2(jsonb, text)
  to next_public_search_executor, api_internal_executor;
grant execute on function private.dataset_alias_jsonb_array_v1(jsonb)
  to next_public_search_executor, api_internal_executor;
grant execute on function private.pgroonga_escape_query_terms(text[])
  to next_public_search_executor, api_internal_executor;

-- Temporary ADMIN-capable memberships are needed on hosted Supabase to
-- transfer function ownership to the two NOLOGIN execution roles. Revoke
-- both before commit.
grant next_public_search_executor, api_internal_executor to postgres;
grant create on schema private to next_public_search_executor, api_internal_executor;
grant create on schema api to api_internal_executor;

create function private.next_public_semantic_version_candidates_v2(
  p_kind text,
  p_query_embedding extensions.vector,
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_data_source text,
  p_team_id uuid
) returns table(
  rank bigint,
  id uuid,
  version text,
  distance double precision,
  semantic_route text,
  candidate_population integer
)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
set hnsw.ef_search = '200'
set hnsw.max_scan_tuples = '20000'
set hnsw.scan_mem_multiplier = '2'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_exact_cutover constant integer := 2000;
  v_state_code integer;
  v_table_name text;
  v_filter_sql text;
  v_exact_filter_sql text;
  v_projection_filter_sql text;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer := 0;
  v_indexed_probe boolean;
  v_route_population integer;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('tg', 'co')
     or p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024
     or pg_catalog.jsonb_typeof(v_residual) is distinct from 'object' then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;

  v_state_code := case p_data_source when 'tg' then 100 else 200 end;
  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  -- Typed public filters are already projected transactionally into the
  -- candidate sidecar.  Rechecking those JSON paths here made every exact
  -- Flow request pay the large dynamic-planning cost again.  Keep only the
  -- source-of-truth boundary and filters that are intentionally not projected.
  v_exact_filter_sql := $sql$
    source.embedding_ft is not null
    and source.state_code = $1
    and ($2::uuid is null or source.team_id = $2)
    and ($3::jsonb = '{}'::jsonb or source.json @> $3)
  $sql$;

  if p_kind = 'process' then
    v_filter_sql := $sql$
      source.embedding_ft is not null
      and source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        $4::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $4
      )
    $sql$;
    v_projection_filter_sql := $sql$
      candidate.dataset_kind = 'process'
      and candidate.state_code = $1
      and ($2::uuid is null or candidate.team_id = $2)
      and ($4::text is null or candidate.dataset_type = $4)
    $sql$;
    v_indexed_probe := p_team_id is not null or p_process_type is not null;
  else
    v_filter_sql := $sql$
      source.embedding_ft is not null
      and source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        pg_catalog.cardinality($5::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($5)
      )
      and (
        not coalesce($6::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($7::text[]) = 0
          and pg_catalog.cardinality($8::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $7
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $8
      )
    $sql$;
    v_projection_filter_sql := $sql$
      candidate.dataset_kind = 'flow'
      and candidate.state_code = $1
      and ($2::uuid is null or candidate.team_id = $2)
      and (
        pg_catalog.cardinality($5::text[]) = 0
        or candidate.dataset_type = any($5)
      )
      and (not coalesce($6::boolean, false) or not candidate.is_emission)
      and (
        (
          pg_catalog.cardinality($7::text[]) = 0
          and pg_catalog.cardinality($8::text[]) = 0
        )
        or candidate.classification_codes && $7
        or candidate.elementary_codes && $8
      )
    $sql$;
    v_indexed_probe := p_team_id is not null
      or pg_catalog.cardinality(v_flow_types) > 0
      or coalesce(p_as_input, false)
      or pg_catalog.cardinality(v_classification_codes) > 0
      or pg_catalog.cardinality(v_elementary_codes) > 0;
  end if;

  if v_indexed_probe then
    v_sql := pg_catalog.format($query$
      select
        pg_catalog.array_agg(candidate.id),
        pg_catalog.array_agg(candidate.version)
      from (
        select candidate.id, candidate.version
        from private.next_hybrid_public_candidates_v2 as candidate
        where %s
        limit $9
      ) as candidate
    $query$, v_projection_filter_sql);

    execute v_sql
      into v_candidate_ids, v_candidate_versions
      using v_state_code, p_team_id, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1;
    v_candidate_count := coalesce(pg_catalog.cardinality(v_candidate_ids), 0);
  end if;

  if v_indexed_probe and v_candidate_count <= v_exact_cutover then
    if v_candidate_count = 0 then
      return;
    end if;

    v_sql := pg_catalog.format($query$
      with candidate_keys as materialized (
        select
          ($10::uuid[])[ordinal] as id,
          ($11::text[])[ordinal] as version
        from pg_catalog.generate_subscripts($10::uuid[], 1) as key(ordinal)
      ), nearest as materialized (
        select
          source.id,
          source.version::text as version,
          source.embedding_ft operator(extensions.<=>) $12::extensions.vector as distance
        from candidate_keys as candidate
        join public.%I as source
          on source.id = candidate.id
         and source.version::text = candidate.version
        where %s
        order by
          (source.embedding_ft operator(extensions.<=>) $12::extensions.vector)
            + 0::double precision,
          source.id,
          source.version::text desc
        limit 200
      )
      select
        pg_catalog.row_number() over (
          order by nearest.distance + 0::double precision,
            nearest.id, nearest.version desc
        )::bigint,
        nearest.id,
        nearest.version,
        nearest.distance,
        'exact'::text,
        $13::integer
      from nearest
      order by nearest.distance + 0::double precision,
        nearest.id, nearest.version desc
    $query$, v_table_name, v_exact_filter_sql);

    return query execute v_sql
      using v_state_code, p_team_id, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1,
        v_candidate_ids, v_candidate_versions, p_query_embedding,
        v_candidate_count;
    return;
  end if;

  v_route_population := case when v_indexed_probe then v_candidate_count else null end;
  v_sql := pg_catalog.format($query$
    with nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) $10::extensions.vector as distance
      from public.%I as source
      where %s
      order by source.embedding_ft operator(extensions.<=>) $10::extensions.vector
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by nearest.distance + 0::double precision,
          nearest.id, nearest.version desc
      )::bigint,
      nearest.id,
      nearest.version,
      nearest.distance,
      'hnsw'::text,
      $11::integer
    from nearest
    order by nearest.distance + 0::double precision,
      nearest.id, nearest.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_state_code, p_team_id, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1,
      p_query_embedding, v_route_population;
end;
$function$;

alter function private.next_public_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, uuid
) owner to next_public_search_executor;
revoke all on function private.next_public_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, uuid
) from public, anon, authenticated, service_role;
grant execute on function private.next_public_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, uuid
) to api_internal_executor;

create function private.next_actor_semantic_version_candidates_v2(
  p_kind text,
  p_query_embedding extensions.vector,
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_data_source text,
  p_state_code integer,
  p_team_id uuid
) returns table(
  rank bigint,
  id uuid,
  version text,
  distance double precision,
  semantic_route text,
  candidate_population integer
)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
set hnsw.ef_search = '200'
set hnsw.max_scan_tuples = '20000'
set hnsw.scan_mem_multiplier = '2'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_exact_cutover constant integer := 2000;
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_table_name text;
  v_scope_sql text;
  v_filter_sql text;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer := 0;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('my', 'te')
     or p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024
     or pg_catalog.jsonb_typeof(v_residual) is distinct from 'object'
     or p_state_code < 0 then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  if v_actor is null then return; end if;
  if p_data_source = 'te' and (
    p_team_id is null
    or not private.dataset_search_can_read_team_filter(p_team_id, v_actor)
  ) then
    return;
  end if;

  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  v_scope_sql := case p_data_source
    when 'my' then 'source.user_id = $1 and ($3::integer is null or source.state_code = $3)'
    else 'source.team_id = $2 and ($3::integer is null or source.state_code = $3)'
  end;

  if p_kind = 'process' then
    v_filter_sql := v_scope_sql || $sql$
      and source.embedding_ft is not null
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        $5::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $5
      )
    $sql$;
  else
    v_filter_sql := v_scope_sql || $sql$
      and source.embedding_ft is not null
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        pg_catalog.cardinality($6::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($6)
      )
      and (
        not coalesce($7::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($8::text[]) = 0
          and pg_catalog.cardinality($9::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $8
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $9
      )
    $sql$;
  end if;

  v_sql := pg_catalog.format($query$
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    from (
      select source.id, source.version::text as version
      from public.%I as source
      where %s
      limit $10
    ) as candidate
  $query$, v_table_name, v_filter_sql);

  execute v_sql
    into v_candidate_ids, v_candidate_versions
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1;
  v_candidate_count := coalesce(pg_catalog.cardinality(v_candidate_ids), 0);

  if v_candidate_count <= v_exact_cutover then
    v_sql := pg_catalog.format($query$
      with candidate_keys as materialized (
        select
          ($11::uuid[])[ordinal] as id,
          ($12::text[])[ordinal] as version
        from pg_catalog.generate_subscripts($11::uuid[], 1) as key(ordinal)
      ), nearest as materialized (
        select
          source.id,
          source.version::text as version,
          source.embedding_ft operator(extensions.<=>) $13::extensions.vector as distance
        from candidate_keys as candidate
        join public.%I as source
          on source.id = candidate.id
         and source.version::text = candidate.version
        where %s
        order by
          (source.embedding_ft operator(extensions.<=>) $13::extensions.vector)
            + 0::double precision,
          source.id,
          source.version::text desc
        limit 200
      )
      select
        pg_catalog.row_number() over (
          order by nearest.distance + 0::double precision,
            nearest.id, nearest.version desc
        )::bigint,
        nearest.id,
        nearest.version,
        nearest.distance,
        'exact'::text,
        $14::integer
      from nearest
      order by nearest.distance + 0::double precision,
        nearest.id, nearest.version desc
    $query$, v_table_name, v_filter_sql);

    return query execute v_sql
      using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1,
        v_candidate_ids, v_candidate_versions, p_query_embedding,
        v_candidate_count;
    return;
  end if;

  v_sql := pg_catalog.format($query$
    with nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) $11::extensions.vector as distance
      from public.%I as source
      where %s
      order by source.embedding_ft operator(extensions.<=>) $11::extensions.vector
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by nearest.distance + 0::double precision,
          nearest.id, nearest.version desc
      )::bigint,
      nearest.id,
      nearest.version,
      nearest.distance,
      'hnsw'::text,
      $12::integer
    from nearest
    order by nearest.distance + 0::double precision,
      nearest.id, nearest.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1,
      p_query_embedding, v_candidate_count;
end;
$function$;

alter function private.next_actor_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) owner to api_internal_executor;
revoke all on function private.next_actor_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) from public, anon, authenticated, service_role;

create function private.next_semantic_version_candidates_v2(
  p_kind text,
  p_query_embedding extensions.vector,
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_data_source text,
  p_state_code integer,
  p_team_id uuid
) returns table(
  rank bigint,
  id uuid,
  version text,
  distance double precision,
  semantic_route text,
  candidate_population integer
)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
as $function$
begin
  if p_data_source in ('tg', 'co') then
    return query
    select candidate.*
    from private.next_public_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_team_id
    ) as candidate;
    return;
  end if;

  if p_data_source in ('my', 'te') then
    return query
    select candidate.*
    from private.next_actor_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate;
    return;
  end if;

  raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
end;
$function$;

alter function private.next_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) owner to api_internal_executor;
revoke all on function private.next_semantic_version_candidates_v2(
  text, extensions.vector, jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) from public, anon, authenticated, service_role;

create function private.next_public_lexical_version_candidates_v2(
  p_kind text,
  p_query text,
  p_terms text[],
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_data_source text,
  p_team_id uuid
) returns table(rank bigint, id uuid, version text, score double precision)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_state_code integer;
  v_table_name text;
  v_filter_sql text;
  v_terms text[];
  v_exact uuid;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow') or p_data_source not in ('tg', 'co') then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  v_state_code := case p_data_source when 'tg' then 100 else 200 end;
  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  v_terms := private.pgroonga_escape_query_terms(p_terms);
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then
    v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  end if;
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then return; end if;
  if pg_catalog.btrim(coalesce(p_query, '')) ~*
     '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact := pg_catalog.btrim(p_query)::uuid;
  end if;

  if p_kind = 'process' then
    v_filter_sql := $sql$
      source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        $4::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $4
      )
    $sql$;
  else
    v_filter_sql := $sql$
      source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        pg_catalog.cardinality($5::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($5)
      )
      and (
        not coalesce($6::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($7::text[]) = 0
          and pg_catalog.cardinality($8::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $7
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $8
      )
    $sql$;
  end if;

  v_sql := pg_catalog.format($query$
    with matched as materialized (
      select
        source.id,
        source.version::text as version,
        source.modified_at,
        case when $10::uuid is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid, source.ctid)::double precision
        end as search_score
      from public.%I as source
      where %s
        and (
          ($10::uuid is not null and source.id = $10)
          or ($10::uuid is null and source.search_text operator(extensions.&@~|) $9::text[])
        )
      order by search_score desc, source.modified_at desc,
        source.id, source.version desc
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by matched.search_score desc, matched.modified_at desc,
          matched.id, matched.version desc
      )::bigint,
      matched.id,
      matched.version,
      matched.search_score
    from matched
    order by matched.search_score desc, matched.modified_at desc,
      matched.id, matched.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_state_code, p_team_id, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_terms, v_exact;
end;
$function$;

alter function private.next_public_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, uuid
) owner to next_public_search_executor;
revoke all on function private.next_public_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, uuid
) from public, anon, authenticated, service_role;
grant execute on function private.next_public_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, uuid
) to api_internal_executor;

create function private.next_actor_lexical_version_candidates_v2(
  p_kind text,
  p_query text,
  p_terms text[],
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_data_source text,
  p_state_code integer,
  p_team_id uuid
) returns table(rank bigint, id uuid, version text, score double precision)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_table_name text;
  v_scope_sql text;
  v_filter_sql text;
  v_terms text[];
  v_exact uuid;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('my', 'te')
     or p_state_code < 0 then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  if v_actor is null then return; end if;
  if p_data_source = 'te' and (
    p_team_id is null
    or not private.dataset_search_can_read_team_filter(p_team_id, v_actor)
  ) then
    return;
  end if;

  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  v_scope_sql := case p_data_source
    when 'my' then 'source.user_id = $1 and ($3::integer is null or source.state_code = $3)'
    else 'source.team_id = $2 and ($3::integer is null or source.state_code = $3)'
  end;
  v_terms := private.pgroonga_escape_query_terms(p_terms);
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then
    v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  end if;
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then return; end if;
  if pg_catalog.btrim(coalesce(p_query, '')) ~*
     '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact := pg_catalog.btrim(p_query)::uuid;
  end if;

  if p_kind = 'process' then
    v_filter_sql := v_scope_sql || $sql$
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        $5::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $5
      )
    $sql$;
  else
    v_filter_sql := v_scope_sql || $sql$
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        pg_catalog.cardinality($6::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($6)
      )
      and (
        not coalesce($7::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($8::text[]) = 0
          and pg_catalog.cardinality($9::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $8
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $9
      )
    $sql$;
  end if;

  v_sql := pg_catalog.format($query$
    with matched as materialized (
      select
        source.id,
        source.version::text as version,
        source.modified_at,
        case when $11::uuid is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid, source.ctid)::double precision
        end as search_score
      from public.%I as source
      where %s
        and (
          ($11::uuid is not null and source.id = $11)
          or ($11::uuid is null and source.search_text operator(extensions.&@~|) $10::text[])
        )
      order by search_score desc, source.modified_at desc,
        source.id, source.version desc
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by matched.search_score desc, matched.modified_at desc,
          matched.id, matched.version desc
      )::bigint,
      matched.id,
      matched.version,
      matched.search_score
    from matched
    order by matched.search_score desc, matched.modified_at desc,
      matched.id, matched.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_terms, v_exact;
end;
$function$;

alter function private.next_actor_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) owner to api_internal_executor;
revoke all on function private.next_actor_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) from public, anon, authenticated, service_role;

create function private.next_lexical_version_candidates_v2(
  p_kind text,
  p_query text,
  p_terms text[],
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_data_source text,
  p_state_code integer,
  p_team_id uuid
) returns table(rank bigint, id uuid, version text, score double precision)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
as $function$
begin
  if p_data_source in ('tg', 'co') then
    return query
    select candidate.*
    from private.next_public_lexical_version_candidates_v2(
      p_kind, p_query, p_terms, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_team_id
    ) as candidate;
    return;
  end if;

  if p_data_source in ('my', 'te') then
    return query
    select candidate.*
    from private.next_actor_lexical_version_candidates_v2(
      p_kind, p_query, p_terms, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate;
    return;
  end if;

  raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
end;
$function$;

alter function private.next_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) owner to api_internal_executor;
revoke all on function private.next_lexical_version_candidates_v2(
  text, text, text[], jsonb, text, text[], boolean, text[], text[], text, integer, uuid
) from public, anon, authenticated, service_role;

create function private.next_hybrid_version_keys_v2(
  p_kind text,
  p_query_text text,
  p_query_terms text[],
  p_query_embedding extensions.vector,
  p_residual_filter jsonb,
  p_process_type text,
  p_flow_types text[],
  p_as_input boolean,
  p_classification_codes text[],
  p_elementary_codes text[],
  p_match_threshold double precision,
  p_lexical_weight double precision,
  p_semantic_weight double precision,
  p_rrf_k integer,
  p_data_source text,
  p_state_code integer,
  p_team_id uuid
) returns table(
  id uuid,
  version text,
  score double precision,
  semantic_route text,
  semantic_candidate_population integer,
  semantic_fallback_used boolean
)
language sql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
as $function$
  with lexical as materialized (
    select candidate.*
    from private.next_lexical_version_candidates_v2(
      p_kind, p_query_text, p_query_terms, p_residual_filter,
      p_process_type, p_flow_types, p_as_input, p_classification_codes,
      p_elementary_codes, p_data_source, p_state_code, p_team_id
    ) as candidate
    where p_lexical_weight > 0
  ), semantic_raw as materialized (
    select candidate.*
    from private.next_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate
    where p_semantic_weight > 0
  ), semantic_primary as materialized (
    select candidate.*
    from semantic_raw as candidate
    where candidate.distance < 1 - p_match_threshold
  ), semantic_meta as (
    select
      max(candidate.semantic_route) as semantic_route,
      max(candidate.candidate_population) as candidate_population
    from semantic_raw as candidate
  ), primary_fused as materialized (
    select
      coalesce(lexical.id, semantic.id) as id,
      coalesce(lexical.version, semantic.version) as version,
      coalesce(p_lexical_weight / (p_rrf_k + lexical.rank), 0::double precision)
        + coalesce(p_semantic_weight / (p_rrf_k + semantic.rank), 0::double precision)
          as score
    from lexical
    full outer join semantic_primary as semantic
      on semantic.id = lexical.id
     and semantic.version = lexical.version
  ), fallback_fused as materialized (
    select
      coalesce(lexical.id, semantic.id) as id,
      coalesce(lexical.version, semantic.version) as version,
      coalesce(p_lexical_weight / (p_rrf_k + lexical.rank), 0::double precision)
        + coalesce(p_semantic_weight / (p_rrf_k + semantic.rank), 0::double precision)
          as score
    from lexical
    full outer join semantic_raw as semantic
      on semantic.id = lexical.id
     and semantic.version = lexical.version
    where not exists (select 1 from primary_fused)
  ), selected as (
    select primary_fused.*, false as fallback_used from primary_fused
    union all
    select fallback_fused.*, true as fallback_used from fallback_fused
  )
  select
    selected.id,
    selected.version,
    selected.score,
    semantic_meta.semantic_route,
    semantic_meta.candidate_population,
    selected.fallback_used
  from selected
  cross join semantic_meta
  order by selected.score desc, selected.id, selected.version desc;
$function$;

alter function private.next_hybrid_version_keys_v2(
  text, text, text[], extensions.vector, jsonb, text, text[], boolean,
  text[], text[], double precision, double precision, double precision,
  integer, text, integer, uuid
) owner to api_internal_executor;
revoke all on function private.next_hybrid_version_keys_v2(
  text, text, text[], extensions.vector, jsonb, text, text[], boolean,
  text[], text[], double precision, double precision, double precision,
  integer, text, integer, uuid
) from public, anon, authenticated, service_role;

create function api.hybrid_search_process_versions_v2(
  query_text text,
  query_embedding text,
  filter_condition jsonb default '{}'::jsonb,
  match_threshold double precision default 0.5,
  match_count integer default 200,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg',
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null,
  state_code_filter integer default null,
  team_id_filter uuid default null,
  type_of_data_set_filter text default null
) returns table(
  id uuid,
  "json" jsonb,
  version character,
  modified_at timestamptz,
  model_id uuid,
  model_version character,
  team_id uuid,
  total_count bigint,
  semantic_route text,
  semantic_candidate_population integer,
  semantic_fallback_used boolean
)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)), ''), 'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_process_type text := nullif(pg_catalog.btrim(type_of_data_set_filter), '');
  v_query_embedding extensions.vector(1024);
begin
  if v_process_type = 'all' then v_process_type := null; end if;
  if v_source not in ('tg', 'co', 'my', 'te')
     or query_text is null or pg_catalog.btrim(query_text) = ''
     or match_count is distinct from 200
     or page_size is null or page_size not between 1 and 100
     or page_current is null or page_current not between 1 and 400
     or match_threshold is null or match_threshold not between 0 and 1
     or lexical_weight is null or lexical_weight not between 0 and 1
     or semantic_weight is null or semantic_weight not between 0 and 1
     or lexical_weight + semantic_weight <= 0
     or rrf_k is null or rrf_k not between 1 and 1000
     or pg_catalog.jsonb_typeof(filter_condition) is distinct from 'object'
     or state_code_filter < 0
     or (
       v_process_type is not null
       and v_process_type not in (
         'Unit process, single operation',
         'Unit process, black box',
         'LCI result',
         'Partly terminated system',
         'Avoided product system'
       )
     ) then
    raise exception using errcode = '22023', message = 'invalid Next Process Hybrid V2 request';
  end if;
  if v_source in ('my', 'te') and v_actor is null then return; end if;
  if v_source = 'te' and (
    team_id_filter is null
    or not private.dataset_search_can_read_team_filter(team_id_filter, v_actor)
  ) then
    return;
  end if;
  if (v_source = 'tg' and state_code_filter is not null and state_code_filter <> 100)
     or (v_source = 'co' and state_code_filter is not null and state_code_filter <> 200) then
    return;
  end if;

  v_query_embedding := query_embedding::extensions.vector(1024);

  return query
  with fused as materialized (
    select candidate.*
    from private.next_hybrid_version_keys_v2(
      'process', query_text, query_terms, v_query_embedding,
      filter_condition, v_process_type, '{}'::text[], false,
      '{}'::text[], '{}'::text[], match_threshold, lexical_weight,
      semantic_weight, rrf_k, v_source, state_code_filter, team_id_filter
    ) as candidate
  ), hydrated as materialized (
    select
      source.id,
      source.json,
      source.version,
      source.modified_at,
      source.model_id,
      source.model_version,
      source.team_id,
      fused.score,
      fused.semantic_route,
      fused.semantic_candidate_population,
      fused.semantic_fallback_used
    from fused
    join public.processes as source
      on source.id = fused.id
     and source.version::text = fused.version
    where (
        (v_source = 'tg' and source.state_code = 100
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'co' and source.state_code = 200
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'my' and source.user_id = v_actor
          and (state_code_filter is null or source.state_code = state_code_filter))
        or (v_source = 'te' and source.team_id = team_id_filter
          and (state_code_filter is null or source.state_code = state_code_filter)
          and private.dataset_search_can_read_team_filter(team_id_filter, v_actor))
      )
      and (filter_condition = '{}'::jsonb or source.json @> filter_condition)
      and (
        v_process_type is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = v_process_type
      )
  ), counted as (
    select hydrated.*, pg_catalog.count(*) over()::bigint as total_count
    from hydrated
  )
  select
    rows.id,
    rows.json,
    rows.version,
    rows.modified_at,
    rows.model_id,
    rows.model_version,
    rows.team_id,
    rows.total_count,
    rows.semantic_route,
    rows.semantic_candidate_population,
    rows.semantic_fallback_used
  from counted as rows
  order by rows.score desc, rows.id, rows.version desc
  limit page_size offset (page_current - 1) * page_size;
end;
$function$;

alter function api.hybrid_search_process_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid, text
) owner to api_internal_executor;
revoke all on function api.hybrid_search_process_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid, text
) from public, anon, authenticated, service_role;
grant execute on function api.hybrid_search_process_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid, text
) to authenticated;

create function api.hybrid_search_flow_versions_v2(
  query_text text,
  query_embedding text,
  filter_condition jsonb default '{}'::jsonb,
  match_threshold double precision default 0.5,
  match_count integer default 200,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg',
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null,
  state_code_filter integer default null,
  team_id_filter uuid default null
) returns table(
  id uuid,
  "json" jsonb,
  version character,
  modified_at timestamptz,
  team_id uuid,
  total_count bigint,
  semantic_route text,
  semantic_candidate_population integer,
  semantic_fallback_used boolean
)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)), ''), 'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_filter jsonb := coalesce(filter_condition, '{}'::jsonb);
  v_residual jsonb;
  v_flow_types text[] := '{}'::text[];
  v_as_input boolean := false;
  v_classification jsonb := '[]'::jsonb;
  v_classification_codes text[] := '{}'::text[];
  v_elementary_codes text[] := '{}'::text[];
  v_query_embedding extensions.vector(1024);
begin
  if v_source not in ('tg', 'co', 'my', 'te')
     or query_text is null or pg_catalog.btrim(query_text) = ''
     or match_count is distinct from 200
     or page_size is null or page_size not between 1 and 100
     or page_current is null or page_current not between 1 and 400
     or match_threshold is null or match_threshold not between 0 and 1
     or lexical_weight is null or lexical_weight not between 0 and 1
     or semantic_weight is null or semantic_weight not between 0 and 1
     or lexical_weight + semantic_weight <= 0
     or rrf_k is null or rrf_k not between 1 and 1000
     or pg_catalog.jsonb_typeof(v_filter) is distinct from 'object'
     or state_code_filter < 0 then
    raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
  end if;

  if v_filter ? 'flowType' then
    if pg_catalog.jsonb_typeof(v_filter -> 'flowType') <> 'string' then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
    select coalesce(pg_catalog.array_agg(value order by value), '{}'::text[])
    into v_flow_types
    from (
      select distinct nullif(pg_catalog.btrim(item), '') as value
      from pg_catalog.regexp_split_to_table(v_filter ->> 'flowType', ',') as item
    ) as values
    where value is not null;
    if pg_catalog.cardinality(v_flow_types) = 0 or exists (
      select 1 from pg_catalog.unnest(v_flow_types) as value
      where value not in ('Elementary flow', 'Product flow', 'Waste flow', 'Other flow')
    ) then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
  end if;

  if v_filter ? 'asInput' then
    if pg_catalog.jsonb_typeof(v_filter -> 'asInput') = 'boolean'
       or (
         pg_catalog.jsonb_typeof(v_filter -> 'asInput') = 'string'
         and pg_catalog.lower(v_filter ->> 'asInput') in ('true', 'false')
       ) then
      v_as_input := (v_filter ->> 'asInput')::boolean;
    else
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
  end if;

  if v_filter ? 'classification' then
    if pg_catalog.jsonb_typeof(v_filter -> 'classification') <> 'array'
       or pg_catalog.jsonb_array_length(v_filter -> 'classification') > 50
       or exists (
         select 1
         from pg_catalog.jsonb_array_elements(v_filter -> 'classification') as selected(item)
         where pg_catalog.jsonb_typeof(selected.item) <> 'object'
           or selected.item ->> 'scope' not in ('classification', 'elementary')
           or nullif(pg_catalog.btrim(selected.item ->> 'code'), '') is null
           or pg_catalog.length(selected.item ->> 'code') > 200
       ) then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
    v_classification := v_filter -> 'classification';
    select
      coalesce(pg_catalog.array_agg(distinct pg_catalog.btrim(selected.item ->> 'code'))
        filter (where selected.item ->> 'scope' = 'classification'), '{}'::text[]),
      coalesce(pg_catalog.array_agg(distinct pg_catalog.btrim(selected.item ->> 'code'))
        filter (where selected.item ->> 'scope' = 'elementary'), '{}'::text[])
    into v_classification_codes, v_elementary_codes
    from pg_catalog.jsonb_array_elements(v_classification) as selected(item);
  end if;

  v_residual := v_filter - 'flowType' - 'asInput' - 'classification';

  if v_source in ('my', 'te') and v_actor is null then return; end if;
  if v_source = 'te' and (
    team_id_filter is null
    or not private.dataset_search_can_read_team_filter(team_id_filter, v_actor)
  ) then
    return;
  end if;
  if (v_source = 'tg' and state_code_filter is not null and state_code_filter <> 100)
     or (v_source = 'co' and state_code_filter is not null and state_code_filter <> 200) then
    return;
  end if;

  v_query_embedding := query_embedding::extensions.vector(1024);

  return query
  with fused as materialized (
    select candidate.*
    from private.next_hybrid_version_keys_v2(
      'flow', query_text, query_terms, v_query_embedding,
      v_residual, null, v_flow_types, v_as_input,
      v_classification_codes, v_elementary_codes, match_threshold,
      lexical_weight, semantic_weight, rrf_k, v_source,
      state_code_filter, team_id_filter
    ) as candidate
  ), hydrated as materialized (
    select
      source.id,
      source.json,
      source.version,
      source.modified_at,
      source.team_id,
      fused.score,
      fused.semantic_route,
      fused.semantic_candidate_population,
      fused.semantic_fallback_used
    from fused
    join public.flows as source
      on source.id = fused.id
     and source.version::text = fused.version
    where (
        (v_source = 'tg' and source.state_code = 100
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'co' and source.state_code = 200
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'my' and source.user_id = v_actor
          and (state_code_filter is null or source.state_code = state_code_filter))
        or (v_source = 'te' and source.team_id = team_id_filter
          and (state_code_filter is null or source.state_code = state_code_filter)
          and private.dataset_search_can_read_team_filter(team_id_filter, v_actor))
      )
      and (v_residual = '{}'::jsonb or source.json @> v_residual)
      and (
        pg_catalog.cardinality(v_flow_types) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any(v_flow_types)
      )
      and (
        not v_as_input
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality(v_classification_codes) = 0
          and pg_catalog.cardinality(v_elementary_codes) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && v_classification_codes
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && v_elementary_codes
      )
  ), counted as (
    select hydrated.*, pg_catalog.count(*) over()::bigint as total_count
    from hydrated
  )
  select
    rows.id,
    rows.json,
    rows.version,
    rows.modified_at,
    rows.team_id,
    rows.total_count,
    rows.semantic_route,
    rows.semantic_candidate_population,
    rows.semantic_fallback_used
  from counted as rows
  order by rows.score desc, rows.id, rows.version desc
  limit page_size offset (page_current - 1) * page_size;
end;
$function$;

alter function api.hybrid_search_flow_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid
) owner to api_internal_executor;
revoke all on function api.hybrid_search_flow_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid
) from public, anon, authenticated, service_role;
grant execute on function api.hybrid_search_flow_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid
) to authenticated;

insert into private.api_capability_grants(
  routine_identity, capability_id,
  allow_anon, allow_authenticated, allow_service_role
) values
  (
    'api.hybrid_search_process_versions_v2(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid, text)',
    'NX-CORE-02', false, true, false
  ),
  (
    'api.hybrid_search_flow_versions_v2(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)',
    'NX-CORE-02', false, true, false
  );

comment on function api.hybrid_search_process_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid, text
) is
  'Authenticated exact-version Next Process Hybrid V2 with canonical type/state/team filters, bounded exact routing, strict HNSW fallback, and single-call threshold fallback.';

comment on function api.hybrid_search_flow_versions_v2(
  text, text, jsonb, double precision, integer, double precision,
  double precision, integer, text, integer, integer, text[], integer, uuid
) is
  'Authenticated exact-version Next Flow Hybrid V2 with canonical classification/type/state/team filters, bounded exact routing, strict HNSW fallback, and single-call threshold fallback.';

revoke create on schema private from next_public_search_executor, api_internal_executor;
revoke create on schema api from api_internal_executor;
revoke next_public_search_executor, api_internal_executor from postgres;

do $verify_next_hybrid_v2_security$
begin
  if not exists (
    select 1 from pg_catalog.pg_roles
    where rolname = 'next_public_search_executor'
      and not rolsuper and not rolinherit and not rolcreaterole
      and not rolcreatedb and not rolcanlogin and not rolreplication
      and not rolbypassrls
  ) then raise exception 'Next Hybrid V2 executor attributes failed' using errcode = '55000';
  end if;
  if exists (
    select 1 from pg_catalog.pg_auth_members as membership
    where (
      membership.member = 'next_public_search_executor'::regrole
      or membership.roleid = 'next_public_search_executor'::regrole
    )
      and not (
        membership.roleid = 'next_public_search_executor'::regrole
        and membership.member = 'postgres'::regrole
        and membership.admin_option
        and not membership.inherit_option
        and not membership.set_option
      )
  ) then raise exception 'Next Hybrid V2 executor membership failed' using errcode = '55000';
  end if;
  if not exists (
    select 1 from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.next_public_semantic_version_candidates_v2(text,extensions.vector,jsonb,text,text[],boolean,text[],text[],text,uuid)'::regprocedure
      and routine.proowner = 'next_public_search_executor'::regrole
      and routine.prosecdef
  ) then raise exception 'Next Hybrid V2 semantic owner failed' using errcode = '55000';
  end if;
  if not exists (
    select 1 from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.next_public_lexical_version_candidates_v2(text,text,text[],jsonb,text,text[],boolean,text[],text[],text,uuid)'::regprocedure
      and routine.proowner = 'next_public_search_executor'::regrole
      and routine.prosecdef
  ) then raise exception 'Next Hybrid V2 lexical owner failed' using errcode = '55000';
  end if;
  if pg_catalog.has_table_privilege(
    'next_public_search_executor', 'public.flows',
    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ) or pg_catalog.has_table_privilege(
    'next_public_search_executor', 'public.processes',
    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ) then raise exception 'Next Hybrid V2 executor write privilege failed' using errcode = '55000';
  end if;
  if pg_catalog.has_function_privilege(
    'anon',
    'api.hybrid_search_flow_versions_v2(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)',
    'EXECUTE'
  ) then raise exception 'Next Hybrid V2 anon ACL failed' using errcode = '55000';
  end if;
  if not pg_catalog.has_function_privilege(
    'authenticated',
    'api.hybrid_search_flow_versions_v2(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)',
    'EXECUTE'
  ) then raise exception 'Next Hybrid V2 authenticated ACL failed' using errcode = '55000';
  end if;
end
$verify_next_hybrid_v2_security$;

notify pgrst, 'reload schema';

commit;
