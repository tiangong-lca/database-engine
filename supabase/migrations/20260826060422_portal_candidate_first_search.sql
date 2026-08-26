-- Issue #531 expand phase: install the synchronized public-safe Portal
-- projection, trigger writer, bounded backfill helper, and dormant Hybrid
-- kernel before the online index/cutover phases.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_projection_role_guard$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) or not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'api_internal_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'Portal projection executor role is missing or unsafe'
      using errcode = '42501';
  end if;
end
$portal_projection_role_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create or replace function private.catalog_portal_projection_payload_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
as $function$
declare
  v_card jsonb;
begin
  v_card := private.portal_catalog_card_v1(
    p_kind,
    p_state_code,
    p_json
  );
  if pg_catalog.jsonb_typeof(v_card) <> 'object' then
    return null;
  end if;
  return pg_catalog.jsonb_build_object(
    'card', v_card,
    'document', coalesce(v_card ->> 'document', '')
  );
end
$function$;

comment on function private.catalog_portal_projection_payload_v1(
  text, integer, jsonb
) is
  'Pure Portal public-card/document projection used only by the private synchronized search relation.';

revoke all on function private.catalog_portal_projection_payload_v1(
  text, integer, jsonb
) from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function private.catalog_portal_projection_payload_v1(
  text, integer, jsonb
) to api_internal_executor;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

create table private.portal_catalog_search_rows_v1 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null
    check (version ~ '^\d{2}\.\d{2}\.\d{3}$'),
  state_code integer not null
    check (state_code in (100, 200)),
  modified_at timestamptz not null,
  card jsonb not null
    check (pg_catalog.jsonb_typeof(card) = 'object'),
  document text not null,
  primary key (dataset_kind, id, version),
  check (coalesce(card ->> 'document', '') = document)
);

alter table private.portal_catalog_search_rows_v1 owner to postgres;
alter table private.portal_catalog_search_rows_v1 enable row level security;
alter table private.portal_catalog_search_rows_v1 force row level security;

create policy portal_catalog_search_rows_portal_select_v1
on private.portal_catalog_search_rows_v1
for select
to portal_public_executor
using (state_code in (100, 200));

create policy portal_catalog_search_rows_internal_all_v1
on private.portal_catalog_search_rows_v1
for all
to api_internal_executor
using (state_code in (100, 200))
with check (state_code in (100, 200));

revoke all on table private.portal_catalog_search_rows_v1
  from public, anon, authenticated, service_role;
grant select (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card,
  document
) on table private.portal_catalog_search_rows_v1
  to portal_public_executor;
grant select, insert, update, delete on table private.portal_catalog_search_rows_v1
  to api_internal_executor;

create index portal_catalog_search_rows_latest_v1_idx
  on private.portal_catalog_search_rows_v1 (
    dataset_kind,
    id,
    version desc,
    modified_at desc,
    state_code desc
  );

create or replace function private.sync_portal_catalog_search_row_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_kind text := case tg_table_name
    when 'processes' then 'process'
    when 'flows' then 'flow'
    else null
  end;
  v_root_key text := case v_kind
    when 'process' then 'processDataSet'
    when 'flow' then 'flowDataSet'
    else null
  end;
  v_payload jsonb;
begin
  if v_kind is null then
    raise exception 'unsupported Portal projection trigger source'
      using errcode = '55000';
  end if;

  if tg_op = 'DELETE' then
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
    return old;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
  end if;


  if new.state_code in (100, 200)
     and new.modified_at is not null
     and pg_catalog.jsonb_typeof(new.json) = 'object'
     and pg_catalog.jsonb_typeof(new.json -> v_root_key) = 'object' then
    v_payload := private.catalog_portal_projection_payload_v1(
      v_kind,
      new.state_code,
      new.json
    );
    if pg_catalog.jsonb_typeof(v_payload) <> 'object' then
      raise exception 'Portal projection payload is invalid'
        using errcode = '55000';
    end if;
    insert into private.portal_catalog_search_rows_v1 (
      dataset_kind,
      id,
      version,
      state_code,
      modified_at,
      card,
      document
    ) values (
      v_kind,
      new.id,
      new.version::text,
      new.state_code,
      new.modified_at,
      v_payload -> 'card',
      v_payload ->> 'document'
    )
    on conflict (dataset_kind, id, version) do update
    set state_code = excluded.state_code,
        modified_at = excluded.modified_at,
        card = excluded.card,
        document = excluded.document;
  else
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = new.id
      and projection.version = new.version::text;
  end if;
  return new;
end
$function$;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
alter function private.sync_portal_catalog_search_row_v1()
  owner to api_internal_executor;
set role api_internal_executor;
revoke all on function private.sync_portal_catalog_search_row_v1()
  from public, anon, authenticated, service_role,
    portal_public_executor, api_internal_executor;
grant execute on function private.sync_portal_catalog_search_row_v1()
  to postgres;
reset role;

create trigger portal_catalog_projection_content_sync_v1
after insert or delete or update of id, version, json, state_code, modified_at
on public.processes
for each row execute function private.sync_portal_catalog_search_row_v1('content');

create trigger portal_catalog_projection_content_sync_v1
after insert or delete or update of id, version, json, state_code, modified_at
on public.flows
for each row execute function private.sync_portal_catalog_search_row_v1('content');

set role api_internal_executor;
comment on function private.sync_portal_catalog_search_row_v1() is
  'NOLOGIN/NOBYPASSRLS writer maintains exact visible public-card rows; derivatives remain source-owned and do not touch the projection.';
revoke execute on function private.sync_portal_catalog_search_row_v1()
  from postgres;
reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

create or replace function private.backfill_portal_catalog_search_range_v1(
  p_lower uuid,
  p_upper uuid
)
returns jsonb
language plpgsql
set search_path = ''
set statement_timeout = '120s'
set row_security = 'on'
as $function$
declare
  v_process_rows integer;
  v_flow_rows integer;
begin
  if p_lower is null or (p_upper is not null and p_upper <= p_lower) then
    raise exception 'invalid Portal projection backfill range'
      using errcode = '22023';
  end if;

  insert into private.portal_catalog_search_rows_v1 (
    dataset_kind, id, version, state_code, modified_at,
    card, document
  )
  select
    'process',
    process.id,
    process.version::text,
    process.state_code,
    process.modified_at,
    payload.value -> 'card',
    payload.value ->> 'document'
  from public.processes as process
  cross join lateral (
    select private.catalog_portal_projection_payload_v1(
      'process', process.state_code, process.json
    ) as value
  ) as payload
  where process.id >= p_lower
    and (p_upper is null or process.id < p_upper)
    and process.state_code in (100, 200)
    and process.modified_at is not null
    and pg_catalog.jsonb_typeof(process.json) = 'object'
    and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    and pg_catalog.jsonb_typeof(payload.value) = 'object'
  on conflict (dataset_kind, id, version) do nothing;
  get diagnostics v_process_rows = row_count;

  insert into private.portal_catalog_search_rows_v1 (
    dataset_kind, id, version, state_code, modified_at,
    card, document
  )
  select
    'flow',
    flow.id,
    flow.version::text,
    flow.state_code,
    flow.modified_at,
    payload.value -> 'card',
    payload.value ->> 'document'
  from public.flows as flow
  cross join lateral (
    select private.catalog_portal_projection_payload_v1(
      'flow', flow.state_code, flow.json
    ) as value
  ) as payload
  where flow.id >= p_lower
    and (p_upper is null or flow.id < p_upper)
    and flow.state_code in (100, 200)
    and flow.modified_at is not null
    and pg_catalog.jsonb_typeof(flow.json) = 'object'
    and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
    and pg_catalog.jsonb_typeof(payload.value) = 'object'
  on conflict (dataset_kind, id, version) do nothing;
  get diagnostics v_flow_rows = row_count;

  return pg_catalog.jsonb_build_object(
    'processRows', v_process_rows,
    'flowRows', v_flow_rows
  );
end
$function$;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
alter function private.backfill_portal_catalog_search_range_v1(uuid, uuid)
  owner to api_internal_executor;
set role api_internal_executor;
revoke all on function private.backfill_portal_catalog_search_range_v1(uuid, uuid)
  from public, anon, authenticated, service_role,
    portal_public_executor, api_internal_executor;
grant execute on function private.backfill_portal_catalog_search_range_v1(uuid, uuid)
  to api_internal_executor;
reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

-- Install isolated source-HNSW semantic helpers and the future projection
-- Hybrid kernel while no existing API wrapper calls them. The later
-- transactional cutover changes only the public wrapper's private target.
grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create or replace function private.portal_projection_semantic_process_v1(
  p_query_embedding extensions.vector(1024)
)
returns table(
  id uuid,
  version text,
  semantic_distance double precision
)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'relaxed_order'
set hnsw.ef_search = '1000'
set hnsw.max_scan_tuples = '200000'
set hnsw.scan_mem_multiplier = '4'
set enable_sort = 'off'
set row_security = 'on'
as $function$
declare
  v_ids uuid[];
  v_versions text[];
  v_distances double precision[];
begin
  if p_query_embedding is null then
    raise exception using
      errcode = '22023',
      message = 'invalid portal semantic query';
  end if;

  select pg_catalog.array_agg(
      candidate.id
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.version
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.semantic_distance
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    )
  into v_ids, v_versions, v_distances
  from (
    select approximate.id,
      approximate.version,
      approximate.semantic_distance
    from (
      select process.id,
        process.version::text as version,
        process.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from public.processes as process
      where process.state_code in (100, 200)
        and process.embedding_ft is not null
        and exists (
          select 1
          from private.portal_catalog_search_rows_v1 as projection
          where projection.dataset_kind = 'process'
            and projection.id = process.id
            and projection.version = process.version::text
            and not exists (
              select 1
              from private.portal_catalog_search_rows_v1 as newer
              where newer.dataset_kind = projection.dataset_kind
                and newer.id = projection.id
                and (
                  newer.version > projection.version
                  or (
                    newer.version = projection.version
                    and newer.modified_at > projection.modified_at
                  )
                  or (
                    newer.version = projection.version
                    and newer.modified_at = projection.modified_at
                    and newer.state_code > projection.state_code
                  )
                )
            )
        )
      order by process.embedding_ft
        operator(extensions.<=>) p_query_embedding
      limit 5000
    ) as approximate
    where approximate.semantic_distance is not null
      and approximate.semantic_distance >= 0::double precision
    order by approximate.semantic_distance + 0::double precision,
      approximate.id,
      approximate.version desc
    limit 200
  ) as candidate;

  if coalesce(pg_catalog.cardinality(v_ids), 0) >= 200 then
    return query
    select v_ids[candidate.ordinal],
      v_versions[candidate.ordinal],
      v_distances[candidate.ordinal]
    from pg_catalog.generate_subscripts(v_ids, 1)
      as candidate(ordinal)
    where v_distances[candidate.ordinal] <= 0.5::double precision
    order by candidate.ordinal;
    return;
  end if;

  return query
  with eligible as materialized (
    select process.id,
      process.version::text as version,
      process.embedding_ft operator(extensions.<=>) p_query_embedding
        as semantic_distance
    from public.processes as process
    where process.state_code in (100, 200)
      and process.embedding_ft is not null
      and exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'process'
          and projection.id = process.id
          and projection.version = process.version::text
          and not exists (
            select 1
            from private.portal_catalog_search_rows_v1 as newer
            where newer.dataset_kind = projection.dataset_kind
              and newer.id = projection.id
              and (
                newer.version > projection.version
                or (
                  newer.version = projection.version
                  and newer.modified_at > projection.modified_at
                )
                or (
                  newer.version = projection.version
                  and newer.modified_at = projection.modified_at
                  and newer.state_code > projection.state_code
                )
              )
          )
      )
  )
  select eligible.id,
    eligible.version,
    eligible.semantic_distance
  from eligible
  where eligible.semantic_distance is not null
    and eligible.semantic_distance >= 0::double precision
    and eligible.semantic_distance <= 0.5::double precision
  order by eligible.semantic_distance,
    eligible.id,
    eligible.version desc
  limit 200;
end
$function$;

create or replace function private.portal_projection_semantic_flow_v1(
  p_query_embedding extensions.vector(1024)
)
returns table(
  id uuid,
  version text,
  semantic_distance double precision
)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'relaxed_order'
set hnsw.ef_search = '1000'
set hnsw.max_scan_tuples = '200000'
set hnsw.scan_mem_multiplier = '4'
set enable_sort = 'off'
set row_security = 'on'
as $function$
declare
  v_ids uuid[];
  v_versions text[];
  v_distances double precision[];
begin
  if p_query_embedding is null then
    raise exception using
      errcode = '22023',
      message = 'invalid portal semantic query';
  end if;

  select pg_catalog.array_agg(
      candidate.id
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.version
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.semantic_distance
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    )
  into v_ids, v_versions, v_distances
  from (
    select approximate.id,
      approximate.version,
      approximate.semantic_distance
    from (
      select flow.id,
        flow.version::text as version,
        flow.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from public.flows as flow
      where flow.state_code in (100, 200)
        and flow.embedding_ft is not null
        and exists (
          select 1
          from private.portal_catalog_search_rows_v1 as projection
          where projection.dataset_kind = 'flow'
            and projection.id = flow.id
            and projection.version = flow.version::text
            and not exists (
              select 1
              from private.portal_catalog_search_rows_v1 as newer
              where newer.dataset_kind = projection.dataset_kind
                and newer.id = projection.id
                and (
                  newer.version > projection.version
                  or (
                    newer.version = projection.version
                    and newer.modified_at > projection.modified_at
                  )
                  or (
                    newer.version = projection.version
                    and newer.modified_at = projection.modified_at
                    and newer.state_code > projection.state_code
                  )
                )
            )
        )
      order by flow.embedding_ft
        operator(extensions.<=>) p_query_embedding
      limit 5000
    ) as approximate
    where approximate.semantic_distance is not null
      and approximate.semantic_distance >= 0::double precision
    order by approximate.semantic_distance + 0::double precision,
      approximate.id,
      approximate.version desc
    limit 200
  ) as candidate;

  if coalesce(pg_catalog.cardinality(v_ids), 0) >= 200 then
    return query
    select v_ids[candidate.ordinal],
      v_versions[candidate.ordinal],
      v_distances[candidate.ordinal]
    from pg_catalog.generate_subscripts(v_ids, 1)
      as candidate(ordinal)
    where v_distances[candidate.ordinal] <= 0.5::double precision
    order by candidate.ordinal;
    return;
  end if;

  return query
  with eligible as materialized (
    select flow.id,
      flow.version::text as version,
      flow.embedding_ft operator(extensions.<=>) p_query_embedding
        as semantic_distance
    from public.flows as flow
    where flow.state_code in (100, 200)
      and flow.embedding_ft is not null
      and exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = flow.id
          and projection.version = flow.version::text
          and not exists (
            select 1
            from private.portal_catalog_search_rows_v1 as newer
            where newer.dataset_kind = projection.dataset_kind
              and newer.id = projection.id
              and (
                newer.version > projection.version
                or (
                  newer.version = projection.version
                  and newer.modified_at > projection.modified_at
                )
                or (
                  newer.version = projection.version
                  and newer.modified_at = projection.modified_at
                  and newer.state_code > projection.state_code
                )
              )
          )
      )
  )
  select eligible.id,
    eligible.version,
    eligible.semantic_distance
  from eligible
  where eligible.semantic_distance is not null
    and eligible.semantic_distance >= 0::double precision
    and eligible.semantic_distance <= 0.5::double precision
  order by eligible.semantic_distance,
    eligible.id,
    eligible.version desc
  limit 200;
end
$function$;

revoke all on function private.portal_projection_semantic_process_v1(
  extensions.vector
) from public, anon, authenticated, service_role,
  portal_public_executor, api_internal_executor;
grant execute on function private.portal_projection_semantic_process_v1(
  extensions.vector
) to api_internal_executor;
revoke all on function private.portal_projection_semantic_flow_v1(
  extensions.vector
) from public, anon, authenticated, service_role,
  portal_public_executor, api_internal_executor;
grant execute on function private.portal_projection_semantic_flow_v1(
  extensions.vector
) to api_internal_executor;

create or replace function private.portal_projection_semantic_candidates_v1(
  p_kind text,
  p_query_embedding extensions.vector(1024)
)
returns table(
  id uuid,
  version text,
  semantic_distance double precision
)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set row_security = 'on'
as $function$
begin
  if p_kind = 'process' then
    return query
    select candidate.*
    from private.portal_projection_semantic_process_v1(
      p_query_embedding
    ) as candidate;
  elsif p_kind = 'flow' then
    return query
    select candidate.*
    from private.portal_projection_semantic_flow_v1(
      p_query_embedding
    ) as candidate;
  end if;
end
$function$;

revoke all on function private.portal_projection_semantic_candidates_v1(
  text, extensions.vector
) from public, anon, authenticated, service_role,
  portal_public_executor, api_internal_executor;
grant execute on function private.portal_projection_semantic_candidates_v1(
  text, extensions.vector
) to api_internal_executor;

create or replace function private.portal_projection_hybrid_search_v1_impl(
  p_kind text,
  p_query_terms text[],
  p_query_embedding extensions.vector(1024),
  p_filters jsonb,
  p_limit integer,
  p_query_fingerprint text
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
set row_security = 'on'
as $function$
declare
  v_items jsonb;
  v_result jsonb;
begin
  with portal_lexical_matches as materialized (
    select match.id,
      match.version,
      match.term_ordinal
    from private.catalog_portal_hybrid_pattern_matches_v1(
      p_kind,
      p_query_terms
    ) as match
  ), portal_latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = p_kind
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), portal_lexical_counts as materialized (
    select portal_lexical_matches.id,
      portal_lexical_matches.version,
      pg_catalog.count(distinct portal_lexical_matches.term_ordinal)::integer
        as lexical_hit_count
    from portal_lexical_matches
    join portal_latest_keys
      on portal_latest_keys.id = portal_lexical_matches.id
     and portal_latest_keys.version = portal_lexical_matches.version
    group by portal_lexical_matches.id,
      portal_lexical_matches.version
  ), portal_lexical_candidates as materialized (
    select portal_lexical_counts.*
    from portal_lexical_counts
    where portal_lexical_counts.lexical_hit_count > 0
    order by portal_lexical_counts.lexical_hit_count desc,
      portal_lexical_counts.id asc,
      portal_lexical_counts.version desc
    limit 200
  ), portal_lexical_ranked as materialized (
    select portal_lexical_candidates.*,
      pg_catalog.row_number() over (
        order by portal_lexical_candidates.lexical_hit_count desc,
          portal_lexical_candidates.id asc,
          portal_lexical_candidates.version desc
      )::integer as lexical_rank
    from portal_lexical_candidates
  ), portal_semantic_candidates as materialized (
    select semantic.*
    from private.portal_projection_semantic_candidates_v1(
      p_kind,
      p_query_embedding
    ) as semantic
  ), portal_semantic_ranked as materialized (
    select portal_semantic_candidates.*,
      pg_catalog.row_number() over (
        order by portal_semantic_candidates.semantic_distance asc,
          portal_semantic_candidates.id asc,
          portal_semantic_candidates.version desc
      )::integer as semantic_rank
    from portal_semantic_candidates
  ), portal_fused as materialized (
    select
      coalesce(portal_lexical_ranked.id, portal_semantic_ranked.id) as id,
      coalesce(portal_lexical_ranked.version, portal_semantic_ranked.version)
        as version,
      portal_lexical_ranked.lexical_rank,
      portal_semantic_ranked.semantic_rank,
      portal_semantic_ranked.semantic_distance,
      pg_catalog.round(
        least(
          1::numeric,
          greatest(
            0::numeric,
            (
              coalesce(
                0.5::numeric / (60 + portal_lexical_ranked.lexical_rank),
                0::numeric
              )
              + coalesce(
                0.5::numeric / (60 + portal_semantic_ranked.semantic_rank),
                0::numeric
              )
            ) * 61::numeric
          )
        ),
        12
      ) as normalized_score
    from portal_lexical_ranked
    full outer join portal_semantic_ranked
      on portal_semantic_ranked.id = portal_lexical_ranked.id
     and portal_semantic_ranked.version = portal_lexical_ranked.version
  ), portal_fused_decorated as materialized (
    select portal_fused.*,
      projection.card,
      projection.state_code,
      projection.modified_at
    from portal_fused
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind
     and projection.id = portal_fused.id
     and projection.version = portal_fused.version
  ), portal_filtered as materialized (
    select portal_fused_decorated.*
    from portal_fused_decorated
    where (
        not (p_filters ? 'accessLevel')
        or portal_fused_decorated.card ->> 'accessLevel'
          = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card #>> '{geography,code}',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(coalesce(
            portal_fused_decorated.card -> 'classifications',
            '[]'::jsonb
          )) as classification(item)
          where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
            = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), portal_ordered as materialized (
    select portal_filtered.*
    from portal_filtered
    order by portal_filtered.normalized_score desc,
      portal_filtered.id asc,
      portal_filtered.version desc
    limit p_limit
  )
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', portal_ordered.id::text,
          'version', portal_ordered.version
        ),
        'accessLevel', portal_ordered.card -> 'accessLevel',
        'capabilities', portal_ordered.card -> 'capabilities',
        'names', portal_ordered.card -> 'names',
        'summary', portal_ordered.card -> 'summary',
        'geography', portal_ordered.card -> 'geography',
        'referenceYear', portal_ordered.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          portal_ordered.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', 'hybrid',
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'score', portal_ordered.normalized_score,
          'reasonCodes', pg_catalog.to_jsonb(pg_catalog.array_remove(array[
            case when portal_ordered.lexical_rank is not null
              then 'lexical_public_projection'::text end,
            case when portal_ordered.semantic_rank is not null
              then 'semantic_public_projection'::text end
          ], null)),
          'evidence', pg_catalog.jsonb_build_object(
            'lexicalRank', portal_ordered.lexical_rank,
            'semanticRank', portal_ordered.semantic_rank,
            'semanticDistance', case
              when portal_ordered.semantic_distance is null then null
              else pg_catalog.trim_scale(
                portal_ordered.semantic_distance::numeric
              )::text
            end
          )
        )
      )
      order by portal_ordered.normalized_score desc,
        portal_ordered.id asc,
        portal_ordered.version desc
    ),
    '[]'::jsonb
  )
  into v_items
  from portal_ordered;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-hybrid-candidate-page.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'items', v_items
  );
  if pg_catalog.octet_length(
    pg_catalog.convert_to(v_result::text, 'UTF8')
  ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_result;
end
$function$;

comment on function private.portal_projection_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) is
  'Portal-hybrid-rank-v1: projection PGroonga lexical candidates and bounded latest-visible source-HNSW candidates with exact underfill parity fuse before stored-card filters.';

revoke all on function private.portal_projection_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function private.portal_projection_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) to portal_public_executor;

reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

comment on table private.portal_catalog_search_rows_v1 is
  'Private synchronized, public-safe Portal card/document projection. Source embeddings and HNSW indexes remain authoritative and are not duplicated.';

commit;
