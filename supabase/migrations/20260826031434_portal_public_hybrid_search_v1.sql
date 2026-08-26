begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

create temporary table portal_hybrid_legacy_before (
  routine_identity text primary key,
  definition text not null,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit drop;

insert into portal_hybrid_legacy_before (
  routine_identity,
  definition,
  owner_name,
  security_definer,
  proconfig,
  acl_text
)
with expected(routine_identity) as (
  values
    ('api.hybrid_search_processes(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('api.hybrid_search_flows(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('api.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('api.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('private.semantic_process_candidates(text,text,double precision,integer,text)'),
    ('private.semantic_flow_candidates(text,text,double precision,integer,text)')
)
select
  expected.routine_identity,
  pg_catalog.pg_get_functiondef(routine.oid),
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from expected
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner;

do $portal_hybrid_legacy_snapshot_guard$
begin
  if (select count(*) from portal_hybrid_legacy_before) <> 8 then
    raise exception 'Portal Hybrid legacy routine snapshot is incomplete';
  end if;
end
$portal_hybrid_legacy_snapshot_guard$;

do $portal_hybrid_executor_guard$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'portal_public_executor is missing or unsafe'
      using errcode = '42501';
  end if;
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'api_internal_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'api_internal_executor is missing or unsafe'
      using errcode = '42501';
  end if;
end
$portal_hybrid_executor_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set local role portal_public_executor;

create function private.portal_public_hybrid_input_v1(
  p_kind text,
  p_query_terms text[],
  p_query_embedding text,
  p_filters jsonb,
  p_limit integer
)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path = ''
as $function$
declare
  v_terms text[];
  v_term text;
  v_filters jsonb;
  v_key text;
  v_year numeric;
  v_embedding extensions.vector(1024);
  v_embedding_components text[];
  v_embedding_text text;
  v_embedding_sha256 text;
  v_fingerprint text;
begin
  if p_kind is null
     or p_kind not in ('process', 'flow')
     or p_limit is null
     or p_limit not between 1 and 20
     or p_query_terms is null
     or pg_catalog.array_ndims(p_query_terms) <> 1
     or pg_catalog.cardinality(p_query_terms) not between 1 and 12
     or exists (
       select 1
       from pg_catalog.unnest(p_query_terms) as supplied(term)
       where supplied.term is null
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  select pg_catalog.array_agg(
    pg_catalog.lower(
      pg_catalog.btrim(supplied.term) collate pg_catalog."und-x-icu"
    )
    order by supplied.ordinality
  )
  into v_terms
  from pg_catalog.unnest(p_query_terms) with ordinality as supplied(term, ordinality);

  foreach v_term in array v_terms
  loop
    if pg_catalog.char_length(v_term) not between 1 and 512
       or pg_catalog.octet_length(v_term) > 2048
       or exists (
         select 1
         from pg_catalog.generate_series(1, pg_catalog.char_length(v_term)) as position(value)
         where pg_catalog.ascii(pg_catalog.substr(v_term, position.value, 1))
           between 0 and 31
            or pg_catalog.ascii(pg_catalog.substr(v_term, position.value, 1))
              between 127 and 159
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  if (
    select count(distinct supplied.term)
    from pg_catalog.unnest(v_terms) as supplied(term)
  ) <> pg_catalog.cardinality(v_terms) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  if p_query_embedding is null
     or pg_catalog.octet_length(p_query_embedding) > 65536
     or pg_catalog.left(p_query_embedding, 1) <> '['
     or pg_catalog.right(p_query_embedding, 1) <> ']' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_embedding_components := pg_catalog.string_to_array(
    pg_catalog.substr(p_query_embedding, 2, pg_catalog.char_length(p_query_embedding) - 2),
    ','
  );
  if pg_catalog.cardinality(v_embedding_components) <> 1024
     or exists (
       select 1
       from pg_catalog.unnest(v_embedding_components) as component(value)
       where component.value
         !~ '^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$'
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  begin
    v_embedding := p_query_embedding::extensions.vector(1024);
  exception
    when others then
      raise exception using errcode = '22023', message = 'invalid portal request';
  end;
  if extensions.vector_dims(v_embedding) <> 1024 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_embedding_text := v_embedding::text;
  v_embedding_sha256 := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_embedding_text, 'UTF8'), 'sha256'),
    'hex'
  );

  if p_filters is null or pg_catalog.jsonb_typeof(p_filters) <> 'object' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if exists (
    select 1
    from pg_catalog.jsonb_object_keys(p_filters) as supplied(key)
    where supplied.key not in (
      'accessLevel', 'geography', 'classification', 'referenceYearFrom',
      'referenceYearTo', 'processSubtype', 'source'
    )
  ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  select coalesce(
    pg_catalog.jsonb_object_agg(
      supplied.key,
      case
        when supplied.key in (
          'geography', 'classification', 'processSubtype', 'source'
        ) and pg_catalog.jsonb_typeof(supplied.value) = 'string'
          then pg_catalog.to_jsonb(pg_catalog.lower(
            pg_catalog.btrim(supplied.value #>> '{}') collate pg_catalog."und-x-icu"
          ))
        else supplied.value
      end
      order by supplied.key
    ),
    '{}'::jsonb
  )
  into v_filters
  from pg_catalog.jsonb_each(p_filters) as supplied(key, value);
  if pg_catalog.octet_length(pg_catalog.convert_to(v_filters::text, 'UTF8')) > 4096
     or (p_kind = 'flow' and v_filters ? 'processSubtype')
     or (
       v_filters ? 'accessLevel'
       and (
         pg_catalog.jsonb_typeof(v_filters -> 'accessLevel') <> 'string'
         or v_filters ->> 'accessLevel' not in ('open', 'metadata_only')
       )
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  foreach v_key in array array['geography', 'classification', 'processSubtype', 'source']
  loop
    if v_filters ? v_key
       and (
         pg_catalog.jsonb_typeof(v_filters -> v_key) <> 'string'
         or pg_catalog.char_length(v_filters ->> v_key) not between 1 and 128
         or pg_catalog.octet_length(v_filters ->> v_key) > 1024
         or exists (
           select 1
           from pg_catalog.generate_series(
             1,
             pg_catalog.char_length(v_filters ->> v_key)
           ) as position(value)
           where pg_catalog.ascii(
             pg_catalog.substr(v_filters ->> v_key, position.value, 1)
           ) between 0 and 31
              or pg_catalog.ascii(
                pg_catalog.substr(v_filters ->> v_key, position.value, 1)
              ) between 127 and 159
         )
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;

  foreach v_key in array array['referenceYearFrom', 'referenceYearTo']
  loop
    if v_filters ? v_key then
      if pg_catalog.jsonb_typeof(v_filters -> v_key) <> 'number' then
        raise exception using errcode = '22023', message = 'invalid portal request';
      end if;
      v_year := (v_filters ->> v_key)::numeric;
      if v_year <> pg_catalog.trunc(v_year) or v_year not between 0 and 9999 then
        raise exception using errcode = '22023', message = 'invalid portal request';
      end if;
    end if;
  end loop;
  if v_filters ? 'referenceYearFrom'
     and v_filters ? 'referenceYearTo'
     and (v_filters ->> 'referenceYearFrom')::numeric
       > (v_filters ->> 'referenceYearTo')::numeric then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  v_fingerprint := pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.jsonb_build_object(
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'kind', p_kind,
          'queryTerms', pg_catalog.to_jsonb(v_terms),
          'queryEmbeddingSha256', v_embedding_sha256,
          'filters', v_filters,
          'limit', p_limit
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  return pg_catalog.jsonb_build_object(
    'kind', p_kind,
    'queryTerms', pg_catalog.to_jsonb(v_terms),
    'queryEmbedding', v_embedding_text,
    'filters', v_filters,
    'limit', p_limit,
    'queryFingerprint', v_fingerprint
  );
end
$function$;

create function private.portal_public_hybrid_card_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language sql
stable
parallel restricted
security definer
set search_path = ''
as $function$
  select private.portal_catalog_card_v1(p_kind, p_state_code, p_json)
$function$;

comment on function private.portal_public_hybrid_input_v1(text, text[], text, jsonb, integer) is
  'Owner-only normalization and fingerprinting for the fixed Portal Database Hybrid request.';
comment on function private.portal_public_hybrid_card_v1(text, integer, jsonb) is
  'Constrained bridge to the existing Portal public-card projector; callable only by api_internal_executor.';

revoke all on function private.portal_public_hybrid_input_v1(text, text[], text, jsonb, integer)
  from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.portal_public_hybrid_card_v1(text, integer, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function private.portal_public_hybrid_card_v1(text, integer, jsonb)
  to api_internal_executor;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set local role api_internal_executor;

create function private.portal_public_hybrid_search_v1_impl(
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
as $function$
declare
  v_items jsonb;
  v_result jsonb;
begin
  with source_rows as materialized (
    select
      process.id,
      process.version::text as version,
      process.json as json_data,
      process.state_code,
      process.modified_at,
      process.embedding_ft
    from public.processes as process
    where p_kind = 'process'
      and process.state_code in (100, 200)
      and process.modified_at is not null
      and pg_catalog.jsonb_typeof(process.json) = 'object'
      and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    union all
    select
      flow.id,
      flow.version::text as version,
      flow.json as json_data,
      flow.state_code,
      flow.modified_at,
      flow.embedding_ft
    from public.flows as flow
    where p_kind = 'flow'
      and flow.state_code in (100, 200)
      and flow.modified_at is not null
      and pg_catalog.jsonb_typeof(flow.json) = 'object'
      and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
  ), latest_ranked as materialized (
    select source_rows.*,
      pg_catalog.row_number() over (
        partition by source_rows.id
        order by source_rows.version desc, source_rows.modified_at desc, source_rows.state_code desc
      ) as latest_rank
    from source_rows
  ), latest as materialized (
    select latest_ranked.*
    from latest_ranked
    where latest_ranked.latest_rank = 1
  ), decorated as materialized (
    select latest.*,
      private.portal_public_hybrid_card_v1(
        p_kind,
        latest.state_code,
        latest.json_data
      ) as card
    from latest
  ), lexical_counts as materialized (
    select decorated.*,
      (
        select count(*)::integer
        from pg_catalog.unnest(p_query_terms) as query_term(term)
        where pg_catalog.strpos(
          lower(coalesce(decorated.card ->> 'document', '')),
          query_term.term
        ) > 0
      ) as lexical_hit_count
    from decorated
    where decorated.card is not null
  ), lexical_candidates as materialized (
    select lexical_counts.*
    from lexical_counts
    where lexical_counts.lexical_hit_count > 0
    order by lexical_counts.lexical_hit_count desc,
      lexical_counts.id asc,
      lexical_counts.version desc
    limit 200
  ), lexical_ranked as materialized (
    select lexical_candidates.id,
      lexical_candidates.version,
      pg_catalog.row_number() over (
        order by lexical_candidates.lexical_hit_count desc,
          lexical_candidates.id asc,
          lexical_candidates.version desc
      )::integer as lexical_rank
    from lexical_candidates
  ), semantic_distances as materialized (
    select latest.id,
      latest.version,
      (
        latest.embedding_ft operator(extensions.<=>) p_query_embedding
      ) as semantic_distance
    from latest
    where latest.embedding_ft is not null
  ), semantic_candidates as materialized (
    select semantic_distances.*
    from semantic_distances
    where semantic_distances.semantic_distance is not null
      and semantic_distances.semantic_distance >= 0::double precision
      and semantic_distances.semantic_distance <= 0.5::double precision
    order by semantic_distances.semantic_distance asc,
      semantic_distances.id asc,
      semantic_distances.version desc
    limit 200
  ), semantic_ranked as materialized (
    select semantic_candidates.id,
      semantic_candidates.version,
      semantic_candidates.semantic_distance,
      pg_catalog.row_number() over (
        order by semantic_candidates.semantic_distance asc,
          semantic_candidates.id asc,
          semantic_candidates.version desc
      )::integer as semantic_rank
    from semantic_candidates
  ), fused as materialized (
    select
      coalesce(lexical_ranked.id, semantic_ranked.id) as id,
      coalesce(lexical_ranked.version, semantic_ranked.version) as version,
      lexical_ranked.lexical_rank,
      semantic_ranked.semantic_rank,
      semantic_ranked.semantic_distance,
      pg_catalog.round(
        least(
          1::numeric,
          greatest(
            0::numeric,
            (
              coalesce(0.5::numeric / (60 + lexical_ranked.lexical_rank), 0::numeric)
              + coalesce(0.5::numeric / (60 + semantic_ranked.semantic_rank), 0::numeric)
            ) * 61::numeric
          )
        ),
        12
      ) as normalized_score
    from lexical_ranked
    full outer join semantic_ranked
      on semantic_ranked.id = lexical_ranked.id
     and semantic_ranked.version = lexical_ranked.version
  ), filtered as materialized (
    select fused.*,
      decorated.card,
      decorated.modified_at
    from fused
    join decorated
      on decorated.id = fused.id
     and decorated.version = fused.version
    where (
        not (p_filters ? 'accessLevel')
        or decorated.card ->> 'accessLevel' = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or lower(btrim(coalesce(decorated.card #>> '{geography,code}', '')))
          = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(
            coalesce(decorated.card -> 'classifications', '[]'::jsonb)
          ) as classification(item)
          where lower(btrim(classification.item ->> 'code'))
            = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (decorated.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (decorated.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or lower(btrim(coalesce(decorated.card ->> 'processSubtype', '')))
          = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or lower(btrim(coalesce(decorated.card ->> 'source', '')))
          = p_filters ->> 'source'
      )
  ), ordered as materialized (
    select filtered.*
    from filtered
    order by filtered.normalized_score desc,
      filtered.id asc,
      filtered.version desc
    limit p_limit
  )
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', ordered.id::text,
          'version', ordered.version
        ),
        'accessLevel', ordered.card -> 'accessLevel',
        'capabilities', ordered.card -> 'capabilities',
        'names', ordered.card -> 'names',
        'summary', ordered.card -> 'summary',
        'geography', ordered.card -> 'geography',
        'referenceYear', ordered.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          ordered.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', 'hybrid',
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'score', ordered.normalized_score,
          'reasonCodes', pg_catalog.to_jsonb(pg_catalog.array_remove(array[
            case when ordered.lexical_rank is not null
              then 'lexical_public_projection'::text end,
            case when ordered.semantic_rank is not null
              then 'semantic_public_projection'::text end
          ], null)),
          'evidence', pg_catalog.jsonb_build_object(
            'lexicalRank', ordered.lexical_rank,
            'semanticRank', ordered.semantic_rank,
            'semanticDistance', case
              when ordered.semantic_distance is null then null
              else pg_catalog.trim_scale(ordered.semantic_distance::numeric)::text
            end
          )
        )
      )
      order by ordered.normalized_score desc,
        ordered.id asc,
        ordered.version desc
    ),
    '[]'::jsonb
  )
  into v_items
  from ordered;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-hybrid-candidate-page.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'items', v_items
  );
  if pg_catalog.octet_length(pg_catalog.convert_to(v_result::text, 'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_result;
end
$function$;

comment on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) is
  'Fixed portal-hybrid-rank-v1 exact-version candidate kernel over one 100/200 public scope.';

revoke all on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) from public, anon, authenticated, service_role;
grant execute on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) to portal_public_executor;

reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
grant create on schema api to portal_public_executor;
set local role portal_public_executor;

create function api.portal_hybrid_search_v1(
  p_kind text,
  p_query_terms text[],
  p_query_embedding text,
  p_filters jsonb,
  p_limit integer
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
declare
  v_input jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,
    p_query_terms,
    p_query_embedding,
    p_filters,
    p_limit
  );
  v_page := private.portal_lcia_decorate_item_page_v1(
    private.portal_public_hybrid_search_v1_impl(
      v_input ->> 'kind',
      array(
        select term.value
        from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
          with ordinality as term(value, ordinality)
        order by term.ordinality
      ),
      (v_input ->> 'queryEmbedding')::extensions.vector(1024),
      v_input -> 'filters',
      (v_input ->> 'limit')::integer,
      v_input ->> 'queryFingerprint'
    )
  );
  if v_page is null
     or pg_catalog.octet_length(pg_catalog.convert_to(v_page::text, 'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end
$function$;

comment on function api.portal_hybrid_search_v1(text, text[], text, jsonb, integer) is
  'Bounded locator-free Process/Flow Hybrid candidate page over fixed public 100/200 scope and portal-hybrid-rank-v1.';

revoke all on function api.portal_hybrid_search_v1(text, text[], text, jsonb, integer)
  from public, anon, authenticated, service_role;
grant execute on function api.portal_hybrid_search_v1(text, text[], text, jsonb, integer)
  to anon, authenticated;

reset role;
revoke create on schema api from portal_public_executor;
revoke portal_public_executor from postgres;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values (
  'api.portal_hybrid_search_v1(text, text[], text, jsonb, integer)',
  'PORTAL-HYBRID-01',
  true,
  true,
  false
)
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

do $verify_portal_hybrid_contract$
declare
  v_api regprocedure := pg_catalog.to_regprocedure(
    'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
  );
  v_input regprocedure := pg_catalog.to_regprocedure(
    'private.portal_public_hybrid_input_v1(text,text[],text,jsonb,integer)'
  );
  v_card regprocedure := pg_catalog.to_regprocedure(
    'private.portal_public_hybrid_card_v1(text,integer,jsonb)'
  );
  v_kernel regprocedure := pg_catalog.to_regprocedure(
    'private.portal_public_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
  );
begin
  if v_api is null or v_input is null or v_card is null or v_kernel is null then
    raise exception 'Portal Hybrid routine installation is incomplete';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array['search_path=""', 'statement_timeout=8s']::text[]
      and routine.prosrc ~ 'portal_lcia_decorate_item_page_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_api
  ) is not true then
    raise exception 'Portal Hybrid API owner or runtime config mismatch';
  end if;
  if (
    select routine.proowner = 'api_internal_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan',
          'hnsw.iterative_scan=strict_order'
        ]::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = v_kernel
  ) is not true then
    raise exception 'Portal Hybrid kernel owner or runtime config mismatch';
  end if;
  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    cross join lateral pg_catalog.aclexplode(
      coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
    ) as acl
    where routine.oid = v_api
      and not (
        acl.privilege_type = 'EXECUTE'
        and not acl.is_grantable
        and acl.grantee in (
          routine.proowner,
          'anon'::regrole,
          'authenticated'::regrole
        )
      )
  ) or (
    select count(*)
    from pg_catalog.pg_proc as routine
    cross join lateral pg_catalog.aclexplode(
      coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
    ) as acl
    where routine.oid = v_api
  ) <> 3 then
    raise exception 'Portal Hybrid API ACL mismatch';
  end if;
  if pg_catalog.has_function_privilege(
       'service_role',
       'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)',
       'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'anon',
       'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)',
       'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'authenticated',
       'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)',
       'EXECUTE'
     ) then
    raise exception 'Portal Hybrid external privilege mismatch';
  end if;
  if not exists (
    select 1
    from private.api_capability_grants as manifest
    where manifest.routine_identity
      = 'api.portal_hybrid_search_v1(text, text[], text, jsonb, integer)'
      and manifest.capability_id = 'PORTAL-HYBRID-01'
      and manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ) then
    raise exception 'Portal Hybrid capability manifest mismatch';
  end if;
  if pg_catalog.has_function_privilege(
       'anon',
       'private.portal_public_hybrid_input_v1(text,text[],text,jsonb,integer)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.portal_public_hybrid_input_v1(text,text[],text,jsonb,integer)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.portal_public_hybrid_input_v1(text,text[],text,jsonb,integer)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.portal_public_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)',
       'EXECUTE'
     ) then
    raise exception 'Portal Hybrid private helper exposure mismatch';
  end if;
end
$verify_portal_hybrid_contract$;

do $verify_portal_hybrid_legacy_unchanged$
begin
  if exists (
    select 1
    from portal_hybrid_legacy_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
    join pg_catalog.pg_roles as owner_role
      on owner_role.oid = routine.proowner
    where before.definition <> pg_catalog.pg_get_functiondef(routine.oid)
       or before.owner_name <> owner_role.rolname
       or before.security_definer <> routine.prosecdef
       or before.proconfig <> coalesce(routine.proconfig, '{}'::text[])
       or before.acl_text <> coalesce(routine.proacl::text, '')
  ) or (
    select count(*)
    from portal_hybrid_legacy_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
  ) <> 8 then
    raise exception 'Existing raw Hybrid routine contract changed';
  end if;
end
$verify_portal_hybrid_legacy_unchanged$;

commit;
