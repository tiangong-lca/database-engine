-- Database #620: avoid deep filtered HNSW traversal when an already
-- synchronized Process facet proves that the exact-version candidate universe is
-- small.  Broad/unfiltered shapes retain the existing strict iterative HNSW
-- path and configuration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

-- Register pgvector's user-settable GUCs in this migration connection before
-- storing them in function configuration (a fresh connection may not have
-- loaded the extension library yet).
select extensions.vector_dims('[1]'::extensions.vector);

do $portal_process_adaptive_semantic_cutover_guard$
declare
  v_function_sha text;
  v_index_fingerprints text[];
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_function_sha;

  select pg_catalog.array_agg(
    index_relation.relname || ':' || pg_catalog.encode(
      extensions.digest(
        pg_catalog.convert_to(
          pg_catalog.pg_get_indexdef(index_relation.oid),
          'UTF8'
        ),
        'sha256'
      ),
      'hex'
    )
    order by index_relation.relname
  )
  into v_index_fingerprints
  from pg_catalog.pg_class as index_relation
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = index_relation.relnamespace
  join pg_catalog.pg_index as index_catalog
    on index_catalog.indexrelid = index_relation.oid
  where namespace.nspname = 'private'
    and index_relation.relname in (
      'portal_catalog_facet_process_access_level_v1_idx',
      'portal_catalog_facet_process_geography_v1_idx'
    )
    and index_catalog.indrelid =
      'private.portal_catalog_facet_rows_v1'::regclass
    and index_catalog.indisvalid
    and index_catalog.indisready
    and index_catalog.indislive;

  if v_function_sha is distinct from
       '81adb1e731c955ab7810b75046b60e176e0448256fb8c0fdcc35aec7a25c2148'
     or v_index_fingerprints is distinct from array[
       'portal_catalog_facet_process_access_level_v1_idx:5f4694628fe3e9037b6dc4bba37fa0a06b06b80155524cd604dae935864cbe9b',
       'portal_catalog_facet_process_geography_v1_idx:ee35080db59786839c5d4a51e4ee09e6e8c619d98f8bbdbde27e24c0d02f9cf2'
     ]::text[]
     or not exists (
       select 1
       from pg_catalog.pg_roles
       where rolname = 'api_internal_executor'
         and not rolcanlogin
         and not rolbypassrls
         and not rolsuper
         and not rolreplication
     )
     or not exists (
       select 1
       from pg_catalog.pg_roles
       where rolname = 'portal_public_executor'
         and not rolcanlogin
         and not rolbypassrls
         and not rolsuper
         and not rolreplication
     ) then
    raise exception 'Portal Process adaptive semantic prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_process_adaptive_semantic_cutover_guard$;

comment on index private.portal_catalog_facet_process_geography_v1_idx is
  'Exact Process id/version candidates for bounded Portal semantic geography routing.';
comment on index private.portal_catalog_facet_process_access_level_v1_idx is
  'Exact Process id/version candidates for bounded Portal semantic access-level routing.';

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

create or replace function private.portal_projection_semantic_process_v2(
  p_query_embedding extensions.vector,
  p_filters jsonb
)
returns table(
  id uuid,
  version text,
  semantic_distance double precision
)
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set statement_timeout = '20s'
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
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer;
  v_indexed_probe boolean := false;
begin
  if p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024 then
    raise exception using
      errcode = '22023',
      message = 'invalid portal request';
  end if;

  -- Geography and access level are exact, normalized facts in the
  -- transactionally synchronized facet child.  Additional filters remain a
  -- final canonical card recheck, so this key set is a safe candidate
  -- superset for combined filter requests.
  if (p_filters ? 'geography') and (p_filters ? 'accessLevel') then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_geography = p_filters ->> 'geography'
        and facet.facet_access_level = p_filters ->> 'accessLevel'
      limit v_exact_cutover + 1
    ) as candidate;
  elsif p_filters ? 'geography' then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_geography = p_filters ->> 'geography'
      limit v_exact_cutover + 1
    ) as candidate;
  elsif p_filters ? 'accessLevel' then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_access_level = p_filters ->> 'accessLevel'
      limit v_exact_cutover + 1
    ) as candidate;
  end if;

  v_candidate_count := coalesce(
    pg_catalog.cardinality(v_candidate_ids),
    0
  );

  if v_indexed_probe
     and v_candidate_count <= v_exact_cutover then
    return query
    with candidate_keys as materialized (
      select
        v_candidate_ids[key.ordinal] as id,
        v_candidate_versions[key.ordinal] as version
      from pg_catalog.generate_subscripts(
        v_candidate_ids,
        1
      ) as key(ordinal)
    ), nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) p_query_embedding
          as distance
      from candidate_keys as candidate
      join public.processes as source
        on source.id = candidate.id
       and source.version::text = candidate.version
      join private.portal_catalog_search_rows_v1 as projection
        on projection.dataset_kind = 'process'
       and projection.id = candidate.id
       and projection.version = candidate.version
      where source.state_code in (100, 200)
        and source.embedding_ft is not null
        and projection.state_code in (100, 200)
        and private.portal_card_matches_filters_v2(
          projection.card,
          p_filters
        )
      -- The no-op addition deliberately prevents the global HNSW index from
      -- satisfying this ORDER BY.  Only the bounded exact-key rows are scored.
      order by
        (
          source.embedding_ft operator(extensions.<=>) p_query_embedding
        ) + 0::double precision,
        source.id,
        source.version::text desc
      limit 200
    )
    select nearest.id, nearest.version, nearest.distance
    from nearest
    where nearest.distance >= 0::double precision
      and nearest.distance <= 0.5::double precision
    order by
      nearest.distance + 0::double precision,
      nearest.id,
      nearest.version desc;
    return;
  end if;

  -- Unfiltered, unsupported-filter-only, and broad indexed-filter requests
  -- retain the predecessor HNSW path byte-for-byte.
  return query
  with nearest as materialized (
    select source.id, source.version::text as version,
      source.embedding_ft operator(extensions.<=>) p_query_embedding as distance
    from public.processes as source
    where source.state_code in (100,200)
      and source.embedding_ft is not null
      and exists (
        select 1 from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'process'
          and projection.id = source.id and projection.version = source.version::text
          and projection.state_code in (100,200)
          and (p_filters = '{}'::jsonb
            or private.portal_card_matches_filters_v2(projection.card, p_filters))
        offset 0
      )
    order by source.embedding_ft operator(extensions.<=>) p_query_embedding
    limit 200
  )
  select nearest.id, nearest.version, nearest.distance
  from nearest
  where nearest.distance >= 0::double precision and nearest.distance <= 0.5::double precision
  order by nearest.distance + 0::double precision, nearest.id, nearest.version desc;
end;
$function$;

revoke all on function private.portal_projection_semantic_process_v2(
  extensions.vector,
  jsonb
) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_semantic_process_v2(
  extensions.vector,
  jsonb
) to portal_public_executor;

comment on function private.portal_projection_semantic_process_v2(
  extensions.vector,
  jsonb
) is
  'Returns at most 200 exact Process versions: exact distance over at most 2000 indexed geography/access candidates, otherwise strict iterative HNSW.';

reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

do $verify_portal_process_adaptive_semantic_cutover$
declare
  v_function_sha text;
  v_index_fingerprints text[];
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_function_sha;

  select pg_catalog.array_agg(
    index_relation.relname || ':' || pg_catalog.encode(
      extensions.digest(
        pg_catalog.convert_to(
          pg_catalog.pg_get_indexdef(index_relation.oid),
          'UTF8'
        ),
        'sha256'
      ),
      'hex'
    )
    order by index_relation.relname
  )
  into v_index_fingerprints
  from pg_catalog.pg_class as index_relation
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = index_relation.relnamespace
  join pg_catalog.pg_index as index_catalog
    on index_catalog.indexrelid = index_relation.oid
  where namespace.nspname = 'private'
    and index_relation.relname in (
      'portal_catalog_facet_process_access_level_v1_idx',
      'portal_catalog_facet_process_geography_v1_idx'
    )
    and index_catalog.indrelid =
      'private.portal_catalog_facet_rows_v1'::regclass
    and index_catalog.indisvalid
    and index_catalog.indisready
    and index_catalog.indislive;

  if v_function_sha is distinct from
       '41aa02a05bd381fb86e068ee6d6830feb77d92022b0733dc6cbb90970dd44801'
     or v_index_fingerprints is distinct from array[
       'portal_catalog_facet_process_access_level_v1_idx:5f4694628fe3e9037b6dc4bba37fa0a06b06b80155524cd604dae935864cbe9b',
       'portal_catalog_facet_process_geography_v1_idx:ee35080db59786839c5d4a51e4ee09e6e8c619d98f8bbdbde27e24c0d02f9cf2'
     ]::text[]
     or (
       select routine.proowner <> 'api_internal_executor'::regrole
         or not routine.prosecdef
         or routine.provolatile <> 's'
         or routine.proparallel <> 'r'
         or routine.proconfig is distinct from array[
           'search_path=""',
           'statement_timeout=20s',
           'plan_cache_mode=force_custom_plan',
           'hnsw.iterative_scan=strict_order',
           'hnsw.ef_search=200',
           'hnsw.max_scan_tuples=20000',
           'hnsw.scan_mem_multiplier=2',
           'jit=off',
           'row_security=on'
         ]::text[]
         or routine.prosrc !~
           'v_exact_cutover constant integer := 2000'
         or routine.prosrc !~
           'portal_catalog_facet_rows_v1'
         or routine.prosrc !~
           'facet_geography'
         or routine.prosrc !~
           'facet_access_level'
         or routine.prosrc !~
           'generate_subscripts'
         or routine.prosrc !~
           'limit v_exact_cutover \+ 1'
         or routine.prosrc !~
           'offset 0'
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)'::regprocedure
     ) is not false
     or not pg_catalog.has_function_privilege(
       'portal_public_executor',
       'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
       'EXECUTE'
     ) then
    raise exception 'Portal Process adaptive semantic cutover drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_process_adaptive_semantic_cutover$;

notify pgrst, 'reload schema';

commit;
