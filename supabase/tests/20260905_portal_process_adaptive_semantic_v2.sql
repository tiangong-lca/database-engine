-- Database #620 catalog/ACL proof for selective exact Process semantic routing.
begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions,public,auth;
select extensions.no_plan();

select extensions.is(
  (
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
      and index_catalog.indislive
  ),
  array[
    'portal_catalog_facet_process_access_level_v1_idx:5f4694628fe3e9037b6dc4bba37fa0a06b06b80155524cd604dae935864cbe9b',
    'portal_catalog_facet_process_geography_v1_idx:ee35080db59786839c5d4a51e4ee09e6e8c619d98f8bbdbde27e24c0d02f9cf2'
  ]::text[],
  'both exact-version Process facet indexes are canonical, valid, ready and live'
);

select extensions.is(
  pg_catalog.encode(
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
  ),
  '41aa02a05bd381fb86e068ee6d6830feb77d92022b0733dc6cbb90970dd44801'::text,
  'Process V2 semantic helper has the reviewed adaptive-route definition'
);

select extensions.is(
  pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  ),
  '534a4e6cead5da4575747d50667e9280e469a672526d0eb97488b99577230b2a'::text,
  'Flow V2 semantic helper remains byte-identical'
);

select extensions.ok(
  (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 's'
      and routine.proparallel = 'r'
      and routine.proconfig = array[
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
      and routine.prosrc ~ 'v_exact_cutover constant integer := 2000'
      and routine.prosrc ~ 'v_indexed_probe'
      and routine.prosrc ~ 'facet_geography'
      and routine.prosrc ~ 'facet_access_level'
      and routine.prosrc ~ 'portal_card_matches_filters_v2'
      and routine.prosrc ~ 'generate_subscripts'
      and routine.prosrc ~ 'limit v_exact_cutover \+ 1'
      and routine.prosrc ~ 'offset 0'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)'::regprocedure
  ),
  'adaptive Process helper retains Portal owner, hardening, 2000/2001 bound and HNSW fallback'
);

select extensions.ok(
  pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'portal_public_executor',
    'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.portal_projection_semantic_process_v2(extensions.vector,jsonb)',
    'EXECUTE'
  ),
  'adaptive helper remains private behind the internal-to-Portal executor boundary'
);

grant portal_public_executor to postgres;
set local role portal_public_executor;
select extensions.lives_ok(
  $$select private.assert_portal_catalog_projection_contract_v1()$$,
  'immutable card/document projection contract remains valid'
);
select extensions.lives_ok(
  $$select private.assert_portal_catalog_facet_contract_v1()$$,
  'immutable facet derivation contract remains valid'
);
reset role;
revoke portal_public_executor from postgres;

select * from extensions.finish();
rollback;
