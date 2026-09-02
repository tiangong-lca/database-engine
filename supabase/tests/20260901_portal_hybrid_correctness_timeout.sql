begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select extensions.no_plan();

create temporary table portal_hybrid_timeout_expected_routines (
  routine_identity text primary key
) on commit drop;

insert into portal_hybrid_timeout_expected_routines(routine_identity)
values
  ('api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'),
  ('private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'),
  ('private.catalog_portal_hybrid_pattern_matches_v1(text,text[])'),
  ('private.catalog_portal_process_pattern_versions_v1(text)'),
  ('private.catalog_portal_flow_pattern_versions_v1(text)'),
  ('private.catalog_portal_process_single_character_versions_v1(text)'),
  ('private.catalog_portal_flow_single_character_versions_v1(text)'),
  ('private.portal_projection_semantic_candidates_v1(text,extensions.vector)'),
  ('private.portal_projection_semantic_process_v1(extensions.vector)'),
  ('private.portal_projection_semantic_flow_v1(extensions.vector)'),
  ('private.portal_projection_semantic_process_exact_v1(extensions.vector)'),
  ('private.portal_projection_semantic_flow_exact_v1(extensions.vector)'),
  ('private.portal_decorate_card_context_v1(jsonb)');

select extensions.is(
  (
    select count(*)
    from portal_hybrid_timeout_expected_routines as expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
    where coalesce(routine.proconfig, '{}'::text[])
        @> array['statement_timeout=20s']::text[]
      and not coalesce(routine.proconfig, '{}'::text[])
        @> array['statement_timeout=8s']::text[]
  ),
  13::bigint,
  'the complete Portal Hybrid statement path uses the bounded 20-second correctness budget'
);

select extensions.ok(
  (
    select routine.proconfig @> array['statement_timeout=8s']::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)'
    )
  ),
  'the lexical Search coordinator retains its independent eight-second budget'
);

grant portal_public_executor to postgres;
set local role portal_public_executor;

select extensions.is(
  private.portal_card_context_manifest_sha256_v1(),
  'db78336c8604848af1e068352f8a39d9ee740308c44c59c639b986ed2660c47e',
  'the config-only card-context manifest advances to the exact reviewed digest'
);

select extensions.lives_ok(
  $$select private.assert_portal_card_context_contract_v1()$$,
  'the card-context runtime guard accepts the config-only timeout manifest'
);

reset role;

select extensions.ok(
  not pg_catalog.has_function_privilege(
    'anon',
    'private.portal_decorate_card_context_v1(jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.portal_decorate_card_context_v1(jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.portal_decorate_card_context_v1(jsonb)',
    'EXECUTE'
  ),
  'the timeout change grants no new card-context execution path'
);

select * from extensions.finish();
rollback;
