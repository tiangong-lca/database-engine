begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(15);

select is(
  (
    select jsonb_object_agg(
      manifest.routine_identity,
      jsonb_build_object(
        'capabilityId', manifest.capability_id,
        'anon', manifest.allow_anon,
        'authenticated', manifest.allow_authenticated,
        'serviceRole', manifest.allow_service_role
      )
      order by manifest.routine_identity
    )
    from private.api_capability_grants as manifest
    where manifest.routine_identity in (
      select pg_catalog.format(
        '%I.%I(%s)',
        namespace.nspname,
        routine.proname,
        pg_catalog.oidvectortypes(routine.proargtypes)
      )
      from pg_catalog.pg_proc as routine
      join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
      where routine.oid = any(array[
        'api.policy_roles_select(uuid,text)'::regprocedure,
        'api.policy_review_can_read(uuid,uuid)'::regprocedure,
        'api.assert_lca_release_manager()'::regprocedure,
        'api.get_current_lca_release()'::regprocedure,
        'api.get_current_lca_release_process(uuid,text)'::regprocedure,
        'api.get_lca_release_artifact_download(uuid)'::regprocedure,
        'api.get_lca_release_run(uuid)'::regprocedure
      ]::oid[])
    )
  ),
  jsonb_build_object(
    'api.assert_lca_release_manager()',
      jsonb_build_object('capabilityId', 'EDGE-REL-01', 'anon', false, 'authenticated', true, 'serviceRole', false),
    'api.get_current_lca_release()',
      jsonb_build_object('capabilityId', 'EDGE-REL-01', 'anon', true, 'authenticated', true, 'serviceRole', true),
    'api.get_current_lca_release_process(uuid, text)',
      jsonb_build_object('capabilityId', 'EDGE-REL-01', 'anon', true, 'authenticated', true, 'serviceRole', true),
    'api.get_lca_release_artifact_download(uuid)',
      jsonb_build_object('capabilityId', 'EDGE-REL-01', 'anon', false, 'authenticated', true, 'serviceRole', true),
    'api.get_lca_release_run(uuid)',
      jsonb_build_object('capabilityId', 'EDGE-REL-01', 'anon', false, 'authenticated', true, 'serviceRole', true),
    'api.policy_roles_select(uuid, text)',
      jsonb_build_object('capabilityId', 'NX-CORE-01', 'anon', false, 'authenticated', true, 'serviceRole', false),
    'api.policy_review_can_read(uuid, uuid)',
      jsonb_build_object('capabilityId', 'NX-REV-01', 'anon', false, 'authenticated', true, 'serviceRole', false)
  ),
  'the exact capability manifest records the seven runtime ACL repairs'
);

select ok(
  has_function_privilege('authenticated', 'api.policy_roles_select(uuid,text)', 'EXECUTE')
    and not has_function_privilege('anon', 'api.policy_roles_select(uuid,text)', 'EXECUTE')
    and not has_function_privilege('service_role', 'api.policy_roles_select(uuid,text)', 'EXECUTE')
    and has_function_privilege('authenticated', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE')
    and not has_function_privilege('anon', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE')
    and not has_function_privilege('service_role', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE'),
  'the roles and review RLS helpers are executable only by authenticated'
);

select ok(
  has_function_privilege('service_role', 'api.get_current_lca_release()', 'EXECUTE')
    and has_function_privilege('service_role', 'api.get_current_lca_release_process(uuid,text)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.get_lca_release_artifact_download(uuid)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.get_lca_release_run(uuid)', 'EXECUTE'),
  'service role can execute all four Edge release read facades'
);

select ok(
  has_function_privilege('anon', 'api.get_current_lca_release()', 'EXECUTE')
    and has_function_privilege('anon', 'api.get_current_lca_release_process(uuid,text)', 'EXECUTE')
    and not has_function_privilege('anon', 'api.get_lca_release_artifact_download(uuid)', 'EXECUTE')
    and not has_function_privilege('anon', 'api.get_lca_release_run(uuid)', 'EXECUTE'),
  'anonymous direct access remains limited to the two public projections'
);

select ok(
  has_function_privilege('authenticated', 'api.assert_lca_release_manager()', 'EXECUTE')
    and not has_function_privilege('anon', 'api.assert_lca_release_manager()', 'EXECUTE')
    and not has_function_privilege('service_role', 'api.assert_lca_release_manager()', 'EXECUTE')
    and has_function_privilege('authenticated', 'api.get_current_lca_release()', 'EXECUTE')
    and has_function_privilege('authenticated', 'api.get_current_lca_release_process(uuid,text)', 'EXECUTE')
    and has_function_privilege('authenticated', 'api.get_lca_release_artifact_download(uuid)', 'EXECUTE')
    and has_function_privilege('authenticated', 'api.get_lca_release_run(uuid)', 'EXECUTE'),
  'authenticated retains the manager assertion and all four release read facades'
);

set local role authenticated;
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select pg_catalog.set_config('request.jwt.claim.sub', '42270000-0000-4000-8000-000000000001', true);

select lives_ok(
  $$select api.policy_roles_select('42270000-0000-4000-8000-000000000010'::uuid, 'member')$$,
  'authenticated can execute the roles policy helper under JWT context'
);

select lives_ok(
  $$
    select
      (select count(*) from public.processes)
      + (select count(*) from public.flows)
      + (select count(*) from public.contacts)
      + (select count(*) from public.sources)
      + (select count(*) from public.unitgroups)
      + (select count(*) from public.flowproperties)
      + (select count(*) from public.lifecyclemodels)
  $$,
  'all seven roles-dependent core relations evaluate RLS without 42501'
);

select lives_ok(
  $$select (select count(*) from public.lciamethods) + (select count(*) from public.ilcd)$$,
  'the two roles-independent core relations remain readable'
);

select lives_ok(
  $$select (select count(*) from private.roles) + (select count(*) from private.reviews)$$,
  'authenticated can evaluate both private RLS dependencies without 42501'
);

select lives_ok(
  $$select count(*) from api.qry_team_list('public', null, 1, 10)$$,
  'the authenticated team read facade remains executable'
);

select lives_ok(
  $$
    select
      (select count(*) from api.qry_review_get_items(null, null, null, null))
      + (select count(*) from api.qry_review_get_comment_items(
          '42270000-0000-4000-8000-000000000050'::uuid,
          'all'
        ))
  $$,
  'the authenticated review and comment read facades remain executable'
);

reset role;
set local role service_role;
select pg_catalog.set_config('request.jwt.claim.role', 'service_role', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"service_role"}', true);
select pg_catalog.set_config('request.jwt.claim.sub', '', true);

select lives_ok(
  $$select api.get_current_lca_release()$$,
  'service role can call the current release projection'
);

select lives_ok(
  $$select api.get_current_lca_release_process('42270000-0000-4000-8000-000000000020'::uuid, '1.0.0')$$,
  'service role can call the current release process projection'
);

select lives_ok(
  $$select api.get_lca_release_artifact_download('42270000-0000-4000-8000-000000000030'::uuid)$$,
  'service role can call the release artifact download projection'
);

select lives_ok(
  $$select api.get_lca_release_run('42270000-0000-4000-8000-000000000040'::uuid)$$,
  'service role can call the release run projection'
);

reset role;

select * from finish();
rollback;
