begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_table(
  'private', 'lcia_result_sets',
  'ResultSet containers are persisted in the private schema'
);
select col_is_pk(
  'private', 'lcia_result_sets', 'id',
  'ResultSet identity is a UUID primary key'
);
select has_column(
  'private', 'lcia_result_sets', 'name',
  'ResultSet persists its user-facing name'
);
select has_column(
  'private', 'lcia_result_sets', 'created_at',
  'ResultSet persists its creation time'
);
select has_column(
  'private', 'lcia_scope_closure_checks', 'result_set_id',
  'Scope Closure optionally binds to a ResultSet'
);
select fk_ok(
  'private', 'lcia_scope_closure_checks', 'result_set_id',
  'private', 'lcia_result_sets', 'id',
  'Scope Closure ResultSet binding is referentially constrained'
);
select ok(
  (
    select is_nullable = 'YES'
    from information_schema.columns
    where table_schema = 'private'
      and table_name = 'lcia_scope_closure_checks'
      and column_name = 'result_set_id'
  ),
  'legacy Scope Closure rows may remain unbound'
);
select ok(
  (
    select relrowsecurity
    from pg_catalog.pg_class
    where oid = 'private.lcia_result_sets'::regclass
  ),
  'ResultSet table enables RLS'
);
select ok(
  not has_table_privilege(
    'authenticated', 'private.lcia_result_sets', 'select'
  ),
  'authenticated callers cannot read ResultSet rows directly'
);
select ok(
  not has_table_privilege(
    'authenticated', 'private.lcia_result_sets', 'insert'
  ),
  'authenticated callers cannot create ResultSet rows directly'
);
select ok(
  exists (
    select 1
    from pg_catalog.pg_indexes
    where schemaname = 'private'
      and indexname = 'lcia_scope_closure_checks_result_set_idx'
  ),
  'ResultSet foreign-key lookups have a supporting index'
);

select has_function(
  'api', 'cmd_lcia_result_set_create', array['text'],
  'ResultSet create command exists'
);
select has_function(
  'api', 'list_lcia_result_sets', array['integer'],
  'bounded ResultSet list query exists'
);
select has_function(
  'api', 'get_lcia_result_set', array['uuid'],
  'ResultSet read query exists'
);
select has_function(
  'api', 'cmd_lcia_scope_closure_check_request_v3',
  array['uuid', 'jsonb', 'text', 'jsonb'],
  'ResultSet-aware Scope Closure command exists'
);
select has_function(
  'api', 'cmd_lcia_scope_closure_check_request_v2',
  array['jsonb', 'text', 'jsonb'],
  'legacy nullable Scope Closure command remains available'
);
select ok(
  has_function_privilege(
    'authenticated', 'api.cmd_lcia_result_set_create(text)', 'execute'
  )
  and has_function_privilege(
    'authenticated', 'api.list_lcia_result_sets(integer)', 'execute'
  )
  and has_function_privilege(
    'authenticated', 'api.get_lcia_result_set(uuid)', 'execute'
  )
  and has_function_privilege(
    'authenticated',
    'api.cmd_lcia_scope_closure_check_request_v3(uuid,jsonb,text,jsonb)',
    'execute'
  ),
  'authenticated callers use only role-gated ResultSet RPCs'
);
select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_lcia_scope_closure_check_request_v3(uuid,jsonb,text,jsonb)'
        ::regprocedure
    ),
    'result_set_id = p_result_set_id'
  ) > 0,
  'ResultSet-aware Scope Closure command persists the binding'
);
select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'private.get_task_summary_v2_feed_unversioned(text,text[],text[],timestamp with time zone,timestamp with time zone,uuid,integer,boolean)'
        ::regprocedure
    ),
    '''resultSetId'''
  ) > 0
  and pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'private.get_task_summary_v2_feed_unversioned(text,text[],text[],timestamp with time zone,timestamp with time zone,uuid,integer,boolean)'
        ::regprocedure
    ),
    '''resultSetName'''
  ) > 0,
  'TaskSummary safely projects ResultSet identity and name'
);
select ok(
  exists (
    select 1
    from private.api_capability_grants
    where routine_identity = 'api.cmd_lcia_result_set_create(text)'
      and allow_authenticated
      and not allow_anon
      and not allow_service_role
  )
  and exists (
    select 1
    from private.api_capability_grants
    where routine_identity =
      'api.cmd_lcia_scope_closure_check_request_v3(uuid, jsonb, text, jsonb)'
      and allow_authenticated
      and not allow_anon
      and not allow_service_role
  ),
  'ResultSet RPCs are recorded in the exact capability manifest'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password,
  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
  created_at, updated_at, is_sso_user, is_anonymous
) values
  (
    '00000000-0000-0000-0000-000000000000',
    '51700000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'result-set-manager@example.com',
    'x', now(), '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '51700000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'result-set-member@example.com',
    'x', now(), '{}', '{}', now(), now(), false, false
  );

insert into private.users(id, raw_user_meta_data, contact) values
  ('51700000-0000-4000-8000-000000000001', '{}', null),
  ('51700000-0000-4000-8000-000000000002', '{}', null);
insert into private.teams(id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System"}', 0, false
)
on conflict (id) do nothing;
insert into private.roles(user_id, team_id, role)
values (
  '51700000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'data_product_manager'
);

create temporary table result_set_test_context (
  result_set_id uuid primary key
) on commit drop;
grant select, insert on result_set_test_context to authenticated;

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '51700000-0000-4000-8000-000000000002',
  true
);

select is(
  api.cmd_lcia_result_set_create('member result')->>'code',
  'not_data_product_manager',
  'ordinary users cannot create ResultSets'
);
select is(
  api.list_lcia_result_sets()->>'code',
  'not_data_product_manager',
  'ordinary users cannot list ResultSets'
);

select set_config(
  'request.jwt.claim.sub',
  '51700000-0000-4000-8000-000000000001',
  true
);

select is(
  api.cmd_lcia_result_set_create('   ')->>'code',
  'invalid_result_set_name',
  'blank ResultSet names are rejected'
);

insert into result_set_test_context(result_set_id)
select (
  api.cmd_lcia_result_set_create('  test  ')->'data'->>'resultSetId'
)::uuid;

select is(
  api.get_lcia_result_set(
    (select result_set_id from result_set_test_context)
  )->'data'->>'name',
  'test',
  'ResultSet create trims and persists the display name'
);
select is(
  api.list_lcia_result_sets()->'data'->'items'->0->>'resultSetId',
  (select result_set_id::text from result_set_test_context),
  'ResultSet list returns the newest persistent container'
);
select is(
  api.list_lcia_result_sets()->'data'->'items'->0->>'schemaVersion',
  'lcia.result-set.v1',
  'ResultSet list items carry the stable DTO version'
);

reset role;

select is(
  (select name from private.lcia_result_sets limit 1),
  'test',
  'ResultSet table stores only the approved minimal user data'
);

select * from finish();
rollback;
