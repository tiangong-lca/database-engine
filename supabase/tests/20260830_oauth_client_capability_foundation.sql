begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(34);

select has_table(
  'private',
  'oauth_client_registry',
  'OAuth client revocation registry exists'
);
select has_table(
  'private',
  'oauth_client_capability_grants',
  'OAuth client capability grants exist'
);
select has_table(
  'private',
  'oauth_relation_capability_grants',
  'OAuth relation capability manifest exists'
);
select has_table(
  'private',
  'oauth_client_registry_audit',
  'OAuth client registry changes are audited'
);

select has_function(
  'private',
  'oauth_client_has_capability',
  array['text'],
  'RLS capability predicate exists'
);
select has_function(
  'api',
  'oauth_client_pre_request',
  array[]::text[],
  'PostgREST OAuth capability hook exists'
);
select has_function(
  'api',
  'svc_oauth_client_configure',
  array['text', 'text', 'boolean', 'text[]'],
  'service-only OAuth client configuration facade exists'
);

select ok(
  not has_table_privilege('anon', 'private.oauth_client_registry', 'select')
  and not has_table_privilege('authenticated', 'private.oauth_client_registry', 'select')
  and not has_table_privilege('service_role', 'private.oauth_client_registry', 'select'),
  'application roles cannot read the OAuth client registry directly'
);
select ok(
  not has_table_privilege('anon', 'private.oauth_client_capability_grants', 'select')
  and not has_table_privilege('authenticated', 'private.oauth_client_capability_grants', 'select')
  and not has_table_privilege('service_role', 'private.oauth_client_capability_grants', 'select'),
  'application roles cannot read OAuth capability grants directly'
);
select ok(
  not has_table_privilege('anon', 'private.oauth_client_registry_audit', 'select')
  and not has_table_privilege('authenticated', 'private.oauth_client_registry_audit', 'select')
  and not has_table_privilege('service_role', 'private.oauth_client_registry_audit', 'select')
  and not has_sequence_privilege(
    'service_role',
    'private.oauth_client_registry_audit_id_seq',
    'usage'
  ),
  'OAuth audit state is writable only through its owner-controlled facade'
);

select is(
  (
    select concat_ws('|', routine.prosecdef::text, array_to_string(routine.proconfig, ','))
    from pg_proc as routine
    where routine.oid = 'private.oauth_client_has_capability(text)'::regprocedure
  ),
  'true|search_path=""'::text,
  'RLS capability predicate is SECURITY DEFINER with an empty search_path'
);
select is(
  (
    select concat_ws('|', routine.prosecdef::text, array_to_string(routine.proconfig, ','))
    from pg_proc as routine
    where routine.oid = 'api.oauth_client_pre_request()'::regprocedure
  ),
  'true|search_path=""'::text,
  'pre-request hook is SECURITY DEFINER with an empty search_path'
);
select is(
  (
    select concat_ws('|', routine.prosecdef::text, array_to_string(routine.proconfig, ','))
    from pg_proc as routine
    where routine.oid =
      'api.svc_oauth_client_configure(text,text,boolean,text[])'::regprocedure
  ),
  'true|search_path=""'::text,
  'configuration facade is SECURITY DEFINER with an empty search_path'
);

select ok(
  has_function_privilege('anon', 'api.oauth_client_pre_request()', 'execute')
  and has_function_privilege('authenticated', 'api.oauth_client_pre_request()', 'execute')
  and has_function_privilege('service_role', 'api.oauth_client_pre_request()', 'execute'),
  'every PostgREST application role can execute the configured hook'
);
select ok(
  has_function_privilege(
    'service_role',
    'api.svc_oauth_client_configure(text,text,boolean,text[])',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'api.svc_oauth_client_configure(text,text,boolean,text[])',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'api.svc_oauth_client_configure(text,text,boolean,text[])',
    'execute'
  ),
  'only service_role can configure OAuth client admission'
);

select is(
  (
    select string_agg(
      concat_ws(
        ':',
        capability_id,
        allow_anon::text,
        allow_authenticated::text,
        allow_service_role::text
      ),
      ','
      order by routine_identity
    )
    from private.api_capability_grants
    where routine_identity in (
      'api.oauth_client_pre_request()',
      'api.svc_oauth_client_configure(text, text, boolean, text[])'
    )
  ),
  'DB-OAUTH-GATE-01:true:true:true,DB-OAUTH-ADMIN-01:false:false:true'::text,
  'OAuth hook and administrator facade have exact capability manifests'
);

select is(
  (
    select setting
    from pg_db_role_setting as role_setting
    cross join lateral unnest(role_setting.setconfig) as setting
    where role_setting.setrole = 'authenticator'::regrole
      and setting like 'pgrst.db_pre_request=%'
  ),
  'pgrst.db_pre_request=api.oauth_client_pre_request'::text,
  'authenticator configures the OAuth pre-request hook'
);

select is(
  (
    select count(*)
    from private.oauth_relation_capability_grants
    where relation_schema = 'public'
      and command = 'select'
      and capability_id = 'DB-CORE-READ-01'
  ),
  9::bigint,
  'all nine public Data API relations map to the read capability'
);
select is(
  (
    select count(*)
    from private.oauth_relation_capability_grants as relation_grant
    join pg_class as relation
      on relation.oid = format(
        '%I.%I',
        relation_grant.relation_schema,
        relation_grant.relation_name
      )::regclass
    where relation.relrowsecurity
  ),
  9::bigint,
  'every relation capability target has RLS enabled'
);
select is(
  (
    select count(*)
    from pg_policy as policy
    where policy.polname = 'oauth_client_select_capability_guard'
      and not policy.polpermissive
      and policy.polcmd = 'r'
      and policy.polroles = array['authenticated'::regrole::oid]
  ),
  9::bigint,
  'every public Data API relation has one authenticated restrictive guard'
);
select is(
  (
    select count(*)
    from pg_policy as policy
    where policy.polname = 'oauth_client_select_capability_guard'
      and pg_get_expr(policy.polqual, policy.polrelid)
        like '%oauth_client_has_capability%DB-CORE-READ-01%'
  ),
  9::bigint,
  'every restrictive guard invokes the database read capability predicate'
);

select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"56600000-0000-4000-8000-000000000001"}',
  true
);
select ok(
  private.oauth_client_has_capability('NOT-A-REAL-CAPABILITY'),
  'a first-party session without client_id retains existing RLS behavior'
);

set local role authenticated;
select throws_ok(
  $$select api.svc_oauth_client_configure(
    'test-oauth-client',
    'cli',
    true,
    array['NOT-A-REAL-CAPABILITY']
  )$$,
  '42501',
  'permission denied for function svc_oauth_client_configure',
  'authenticated callers cannot configure their own grants'
);
reset role;

create temporary table oauth_test_results (
  label text primary key,
  value jsonb not null
) on commit drop;
grant all on oauth_test_results to service_role;

set local role service_role;
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
insert into oauth_test_results (label, value)
values (
  'create',
  api.svc_oauth_client_configure(
    'test-oauth-client',
    'cli',
    true,
    array['NX-CORE-02', 'DB-CORE-READ-01', 'NX-CORE-02']
  )
);
reset role;

select is(
  (select value ->> 'ok' from oauth_test_results where label = 'create'),
  'true'::text,
  'service configuration creates an admitted client'
);
select is(
  (
    select count(*)
    from private.oauth_client_capability_grants
    where client_id = 'test-oauth-client'
      and allowed
  ),
  2::bigint,
  'service configuration normalizes and replaces capability grants'
);
select is(
  (
    select action
    from private.oauth_client_registry_audit
    where client_id = 'test-oauth-client'
    order by id desc
    limit 1
  ),
  'create'::text,
  'client creation is durably audited'
);

set local role authenticated;
select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"56600000-0000-4000-8000-000000000001","client_id":"test-oauth-client"}',
  true
);
select ok(
  private.oauth_client_has_capability('NX-CORE-02'),
  'admitted OAuth client can use an explicitly granted RPC capability'
);
select ok(
  not private.oauth_client_has_capability('NX-CMD-01'),
  'admitted OAuth client cannot use an ungranted RPC capability'
);
select set_config('request.path', '/rpc/search_processes_latest_v2', true);
select set_config('request.method', 'POST', true);
select lives_ok(
  'select api.oauth_client_pre_request()',
  'pre-request hook admits an explicitly granted RPC route'
);
select set_config('request.path', '/rpc/cmd_review_assign_reviewers', true);
select throws_ok(
  'select api.oauth_client_pre_request()',
  '42501',
  'OAuth client is not authorized for this API route',
  'pre-request hook rejects an ungranted SECURITY DEFINER RPC route'
);
select set_config('request.path', '/processes', true);
select set_config('request.method', 'GET', true);
select set_config('request.headers', '{"accept-profile":"public"}', true);
select lives_ok(
  'select api.oauth_client_pre_request()',
  'pre-request hook admits a granted public relation read'
);
select set_config('request.path', '/processes', true);
select set_config('request.method', 'POST', true);
select set_config('request.headers', '{"content-profile":"public"}', true);
select throws_ok(
  'select api.oauth_client_pre_request()',
  '42501',
  'OAuth client is not authorized for this API route',
  'pre-request hook fails closed for an unmapped relation write'
);
reset role;

set local role service_role;
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
insert into oauth_test_results (label, value)
values (
  'disable',
  api.svc_oauth_client_configure(
    'test-oauth-client',
    'cli',
    false,
    array['NX-CORE-02', 'DB-CORE-READ-01']
  )
);
reset role;

select is(
  (
    select concat_ws('|', enabled::text, action)
    from private.oauth_client_registry as client
    cross join lateral (
      select audit.action
      from private.oauth_client_registry_audit as audit
      where audit.client_id = client.client_id
      order by audit.id desc
      limit 1
    ) as latest
    where client.client_id = 'test-oauth-client'
  ),
  'false|disable'::text,
  'revocation disables the client and records an audit event'
);

set local role authenticated;
select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"56600000-0000-4000-8000-000000000001","client_id":"test-oauth-client"}',
  true
);
select ok(
  not private.oauth_client_has_capability('NX-CORE-02'),
  'revocation immediately invalidates every client capability'
);
reset role;

select * from finish();
rollback;
