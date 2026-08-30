begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(12);

select is(
  (
    select count(*)
    from private.oauth_relation_capability_grants
    where command in ('insert', 'update', 'delete')
  ),
  0::bigint,
  'OAuth does not reopen raw core-table write routes'
);

select is(
  (
    select count(*)
    from pg_policy
    where polname like 'oauth_client_%_capability_guard'
      and polcmd in ('a', 'w', 'd')
  ),
  0::bigint,
  'OAuth adds no raw relation write policies'
);

select is(
  (
    select string_agg(
      concat_ws(':', routine.proname, manifest.capability_id),
      ','
      order by routine.proname
    )
    from private.api_capability_grants as manifest
    join pg_proc as routine
      on routine.oid = to_regprocedure(manifest.routine_identity)
    join pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname in (
        'cmd_dataset_create',
        'cmd_dataset_save_draft',
        'cmd_dataset_delete'
      )
  ),
  'cmd_dataset_create:DB-CORE-WRITE-01,cmd_dataset_delete:DB-CORE-WRITE-01,cmd_dataset_save_draft:DB-CORE-WRITE-01'::text,
  'three actor-bound CRUD commands use the narrow write capability'
);

select is(
  (
    select count(*)
    from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and has_function_privilege('authenticated', routine.oid, 'execute')
      and not exists (
        select 1
        from private.api_capability_grants as manifest
        where to_regprocedure(manifest.routine_identity) = routine.oid
          and manifest.allow_authenticated
      )
  ),
  0::bigint,
  'every authenticated API ACL has an OAuth capability manifest row'
);

select is(
  (
    select count(*)
    from (
      select routine.proname
      from private.api_capability_grants as manifest
      join pg_proc as routine
        on routine.oid = to_regprocedure(manifest.routine_identity)
      join pg_namespace as namespace on namespace.oid = routine.pronamespace
      where namespace.nspname = 'api'
      group by routine.proname
      having count(distinct manifest.capability_id) <> 1
    ) as ambiguous
  ),
  0::bigint,
  'every PostgREST RPC route resolves to one capability'
);

create temporary table oauth_actor_command_results (
  label text primary key,
  value jsonb not null
) on commit drop;
grant all on oauth_actor_command_results to service_role;

set local role service_role;
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
insert into oauth_actor_command_results (label, value)
values (
  'writer',
  api.svc_oauth_client_configure(
    'test-oauth-command-writer',
    'mcp',
    true,
    array['DB-CORE-READ-01', 'DB-CORE-WRITE-01']
  )
), (
  'reader',
  api.svc_oauth_client_configure(
    'test-oauth-command-reader',
    'cli',
    true,
    array['DB-CORE-READ-01']
  )
);
reset role;

set local role authenticated;
select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"83100000-0000-4000-8000-000000000001","client_id":"test-oauth-command-writer"}',
  true
);
select ok(
  private.oauth_client_has_capability('DB-CORE-WRITE-01'),
  'MCP writer receives the narrow command capability'
);
select set_config('request.path', '/rpc/cmd_dataset_create', true);
select set_config('request.method', 'POST', true);
select lives_ok(
  'select api.oauth_client_pre_request()',
  'writer passes create-command pre-request admission'
);
select set_config('request.path', '/rpc/cmd_dataset_save_draft', true);
select lives_ok(
  'select api.oauth_client_pre_request()',
  'writer passes save-command pre-request admission'
);
select set_config('request.path', '/rpc/cmd_dataset_delete', true);
select lives_ok(
  'select api.oauth_client_pre_request()',
  'writer passes delete-command pre-request admission'
);

select set_config('request.path', '/contacts', true);
select set_config('request.headers', '{"content-profile":"public"}', true);
select throws_ok(
  'select api.oauth_client_pre_request()',
  '42501',
  'OAuth client is not authorized for this API route',
  'raw table write remains unclassified and fails closed'
);

select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"83100000-0000-4000-8000-000000000001","client_id":"test-oauth-command-reader"}',
  true
);
select set_config('request.path', '/rpc/cmd_dataset_create', true);
select throws_ok(
  'select api.oauth_client_pre_request()',
  '42501',
  'OAuth client is not authorized for this API route',
  'read-only client cannot call the create command'
);

select ok(
  private.oauth_client_has_capability('DB-CORE-READ-01'),
  'read-only client retains the explicit relation-read capability'
);

reset role;

select * from finish();
rollback;
