begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

-- This is an explicit fault-injection test, not a normal pgTAP file.  The
-- standard local Supabase pg_prove path is deliberately recognized only by
-- its pg_prove-launched psql connection on a non-SSL private Docker network,
-- postgres database/user, and absence of project_url:
--
--   local:
--     npx --yes supabase@2.109.1 test db --local \
--       supabase/tests/preview/protected_alias_statement_timeout_fault.sql
--
-- A direct private-TCP local run or an optional disposable Preview run must
-- instead attest the exact target through PGOPTIONS:
--
--   direct private-TCP local:
--     PGOPTIONS='-c database_engine.protected_timeout_fault_target=local:database-engine-262'
--
--   disposable Preview (optional):
--     PGOPTIONS='-c database_engine.protected_timeout_fault_target=preview:<project-ref>:database-engine-262'
--
-- Production is rejected even when an attestation is supplied.  Local runs
-- without branch-local Vault values additionally require a non-SSL private
-- Docker connection before this transaction creates rollback-only test values.
create temporary table protected_timeout_fault_context (
  environment text not null,
  project_ref text not null,
  attestation text not null
) on commit drop;

do $safety_gate$
declare
  v_attestation text := current_setting(
    'database_engine.protected_timeout_fault_target',
    true
  );
  v_project_url text;
  v_context jsonb;
  v_application_name text := current_setting('application_name', true);
  v_server_addr inet := pg_catalog.inet_server_addr();
  v_client_addr inet := pg_catalog.inet_client_addr();
  v_ssl boolean := coalesce(
    (select connection.ssl
     from pg_catalog.pg_stat_ssl as connection
     where connection.pid = pg_catalog.pg_backend_pid()),
    false
  );
begin
  select nullif(btrim(secret.decrypted_secret), '')
  into v_project_url
  from vault.decrypted_secrets as secret
  where secret.name = 'project_url'
  limit 1;

  if v_project_url is null then
    if not (
      (
        v_attestation is not distinct from
          'local:database-engine-262'
        and not v_ssl
        and pg_catalog.inet_server_addr() is not null
        and (
          pg_catalog.inet_server_addr() <<= '127.0.0.0/8'::inet
          or pg_catalog.inet_server_addr() <<= '10.0.0.0/8'::inet
          or pg_catalog.inet_server_addr() <<= '172.16.0.0/12'::inet
          or pg_catalog.inet_server_addr() <<= '192.168.0.0/16'::inet
        )
      )
      or (
        v_attestation is null
        and not v_ssl
        and v_server_addr is not null
        and (
          v_server_addr <<= '10.0.0.0/8'::inet
          or v_server_addr <<= '172.16.0.0/12'::inet
          or v_server_addr <<= '192.168.0.0/16'::inet
        )
        and v_client_addr is not null
        and (
          v_client_addr <<= '10.0.0.0/8'::inet
          or v_client_addr <<= '172.16.0.0/12'::inet
          or v_client_addr <<= '192.168.0.0/16'::inet
        )
        and current_database() = 'postgres'
        and current_user = 'postgres'
        and v_application_name = 'psql'
      )
    ) then
      raise exception using
        errcode = '42501',
        message = 'Protected timeout fault requires an attested private local connection, the local Supabase pg_prove Docker session, or a trusted Preview project_url',
        detail = format(
          'application_name=%L database=%L user=%L server_addr=%L client_addr=%L server_port=%L ssl=%L attestation=%L',
          v_application_name,
          current_database(),
          current_user,
          v_server_addr,
          v_client_addr,
          pg_catalog.inet_server_port(),
          v_ssl,
          v_attestation
        );
    end if;

    insert into protected_timeout_fault_context (
      environment,
      project_ref,
      attestation
    ) values (
      'local',
      'local',
      coalesce(v_attestation, 'local:pg_prove:database-engine-262')
    );
  else
    v_context := util.dataset_alias_execution_server_context();

    if v_context->>'environment' = 'production'
      or v_context->>'project_ref' = 'qgzvkongdjqiiamzbbts' then
      raise exception using
        errcode = '42501',
        message = 'Protected timeout fault is forbidden on production';
    end if;

    if v_context->>'environment' = 'local' then
      if v_attestation is distinct from
          'local:database-engine-262' then
        raise exception using
          errcode = '42501',
          message = 'Protected timeout fault local attestation mismatch';
      end if;
    elsif v_context->>'environment' = 'preview' then
      if v_attestation is distinct from
          'preview:' || (v_context->>'project_ref') || ':database-engine-262' then
        raise exception using
          errcode = '42501',
          message = 'Protected timeout fault Preview attestation mismatch';
      end if;
    else
      raise exception using
        errcode = '42501',
        message = 'Protected timeout fault target is neither local nor Preview';
    end if;

    insert into protected_timeout_fault_context (
      environment,
      project_ref,
      attestation
    ) values (
      v_context->>'environment',
      v_context->>'project_ref',
      v_attestation
    );
  end if;
end
$safety_gate$;

-- Fresh local databases intentionally have no shared Vault data.  Add only
-- transaction-local values after the safety gate; the final ROLLBACK removes
-- them.  Preview must already have its branch-local values and never receives
-- synthetic credentials from this test.
do $local_vault_bootstrap$
declare
  v_environment text;
begin
  select context.environment
  into strict v_environment
  from protected_timeout_fault_context as context;

  if v_environment = 'local' then
    if not exists (
      select 1
      from vault.decrypted_secrets as secret
      where secret.name = 'project_url'
    ) then
      perform vault.create_secret(
        'http://127.0.0.1:54321',
        'project_url',
        'rollback-only database-engine #262 timeout proof URL'
      );
    end if;

    if not exists (
      select 1
      from vault.decrypted_secrets as secret
      where secret.name = 'project_secret_key'
    ) then
      perform vault.create_secret(
        'rollback-only-database-engine-262-service-key',
        'project_secret_key',
        'rollback-only database-engine #262 timeout proof key'
      );
    end if;
  elsif 2 <> (
    select count(distinct secret.name)
    from vault.decrypted_secrets as secret
    where secret.name in ('project_url', 'project_secret_key')
  ) then
    raise exception using
      errcode = '42501',
      message = 'Disposable Preview must provide both branch-local Vault values';
  end if;
end
$local_vault_bootstrap$;

select plan(9);

select ok(
  (
    select context.environment in ('local', 'preview')
      and context.project_ref <> 'qgzvkongdjqiiamzbbts'
      and util.dataset_alias_execution_server_context()
        @> jsonb_build_object(
          'environment', context.environment,
          'project_ref', context.project_ref
        )
    from protected_timeout_fault_context as context
  ),
  'fault target is an explicitly attested local or disposable Preview database'
);

create temporary table protected_timeout_function_baseline on commit drop as
select
  function_meta.oid::regprocedure::text as identity,
  pg_catalog.pg_get_functiondef(function_meta.oid) as definition,
  function_meta.proconfig,
  function_meta.proacl,
  function_meta.proowner,
  function_meta.prosecdef,
  function_meta.provolatile
from pg_catalog.pg_proc as function_meta
where function_meta.oid in (
  'public.cmd_dataset_alias_plan_guarded(jsonb)'::regprocedure,
  'public.cmd_dataset_alias_execution_execute(uuid,text)'::regprocedure
);

select ok(
  (
    select count(*) = 2
      and bool_and(baseline.definition is not null)
    from protected_timeout_function_baseline as baseline
  )
  and (
    select coalesce(function_meta.proconfig, array[]::text[])
      @> array[
        'search_path=""',
        'lock_timeout=5s',
        'statement_timeout=60s'
      ]::text[]
    from pg_catalog.pg_proc as function_meta
    where function_meta.oid =
      'public.cmd_dataset_alias_execution_execute(uuid,text)'::regprocedure
  )
  and has_function_privilege(
    'service_role',
    'public.cmd_dataset_alias_execution_execute(uuid,text)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_execute(uuid,text)',
    'execute'
  ),
  'baseline captures both functions and the released 60-second service-only executor contract'
);

do $fixture_collision_gate$
begin
  if exists (
      select 1
      from util.dataset_alias_execution_preflights
      where id = 'e2620000-0000-4000-8000-000000000002'
    ) or exists (
      select 1
      from util.dataset_alias_execution_requests
      where id = 'e2620000-0000-4000-8000-000000000002'
    ) or exists (
      select 1
      from public.command_audit_log
      where command = 'protected_alias_statement_timeout_fault_sentinel'
    ) then
    raise exception using
      errcode = '23505',
      message = 'Protected timeout fault fixture namespace is already in use';
  end if;
end
$fixture_collision_gate$;

with fixture_clock as (
  select pg_catalog.clock_timestamp() - interval '1 second' as completed_at
)
insert into util.dataset_alias_execution_preflights (
  id,
  actor_user_id,
  actor_email,
  environment,
  project_ref,
  target_visibility,
  plan,
  freeze_envelope,
  approval_envelope,
  plan_sha256,
  operation_id,
  plan_request_sha256,
  bindings,
  bindings_sha256,
  expected,
  expected_sha256,
  derivative_targets,
  derivative_targets_sha256,
  gate_expectations,
  gate_expectations_sha256,
  failure_baseline_sha256,
  preflight_request_sha256,
  preflight_proof_sha256,
  freeze_sha256,
  approval_identity_sha256,
  token_sha256,
  completed_at,
  expires_at,
  consumed_at,
  created_at
)
select
  'e2620000-0000-4000-8000-000000000002',
  'e2620000-0000-4000-8000-000000000001',
  'protected-timeout-owner@example.com',
  context.environment,
  context.project_ref,
  'owner_draft',
  '{}'::jsonb,
  '{}'::jsonb,
  '{}'::jsonb,
  repeat('1', 64),
  'protected-alias-statement-timeout-fault',
  repeat('2', 64),
  '{}'::jsonb,
  repeat('3', 64),
  '{}'::jsonb,
  repeat('4', 64),
  '[]'::jsonb,
  repeat('5', 64),
  '{}'::jsonb,
  repeat('6', 64),
  repeat('7', 64),
  repeat('8', 64),
  repeat('9', 64),
  repeat('a', 64),
  repeat('b', 64),
  repeat('c', 64),
  fixture_clock.completed_at,
  fixture_clock.completed_at + interval '180 seconds',
  fixture_clock.completed_at + interval '500 milliseconds',
  fixture_clock.completed_at
from protected_timeout_fault_context as context
cross join fixture_clock;

insert into util.dataset_alias_execution_requests (
  id,
  actor_user_id,
  plan_sha256,
  operation_id,
  plan_request_sha256,
  freeze_sha256,
  approval_identity_sha256,
  approval_text_sha256,
  derivative_target_set_sha256,
  preflight_proof_sha256,
  admission_request_sha256,
  gate_results,
  gate_results_sha256,
  nonce_sha256,
  attempt_count,
  dispatch_count,
  net_request_id,
  status,
  admitted_at,
  dispatched_at
) values (
  'e2620000-0000-4000-8000-000000000002',
  'e2620000-0000-4000-8000-000000000001',
  repeat('1', 64),
  'protected-alias-statement-timeout-fault',
  repeat('2', 64),
  repeat('a', 64),
  repeat('b', 64),
  repeat('d', 64),
  repeat('e', 64),
  repeat('9', 64),
  repeat('f', 64),
  '{}'::jsonb,
  repeat('0', 64),
  util.dataset_alias_execution_sha256(repeat('e', 64)),
  1,
  1,
  826200000000000002,
  'dispatched',
  pg_catalog.clock_timestamp(),
  pg_catalog.clock_timestamp()
);

-- The transaction-local primitive writes one recognizable effect before a
-- two-second sleep.  A real executor statement_timeout must cancel the call;
-- its subtransaction then removes both this sentinel and the executor's prior
-- status='running' update.
create or replace function public.cmd_dataset_alias_plan_guarded(
  p_plan jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $fault_primitive$
begin
  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    'protected_alias_statement_timeout_fault_sentinel',
    auth.uid(),
    null,
    null,
    null,
    jsonb_build_object('record_type', 'rollback_sentinel')
  );

  perform pg_catalog.pg_sleep(2);

  return jsonb_build_object(
    'ok', true,
    'idempotent_replay', false,
    'plan_sha256', repeat('1', 64),
    'operation_id', 'protected-alias-statement-timeout-fault',
    'plan_request_sha256', repeat('2', 64),
    'row_count', 52,
    'exchange_count', 59
  );
end
$fault_primitive$;

alter function public.cmd_dataset_alias_execution_execute(uuid, text)
  set statement_timeout = '250ms';

create or replace function pg_temp.capture_protected_timeout()
returns jsonb
language plpgsql
set search_path = ''
as $capture$
begin
  begin
    perform public.cmd_dataset_alias_execution_execute(
      'e2620000-0000-4000-8000-000000000002',
      repeat('e', 64)
    );

    return jsonb_build_object(
      'caught', false,
      'sqlstate', null,
      'message', 'executor unexpectedly returned'
    );
  exception
    when sqlstate '57014' then
      return jsonb_build_object(
        'caught', true,
        'sqlstate', sqlstate,
        'message', sqlerrm
      );
    when others then
      return jsonb_build_object(
        'caught', true,
        'sqlstate', sqlstate,
        'message', sqlerrm
      );
  end;
end
$capture$;

create temporary table protected_timeout_result (
  result jsonb not null
) on commit drop;
grant insert, select on protected_timeout_result to service_role;

select set_config(
  'request.headers',
  jsonb_build_object(
    'apikey', util.project_secret_key(),
    'authorization', 'Bearer ' || util.project_secret_key()
  )::text,
  true
);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

set local role service_role;
set local statement_timeout = '250ms';
insert into pg_temp.protected_timeout_result (result)
select pg_temp.capture_protected_timeout();
set local statement_timeout = '0';
reset role;

-- Restore both definitions from the exact catalog snapshots before asserting
-- equivalence.  The outer ROLLBACK remains the final fail-safe.
do $restore_functions$
declare
  v_definition text;
begin
  select baseline.definition
  into strict v_definition
  from protected_timeout_function_baseline as baseline
  where baseline.identity =
    'cmd_dataset_alias_plan_guarded(jsonb)';
  execute v_definition;

  select baseline.definition
  into strict v_definition
  from protected_timeout_function_baseline as baseline
  where baseline.identity =
    'cmd_dataset_alias_execution_execute(uuid,text)';
  execute v_definition;
end
$restore_functions$;

select ok(
  (
    select result->>'caught' = 'true'
      and result->>'sqlstate' = '57014'
      and result->>'message' like '%statement timeout%'
    from protected_timeout_result
  ),
  'service-role executor is genuinely canceled with SQLSTATE 57014 by statement_timeout'
);

select is(
  (
    select request.attempt_count::text || ':' ||
      request.dispatch_count::text || ':' ||
      request.status || ':' ||
      coalesce(request.started_at::text, 'null') || ':' ||
      coalesce(request.terminal_at::text, 'null')
    from util.dataset_alias_execution_requests as request
    where request.id = 'e2620000-0000-4000-8000-000000000002'
  ),
  '1:1:dispatched:null:null',
  'statement timeout rolls back executor-local state while preserving the admitted one-shot attempt'
);

select ok(
  (
    select preflight.consumed_at is not null
      and preflight.preflight_proof_sha256 = repeat('9', 64)
      and preflight.approval_identity_sha256 = repeat('b', 64)
    from util.dataset_alias_execution_preflights as preflight
    where preflight.id = 'e2620000-0000-4000-8000-000000000002'
  ),
  'statement timeout preserves the consumed preflight and its immutable approval proof'
);

select is(
  (
    select count(*)::integer
    from public.command_audit_log as audit
    where audit.command = 'protected_alias_statement_timeout_fault_sentinel'
  ),
  0,
  'statement timeout rolls back the inner primitive sentinel with no partial audit effect'
);

select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_requests as child
    where child.actor_user_id =
        'e2620000-0000-4000-8000-000000000001'
      or child.batch_id =
        'e2620000-0000-4000-8000-000000000002'
  ),
  0,
  'statement timeout creates no partial derivative child request'
);

select ok(
  not exists (
    select 1
    from protected_timeout_function_baseline as baseline
    join pg_catalog.pg_proc as function_meta
      on function_meta.oid = baseline.identity::regprocedure
    where pg_catalog.pg_get_functiondef(function_meta.oid)
        is distinct from baseline.definition
      or function_meta.proconfig is distinct from baseline.proconfig
      or function_meta.proowner is distinct from baseline.proowner
      or function_meta.prosecdef is distinct from baseline.prosecdef
      or function_meta.provolatile is distinct from baseline.provolatile
  ),
  'temporary primitive body and executor timeout are restored to their exact catalog definitions'
);

select ok(
  not exists (
    select 1
    from protected_timeout_function_baseline as baseline
    join pg_catalog.pg_proc as function_meta
      on function_meta.oid = baseline.identity::regprocedure
    where function_meta.proacl is distinct from baseline.proacl
  )
  and has_function_privilege(
    'service_role',
    'public.cmd_dataset_alias_execution_execute(uuid,text)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_execute(uuid,text)',
    'execute'
  ),
  'temporary fault injection preserves exact ACLs and the service-only executor boundary'
);

select * from finish();
rollback;
