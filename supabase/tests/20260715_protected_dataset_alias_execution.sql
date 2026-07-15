begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.protected_actor_id()
returns uuid
language sql
immutable
as $$
  select 'a1000000-0000-0000-0000-000000000001'::uuid
$$;

create or replace function pg_temp.protected_entity_id(
  p_table text,
  p_ordinal integer
) returns uuid
language sql
immutable
as $$
  select (
    case p_table
      when 'flowproperties' then 'a3000000'
      when 'flows' then 'a4000000'
      when 'processes' then 'b5000000'
      when 'target_unitgroups' then 'a6000000'
      when 'source_unitgroups' then 'a7000000'
    end
    || '-0000-0000-0000-'
    || lpad(p_ordinal::text, 12, '0')
  )::uuid
$$;

create or replace function pg_temp.protected_action_payload(
  p_table text,
  p_ordinal integer
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'protected', true,
    'table', p_table,
    'id', pg_temp.protected_entity_id(p_table, p_ordinal),
    'ordinal', p_ordinal
  )
$$;

create or replace function pg_temp.protected_support_payload(
  p_dimension text,
  p_role text
) returns jsonb
language sql
immutable
as $$
  select case p_role
    when 'flowproperty' then pg_temp.protected_action_payload(
      'flowproperties',
      case p_dimension when 'time' then 1 else 2 end
    )
    else jsonb_build_object(
      'protected', true,
      'dimension', p_dimension,
      'role', p_role,
      'id', pg_temp.protected_entity_id(
        case p_role
          when 'unitgroup' then 'target_unitgroups'
          else 'source_unitgroups'
        end,
        case p_dimension when 'time' then 1 else 2 end
      )
    )
  end
$$;

create or replace function pg_temp.protected_support_snapshot(
  p_dimension text,
  p_role text
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'id', case p_role
      when 'flowproperty' then pg_temp.protected_entity_id(
        'flowproperties',
        case p_dimension when 'time' then 1 else 2 end
      )
      when 'unitgroup' then pg_temp.protected_entity_id(
        'target_unitgroups',
        case p_dimension when 'time' then 1 else 2 end
      )
      else pg_temp.protected_entity_id(
        'source_unitgroups',
        case p_dimension when 'time' then 1 else 2 end
      )
    end,
    'version', '00.00.001',
    'expected_modified_at', '2026-07-15T00:00:00+00:00',
    'expected_json_ordered',
      pg_temp.protected_support_payload(p_dimension, p_role)
  )
$$;

create or replace function pg_temp.protected_actions(p_dimension text)
returns jsonb
language sql
stable
as $$
  with action_rows as (
    select
      'flowproperty-' || p_dimension as action_id,
      'flowproperties'::text as table_name,
      case p_dimension when 'time' then 1 else 2 end as ordinal
    union all
    select
      'flow-' || lpad(flow_ordinal::text, 3, '0'),
      'flows',
      flow_ordinal
    from generate_series(
      case p_dimension when 'time' then 1 else 13 end,
      case p_dimension when 'time' then 12 else 23 end
    ) as flow_ordinal
    union all
    select
      'process-' || lpad(process_ordinal::text, 3, '0'),
      'processes',
      process_ordinal
    from generate_series(
      case p_dimension when 'time' then 1 else 15 end,
      case p_dimension when 'time' then 14 else 27 end
    ) as process_ordinal
  )
  select jsonb_agg(
    jsonb_build_object(
      'action_id', action_id,
      'table', table_name,
      'id', pg_temp.protected_entity_id(table_name, ordinal),
      'version', '00.00.001',
      'desired_json_ordered',
        pg_temp.protected_action_payload(table_name, ordinal)
    ) order by action_id
  )
  from action_rows
$$;

create or replace function pg_temp.protected_plan()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-plan.v1',
    'plan_sha256', repeat('c', 64),
    'operation_id', 'protected-alias-test',
    'target_visibility', 'owner_draft',
    'batches', jsonb_build_array(
      jsonb_build_object(
        'dimension', 'time',
        'target', jsonb_build_object(
          'flowproperty',
            pg_temp.protected_support_snapshot('time', 'flowproperty'),
          'unitgroup',
            pg_temp.protected_support_snapshot('time', 'unitgroup'),
          'source_unitgroup',
            pg_temp.protected_support_snapshot('time', 'source_unitgroup')
        ),
        'actions', pg_temp.protected_actions('time')
      ),
      jsonb_build_object(
        'dimension', 'length_time',
        'target', jsonb_build_object(
          'flowproperty',
            pg_temp.protected_support_snapshot('length_time', 'flowproperty'),
          'unitgroup',
            pg_temp.protected_support_snapshot('length_time', 'unitgroup'),
          'source_unitgroup',
            pg_temp.protected_support_snapshot(
              'length_time',
              'source_unitgroup'
            )
        ),
        'actions', pg_temp.protected_actions('length_time')
      )
    )
  )
$$;

create or replace function pg_temp.protected_targets()
returns jsonb
language sql
stable
as $$
  select jsonb_agg(
    jsonb_build_object(
      'table', action.value->>'table',
      'id', action.value->>'id',
      'version', action.value->>'version',
      'user_id', pg_temp.protected_actor_id(),
      'state_code', 0,
      'baseline_snapshot_sha256', repeat('b', 64)
    ) order by
      action.value->>'table',
      action.value->>'id',
      action.value->>'version'
  )
  from jsonb_array_elements(pg_temp.protected_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
  where action.value->>'table' in ('flows', 'processes')
$$;

create or replace function pg_temp.protected_internal_targets()
returns jsonb
language sql
stable
as $$
  select jsonb_agg(
    jsonb_build_object(
      'table', action.value->>'table',
      'id', action.value->>'id',
      'version', action.value->>'version',
      'expected_json_ordered_sha256',
        util.dataset_alias_execution_sha256(
          (action.value->'desired_json_ordered')::text
        ),
      'baseline_snapshot_sha256', repeat('b', 64)
    ) order by
      action.value->>'table',
      action.value->>'id',
      action.value->>'version'
  )
  from jsonb_array_elements(pg_temp.protected_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
  where action.value->>'table' in ('flows', 'processes')
$$;

create or replace function pg_temp.protected_expected()
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'action_count', 52,
    'batch_count', 2,
    'exchange_count', 59,
    'amount_field_count', 118,
    'unrelated_exchange_count', 309,
    'audit_count', 55,
    'flowproperty_count', 2,
    'flow_count', 23,
    'process_count', 27,
    'derivative_target_count', 50
  )
$$;

create or replace function pg_temp.protected_alias_plan_request_sha256()
returns text
language sql
stable
as $$
  select util.dataset_alias_execution_artifact_sha256(
    pg_temp.protected_plan()
  )
$$;

create or replace function pg_temp.protected_derivative_target_set_sha256()
returns text
language sql
stable
as $$
  select util.dataset_alias_execution_artifact_sha256(
    jsonb_agg(
      jsonb_build_object(
        'table', target.value->>'table',
        'id', target.value->>'id',
        'version', target.value->>'version',
        'user_id', target.value->>'user_id',
        'state_code', 0
      ) order by
        target.value->>'table',
        target.value->>'id',
        target.value->>'version'
    )
  )
  from jsonb_array_elements(pg_temp.protected_targets()) as target(value)
$$;

create or replace function pg_temp.protected_derivative_baseline_set_sha256()
returns text
language sql
stable
as $$
  select util.dataset_alias_execution_artifact_sha256(
    jsonb_agg(
      jsonb_build_object(
        'table', target.value->>'table',
        'id', target.value->>'id',
        'version', target.value->>'version',
        'baseline_snapshot_sha256',
          target.value->>'baseline_snapshot_sha256'
      ) order by
        target.value->>'table',
        target.value->>'id',
        target.value->>'version'
    )
  )
  from jsonb_array_elements(pg_temp.protected_targets()) as target(value)
$$;

create or replace function pg_temp.protected_freeze_without_sha()
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-execution-freeze.v1',
    'environment', 'local',
    'project_ref', 'local',
    'account', jsonb_build_object(
      'user_id', pg_temp.protected_actor_id(),
      'email', 'protected-owner@example.com'
    ),
    'target_visibility', 'owner_draft',
    'plan', jsonb_build_object(
      'plan_file_sha256', repeat('1', 64),
      'plan_sha256', repeat('c', 64),
      'operation_id', 'protected-alias-test'
    ),
    'sets', jsonb_build_object(
      'alias_plan_request_sha256',
        pg_temp.protected_alias_plan_request_sha256(),
      'before_hash_set_sha256', repeat('7', 64),
      'desired_hash_set_sha256', repeat('8', 64),
      'exchange_rewrite_set_sha256', repeat('9', 64),
      'support_snapshot_set_sha256', repeat('a', 64),
      'derivative_baseline_set_sha256',
        pg_temp.protected_derivative_baseline_set_sha256(),
      'derivative_target_set_sha256',
        pg_temp.protected_derivative_target_set_sha256(),
      'toolchain_evidence_sha256', repeat('d', 64)
    ),
    'expected', pg_temp.protected_expected(),
    'derivative_targets', pg_temp.protected_targets(),
    'policy', jsonb_build_object(
      'state_code_changes', 0,
      'save_draft', 0,
      'deletes', 0,
      'rebuild_derivatives', 0,
      'unitgroup_actions', 0,
      'person_distance_actions', 0,
      'max_admit_posts', 1,
      'automatic_retry', false
    ),
    'freeze_sha256', ''
  )
$$;

create or replace function pg_temp.protected_freeze_sha256()
returns text
language sql
stable
as $$
  select util.dataset_alias_execution_artifact_sha256(
    pg_temp.protected_freeze_without_sha()
  )
$$;

create or replace function pg_temp.protected_freeze()
returns jsonb
language sql
stable
as $$
  select jsonb_set(
    pg_temp.protected_freeze_without_sha(),
    '{freeze_sha256}',
    to_jsonb(pg_temp.protected_freeze_sha256()),
    false
  )
$$;

create or replace function pg_temp.protected_approval_without_sha(
  p_approved_at text
) returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-execution-approval.v1',
    'approved_at_utc', p_approved_at,
    'environment', 'local',
    'project_ref', 'local',
    'account', jsonb_build_object(
      'user_id', pg_temp.protected_actor_id(),
      'email', 'protected-owner@example.com'
    ),
    'target_visibility', 'owner_draft',
    'plan_sha256', repeat('c', 64),
    'operation_id', 'protected-alias-test',
    'plan_file_sha256', repeat('1', 64),
    'freeze_file_sha256', repeat('2', 64),
    'freeze_sha256', pg_temp.protected_freeze_sha256(),
    'approval_text_sha256', repeat('6', 64),
    'max_admit_posts', 1,
    'automatic_retry', false,
    'approval_identity_sha256', ''
  )
$$;

create or replace function pg_temp.protected_approval_identity_sha256(
  p_approved_at text
) returns text
language sql
stable
as $$
  select util.dataset_alias_execution_artifact_sha256(
    pg_temp.protected_approval_without_sha(p_approved_at)
  )
$$;

create or replace function pg_temp.protected_approval(p_approved_at text)
returns jsonb
language sql
stable
as $$
  select jsonb_set(
    pg_temp.protected_approval_without_sha(p_approved_at),
    '{approval_identity_sha256}',
    to_jsonb(pg_temp.protected_approval_identity_sha256(p_approved_at)),
    false
  )
$$;

create or replace function pg_temp.protected_bindings(p_approved_at text)
returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'plan_file_sha256', repeat('1', 64),
    'freeze_file_sha256', repeat('2', 64),
    'freeze_sha256', pg_temp.protected_freeze_sha256(),
    'approval_file_sha256', repeat('4', 64),
    'approval_identity_sha256',
      pg_temp.protected_approval_identity_sha256(p_approved_at),
    'approval_text_sha256', repeat('6', 64),
    'alias_plan_request_sha256',
      pg_temp.protected_alias_plan_request_sha256(),
    'before_hash_set_sha256', repeat('7', 64),
    'desired_hash_set_sha256', repeat('8', 64),
    'exchange_rewrite_set_sha256', repeat('9', 64),
    'support_snapshot_set_sha256', repeat('a', 64),
    'derivative_baseline_set_sha256',
      pg_temp.protected_derivative_baseline_set_sha256(),
    'derivative_target_set_sha256',
      pg_temp.protected_derivative_target_set_sha256(),
    'toolchain_evidence_sha256', repeat('d', 64)
  )
$$;

create or replace function pg_temp.protected_preflight_request(
  p_request_id uuid,
  p_approved_at text default '2026-07-15T12:00:00Z'
) returns jsonb
language sql
stable
as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-execution-preflight.v1',
    'request_id', p_request_id,
    'environment', 'local',
    'project_ref', 'local',
    'actor', jsonb_build_object(
      'user_id', pg_temp.protected_actor_id(),
      'email', 'protected-owner@example.com'
    ),
    'target_visibility', 'owner_draft',
    'plan', pg_temp.protected_plan(),
    'freeze', pg_temp.protected_freeze(),
    'approval', pg_temp.protected_approval(p_approved_at),
    'bindings', pg_temp.protected_bindings(p_approved_at),
    'expected', pg_temp.protected_expected(),
    'derivative_targets', pg_temp.protected_targets()
  )
$$;

-- The wrapper test uses transaction-local replacements for the already
-- exhaustive alias and derivative primitive suites.  The replacements create
-- the exact audit closure and terminal derivative proof expected by the
-- protected orchestration while rollback restores the production bodies.
create or replace function public.cmd_dataset_alias_plan_guarded(
  p_plan jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_plan_request_sha256 text :=
    util.dataset_alias_execution_sha256(p_plan::text);
begin
  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  select
    'cmd_dataset_alias_batch_guarded',
    v_actor,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', case when ordinal <= 52 then 'row' else 'batch_summary' end,
      'plan_sha256', p_plan->>'plan_sha256',
      'operation_id', p_plan->>'operation_id',
      'ordinal', ordinal
    )
  from generate_series(1, 54) as ordinal;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    'cmd_dataset_alias_plan_guarded',
    v_actor,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', 'plan_summary',
      'plan_request_sha256', v_plan_request_sha256
    )
  );

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_alias_plan_guarded',
    'plan_sha256', p_plan->>'plan_sha256',
    'operation_id', p_plan->>'operation_id',
    'plan_request_sha256', v_plan_request_sha256,
    'row_count', 52,
    'exchange_count', 59,
    'idempotent_replay', false
  );
end;
$$;

create or replace function util.dataset_derivative_rebuild_snapshot(
  p_table text,
  p_id uuid,
  p_version text
) returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  select jsonb_build_object(
    'schema_version', 'dataset-derivative-snapshot.v1',
    'table', p_table,
    'id', p_id,
    'version', p_version,
    'user_id', pg_temp.protected_actor_id(),
    'state_code', 0,
    'modified_at', '2026-07-15T00:00:00Z',
    'json_sha256', repeat('a', 64),
    'json_ordered_sha256', repeat('a', 64),
    'extracted_text_sha256', repeat('d', 64),
    'extracted_md_sha256', repeat('e', 64),
    'embedding_ft_sha256', repeat('f', 64),
    'embedding_ft_at', '2026-07-15T00:00:00Z',
    'snapshot_sha256', repeat('b', 64)
  )
$$;

create or replace function util.admit_dataset_derivative_rebuild_batch(
  p_actor_user_id uuid,
  p_batch_id uuid,
  p_plan_sha256 text,
  p_operation_id text,
  p_reason_code text,
  p_targets jsonb
) returns jsonb
language sql
security definer
set search_path = ''
as $$
  select jsonb_build_object(
    'ok', true,
    'batch_id', p_batch_id,
    'target_count', jsonb_array_length(p_targets),
    'flow_count', 23,
    'process_count', 27
  )
$$;

create or replace function util.read_dataset_derivative_rebuild_batch(
  p_actor_user_id uuid,
  p_batch_id uuid
) returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  select jsonb_build_object(
    'ok', true,
    'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
    'batch_id', p_batch_id,
    'status', 'completed',
    'code', 'DERIVATIVE_BATCH_COMPLETED',
    'causal_terminal_proof', true,
    'target_count', 50,
    'flow_count', 23,
    'process_count', 27,
    'completed_count', 50,
    'nonterminal_count', 0,
    'failed_count', 0,
    'targets', '[]'::jsonb
  )
$$;

-- Install exact live desired rows for the primary-closure helper.  Trigger
-- side effects are irrelevant to this orchestration test and stay disabled
-- only for fixture writes inside the surrounding rollback transaction.
set local session_replication_role = replica;

insert into public.flowproperties (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  modified_at
)
select
  pg_temp.protected_entity_id('flowproperties', ordinal),
  '00.00.001',
  pg_temp.protected_action_payload('flowproperties', ordinal),
  pg_temp.protected_action_payload('flowproperties', ordinal)::json,
  pg_temp.protected_actor_id(),
  0,
  '2026-07-15T00:00:00Z'::timestamptz
from generate_series(1, 2) as ordinal;

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  modified_at
)
select
  pg_temp.protected_entity_id('flows', ordinal),
  '00.00.001',
  pg_temp.protected_action_payload('flows', ordinal),
  pg_temp.protected_action_payload('flows', ordinal)::json,
  pg_temp.protected_actor_id(),
  0,
  '2026-07-15T00:00:00Z'::timestamptz
from generate_series(1, 23) as ordinal;

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  modified_at
)
select
  pg_temp.protected_entity_id('processes', ordinal),
  '00.00.001',
  pg_temp.protected_action_payload('processes', ordinal),
  pg_temp.protected_action_payload('processes', ordinal)::json,
  pg_temp.protected_actor_id(),
  0,
  '2026-07-15T00:00:00Z'::timestamptz
from generate_series(1, 27) as ordinal;

insert into public.unitgroups (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  modified_at
)
select
  pg_temp.protected_entity_id(table_name, ordinal),
  '00.00.001',
  pg_temp.protected_support_payload(dimension, role),
  pg_temp.protected_support_payload(dimension, role)::json,
  pg_temp.protected_actor_id(),
  0,
  '2026-07-15T00:00:00Z'::timestamptz
from (values
  ('time'::text, 'unitgroup'::text, 'target_unitgroups'::text, 1),
  ('time'::text, 'source_unitgroup'::text, 'source_unitgroups'::text, 1),
  ('length_time'::text, 'unitgroup'::text, 'target_unitgroups'::text, 2),
  ('length_time'::text, 'source_unitgroup'::text, 'source_unitgroups'::text, 2)
) as support(dimension, role, table_name, ordinal);

set local session_replication_role = origin;

delete from public.command_audit_log
where actor_user_id = pg_temp.protected_actor_id();
delete from util.dataset_derivative_rebuild_requests
where actor_user_id = pg_temp.protected_actor_id();
delete from net.http_request_queue;
delete from pgmq.q_dataset_extraction_jobs;
delete from pgmq.q_embedding_jobs;
delete from util.pending_embedding_jobs;
delete from vault.secrets
where name in ('project_secret_key', 'project_url');
select vault.create_secret(
  'protected-test-service-secret',
  'project_secret_key',
  'transaction-local protected execution test key'
);
select vault.create_secret(
  'http://127.0.0.1:54321',
  'project_url',
  'transaction-local protected execution test URL'
);

select set_config(
  'request.jwt.claim.sub',
  pg_temp.protected_actor_id()::text,
  true
);
select set_config(
  'request.jwt.claim.email',
  'protected-owner@example.com',
  true
);
select set_config('request.headers', '{}'::jsonb::text, true);

create temporary table protected_results (
  name text primary key,
  result jsonb not null
) on commit drop;

select plan(44);

select ok(
  not has_function_privilege(
    'anon',
    'public.cmd_dataset_alias_execution_preflight_guarded(jsonb)',
    'execute'
  )
  and has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_preflight_guarded(jsonb)',
    'execute'
  )
  and has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_gate_guarded(uuid,text,text)',
    'execute'
  )
  and has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_admit_guarded(jsonb)',
    'execute'
  )
  and has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_read(uuid)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_execution_execute(uuid,text)',
    'execute'
  )
  and has_function_privilege(
    'service_role',
    'public.cmd_dataset_alias_execution_execute(uuid,text)',
    'execute'
  ),
  'protected API grants expose only actor preflight/gate/admit/read and service executor'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_plan_guarded(jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'public.cmd_dataset_alias_plan_guarded(jsonb)',
    'execute'
  ),
  'legacy replay-capable alias RPC remains unavailable to authenticated and service roles'
);

select ok(
  not (pg_temp.protected_preflight_request(
    'a2000000-0000-0000-0000-000000000001'
  ) ? 'gate_expectations')
  and (
    select count(*)
    from jsonb_object_keys(pg_temp.protected_expected())
  ) = 10
  and pg_temp.protected_bindings('2026-07-15T12:00:00Z')
    ->>'alias_plan_request_sha256'
      = pg_temp.protected_alias_plan_request_sha256(),
  'client request has no gate expectations and binds the canonical alias request plus all ten counts'
);

select is(
  util.dataset_alias_execution_server_context()
    - 'project_url_sha256',
  '{"environment":"local","project_ref":"local"}'::jsonb,
  'trusted Vault project_url derives the local server context'
);

insert into protected_results values (
  'preflight',
  public.cmd_dataset_alias_execution_preflight_guarded(
    pg_temp.protected_preflight_request(
      'a2000000-0000-0000-0000-000000000001'
    )
  )
);

select ok(
  (select result @> '{"ok":true,"environment":"local","project_ref":"local","simulation":{"rolled_back":true,"plan_rows":52,"plan_exchanges":59,"alias_audits":55,"derivative_targets":50}}'::jsonb
   from protected_results where name = 'preflight')
  and (select result->>'preflight_token' ~ '^[a-f0-9]{64}$'
       from protected_results where name = 'preflight')
  and (select result->>'preflight_proof_sha256' ~ '^[a-f0-9]{64}$'
       from protected_results where name = 'preflight'),
  'preflight issues one server-clock token only after rollback simulation passes'
);

select ok(
  (select result->>'alias_plan_request_sha256'
       = pg_temp.protected_alias_plan_request_sha256()
   from protected_results where name = 'preflight')
  and (select result->>'freeze_sha256' = pg_temp.protected_freeze_sha256()
       from protected_results where name = 'preflight')
  and (select result->>'approval_identity_sha256'
       = pg_temp.protected_approval_identity_sha256('2026-07-15T12:00:00Z')
       from protected_results where name = 'preflight')
  and (select (
         select count(*)
         from jsonb_object_keys(result->'gate_expectations')
       ) = 3
       from protected_results where name = 'preflight'),
  'server proof binds canonical plan, freeze, approval, and three server-derived gate digests'
);

select ok(
  (
    select preflight.freeze_envelope = pg_temp.protected_freeze()
      and preflight.approval_envelope =
        pg_temp.protected_approval('2026-07-15T12:00:00Z')
      and preflight.bindings->>'alias_plan_request_sha256'
        = pg_temp.protected_alias_plan_request_sha256()
      and preflight.expires_at =
        preflight.completed_at + interval '180 seconds'
    from util.dataset_alias_execution_preflights as preflight
    where preflight.id = 'a2000000-0000-0000-0000-000000000001'
  ),
  'durable preflight stores the exact freeze, approval, alias hash, and 180-second server window'
);

select is(
  (select count(*)::text from public.command_audit_log)
  || ':' ||
  (select count(*)::text from util.dataset_derivative_rebuild_requests),
  '0:0',
  'rollback-only preflight leaves no alias audits or derivative child requests'
);

select ok(
  (
    select count(*) = 50
      and count(*) filter (
        where (select count(*) from jsonb_object_keys(target)) = 5
          and target ? 'expected_json_ordered_sha256'
          and not (target ? 'user_id')
          and not (target ? 'state_code')
      ) = 50
    from util.dataset_alias_execution_preflights as preflight
    cross join lateral jsonb_array_elements(preflight.derivative_targets) as target
    where preflight.id = 'a2000000-0000-0000-0000-000000000001'
  ),
  'preflight derives and persists only the internal five-field derivative target contract'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    pg_temp.protected_preflight_request(
      'a2000000-0000-0000-0000-000000000010'
    ) || jsonb_build_object('gate_expectations', '{}'::jsonb)
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_INVALID_REQUEST',
  'client-supplied gate expectations are rejected by the exact request schema'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      jsonb_set(
        pg_temp.protected_preflight_request(
          'a2000000-0000-0000-0000-000000000011'
        ),
        '{environment}',
        '"production"'::jsonb
      ),
      '{project_ref}',
      '"qgzvkongdjqiiamzbbts"'::jsonb
    )
  )->>'code',
  'ALIAS_EXECUTION_SERVER_CONTEXT_MISMATCH',
  'requested production context cannot substitute for the connected local database'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      pg_temp.protected_preflight_request(
        'a2000000-0000-0000-0000-000000000012'
      ),
      '{plan,batches,0,actions,1,desired_json_ordered,substituted}',
      'true'::jsonb,
      true
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_ARTIFACT_SET_MISMATCH',
  'a substituted plan payload is rejected by the canonical alias request hash'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      pg_temp.protected_preflight_request(
        'a2000000-0000-0000-0000-000000000013'
      ),
      '{freeze,policy,automatic_retry}',
      'true'::jsonb
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_FREEZE_MISMATCH',
  'a substituted freeze policy is rejected before simulation'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      pg_temp.protected_preflight_request(
        'a2000000-0000-0000-0000-000000000014'
      ),
      '{approval,operation_id}',
      '"substituted-operation"'::jsonb
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_APPROVAL_MISMATCH',
  'a substituted approval envelope is rejected before simulation'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      pg_temp.protected_preflight_request(
        'a2000000-0000-0000-0000-000000000015'
      ),
      '{actor,user_id}',
      '"a1000000-0000-0000-0000-000000000099"'::jsonb
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_ACTOR_MISMATCH',
  'preflight rejects an actor binding different from auth.uid'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      pg_temp.protected_preflight_request(
        'a2000000-0000-0000-0000-000000000016'
      ),
      '{derivative_targets}',
      (pg_temp.protected_targets() - 49)
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_INVALID_TARGETS',
  'preflight rejects a 49-target derivative set before simulation'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    jsonb_set(
      pg_temp.protected_preflight_request(
        'a2000000-0000-0000-0000-000000000017'
      ),
      '{derivative_targets,0,baseline_snapshot_sha256}',
      to_jsonb(repeat('f', 64))
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_ARTIFACT_SET_MISMATCH',
  'a derivative baseline substitution is rejected by its canonical set hash'
);

select is(
  public.cmd_dataset_alias_execution_preflight_guarded(
    pg_temp.protected_preflight_request(
      'a2000000-0000-0000-0000-000000000018'
    )
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_APPROVAL_ALREADY_USED',
  'the same sealed approval identity cannot create a second preflight attempt'
);

select is(
  public.cmd_dataset_alias_execution_gate_guarded(
    'a2000000-0000-0000-0000-000000000001',
    (select result->>'preflight_token'
     from protected_results where name = 'preflight'),
    'execution_unused'
  )->>'code',
  'ALIAS_EXECUTION_GATE_ORDER_MISMATCH',
  'server gates must be captured in their exact protected order'
);

insert into protected_results
select
  gate_name,
  public.cmd_dataset_alias_execution_gate_guarded(
    'a2000000-0000-0000-0000-000000000001',
    (select result->>'preflight_token'
     from protected_results where name = 'preflight'),
    gate_name
  )
from unnest(array[
  'primary_support_plan',
  'execution_unused',
  'derivative_quiescence'
]) with ordinality as gate(gate_name, ordinal)
order by ordinal;

select is(
  (select result->>'status'
   from protected_results where name = 'primary_support_plan'),
  'passed',
  'primary/support gate reruns the rollback-only exact-plan simulation'
);

select is(
  (select result->>'status'
   from protected_results where name = 'execution_unused'),
  'passed',
  'execution-unused gate proves zero sealed attempts, alias audits, and derivative children'
);

select is(
  (select result->>'status'
   from protected_results where name = 'derivative_quiescence'),
  'passed',
  'derivative-quiescence gate proves stable baselines and zero target work residue'
);

select ok(
  (
    select count(*) = 3
      and count(*) filter (
        where receipt.expected_sha256 = receipt.observed_sha256
          and receipt.expected_sha256 = case receipt.gate_name
            when 'primary_support_plan' then
              preflight.gate_expectations->>'primary_support_plan_sha256'
            when 'execution_unused' then
              preflight.gate_expectations->>'execution_unused_sha256'
            else
              preflight.gate_expectations->>'derivative_quiescence_sha256'
          end
      ) = 3
    from util.dataset_alias_execution_gate_receipts as receipt
    join util.dataset_alias_execution_preflights as preflight
      on preflight.id = receipt.preflight_id
    where receipt.preflight_id = 'a2000000-0000-0000-0000-000000000001'
  ),
  'all three persisted receipts exactly match server-derived expectations'
);

insert into protected_results values (
  'server_expectation_tamper_preflight',
  public.cmd_dataset_alias_execution_preflight_guarded(
    pg_temp.protected_preflight_request(
      'a2000000-0000-0000-0000-000000000019',
      '2026-07-15T12:00:01Z'
    )
  )
);

update util.dataset_alias_execution_preflights
set gate_expectations = jsonb_set(
  gate_expectations,
  '{primary_support_plan_sha256}',
  to_jsonb(repeat('f', 64))
)
where id = 'a2000000-0000-0000-0000-000000000019';

select ok(
  public.cmd_dataset_alias_execution_gate_guarded(
    'a2000000-0000-0000-0000-000000000019',
    (select result->>'preflight_token'
     from protected_results where name = 'server_expectation_tamper_preflight'),
    'primary_support_plan'
  )->>'code' = 'ALIAS_EXECUTION_GATE_EVIDENCE_MISMATCH'
  and not exists (
    select 1
    from util.dataset_alias_execution_gate_receipts
    where preflight_id = 'a2000000-0000-0000-0000-000000000019'
  ),
  'tampered durable server expectation fails closed without persisting a receipt'
);

insert into protected_results values (
  'expired_preflight',
  public.cmd_dataset_alias_execution_preflight_guarded(
    pg_temp.protected_preflight_request(
      'a2000000-0000-0000-0000-000000000020',
      '2026-07-15T12:00:02Z'
    )
  )
);

update util.dataset_alias_execution_preflights
set
  completed_at = completed_at - interval '181 seconds',
  expires_at = expires_at - interval '181 seconds'
where id = 'a2000000-0000-0000-0000-000000000020';

select is(
  public.cmd_dataset_alias_execution_gate_guarded(
    'a2000000-0000-0000-0000-000000000020',
    (select result->>'preflight_token'
     from protected_results where name = 'expired_preflight'),
    'primary_support_plan'
  )->>'code',
  'ALIAS_EXECUTION_PREFLIGHT_EXPIRED',
  'server rejects gate capture after the exact 180-second preflight window'
);

insert into protected_results values (
  'admission',
  public.cmd_dataset_alias_execution_admit_guarded(jsonb_build_object(
    'schema_version', 'dataset-alias-execution-admit.v1',
    'request_id', 'a2000000-0000-0000-0000-000000000001',
    'preflight_token', (
      select result->>'preflight_token'
      from protected_results where name = 'preflight'
    ),
    'preflight_proof_sha256', (
      select result->>'preflight_proof_sha256'
      from protected_results where name = 'preflight'
    ),
    'gate_results', (
      select jsonb_object_agg(
        receipt.gate_name,
        jsonb_build_object(
          'expected_sha256', receipt.expected_sha256,
          'observed_sha256', receipt.observed_sha256,
          'status', receipt.status,
          'captured_at', receipt.captured_at
        )
      )
      from util.dataset_alias_execution_gate_receipts as receipt
      where receipt.preflight_id = 'a2000000-0000-0000-0000-000000000001'
    )
  ))
);

select ok(
  (select result @> '{"ok":true,"status":"dispatched","attempt_count":1,"dispatch_count":1,"attempt_consumed":true,"retry_allowed":false}'::jsonb
   from protected_results where name = 'admission'),
  'admission consumes exact persisted receipts and returns one non-retryable dispatch'
);

select is(
  (
    select attempt_count::text || ':' || dispatch_count::text || ':' || status
    from util.dataset_alias_execution_requests
    where id = 'a2000000-0000-0000-0000-000000000001'
  ),
  '1:1:dispatched',
  'durable execution ledger fixes attempt_count and dispatch_count at one'
);

select is(
  (
    select count(*)::text
    from net.http_request_queue as queued
    join util.dataset_alias_execution_requests as request
      on request.net_request_id = queued.id
    where request.id = 'a2000000-0000-0000-0000-000000000001'
  ),
  '1',
  'admission transaction creates exactly one pg_net service dispatch'
);

select ok(
  public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000001'
  ) @> '{
    "ok": true,
    "status": "pending",
    "execution_status": "dispatched",
    "primary_readback": {
      "row_count": null,
      "exchange_count": null,
      "live_closure_proof": false,
      "closure": {
        "code": "ALIAS_EXECUTION_PRIMARY_CLOSURE_PENDING"
      }
    },
    "derivative_readback": {
      "status": "not_started",
      "proof_level": "none",
      "causal_terminal_proof": false
    }
  }'::jsonb,
  'the dispatch grace read is lightweight pending evidence and never races the one-shot executor'
);

select is(
  public.cmd_dataset_alias_execution_admit_guarded(
    jsonb_build_object(
      'schema_version', 'dataset-alias-execution-admit.v1',
      'request_id', 'a2000000-0000-0000-0000-000000000001',
      'preflight_token', (
        select result->>'preflight_token'
        from protected_results where name = 'preflight'
      ),
      'preflight_proof_sha256', (
        select result->>'preflight_proof_sha256'
        from protected_results where name = 'preflight'
      ),
      'gate_results', '{}'::jsonb
    )
  )->>'code',
  'ALIAS_EXECUTION_ATTEMPT_ALREADY_CONSUMED',
  'duplicate admission is rejected before gate parsing and never replayed'
);

select is(
  public.cmd_dataset_alias_execution_execute(
    'a2000000-0000-0000-0000-000000000001',
    repeat('0', 64)
  )->>'code',
  'SERVICE_ROLE_REQUIRED',
  'executor rejects non-service calls before reading the private nonce ledger'
);

select set_config(
  'request.headers',
  jsonb_build_object(
    'apikey', util.project_secret_key(),
    'authorization', 'Bearer ' || util.project_secret_key()
  )::text,
  true
);

insert into protected_results
select
  'executor',
  public.cmd_dataset_alias_execution_execute(
    'a2000000-0000-0000-0000-000000000001',
    convert_from(queued.body, 'UTF8')::jsonb->>'p_nonce'
  )
from net.http_request_queue as queued
join util.dataset_alias_execution_requests as request
  on request.net_request_id = queued.id
where request.id = 'a2000000-0000-0000-0000-000000000001';

select ok(
  (select result @> '{"ok":true,"status":"derivatives_pending","row_count":52,"exchange_count":59,"alias_audit_count":55,"derivative_target_count":50}'::jsonb
   from protected_results where name = 'executor'),
  'service executor atomically commits exact primary, audit, and derivative-admission closure'
);

select ok(
  (
    select proof @> '{"ok":true,"live_closure_proof":true,"batch_count":2,"action_count":52,"distinct_action_count":52,"flowproperty_count":2,"flow_count":23,"process_count":27,"support_reference_count":6,"flowproperty_support_count":2,"unitgroup_support_count":2,"source_unitgroup_support_count":2,"invalid_action_count":0,"invalid_support_count":0,"row_count":52,"exchange_count":59}'::jsonb
    from (
      select util.read_dataset_alias_execution_primary_closure(
        pg_temp.protected_actor_id(),
        pg_temp.protected_plan()
      ) as proof
    ) as closure
  ),
  'live primary closure proves all 52 desired actions and all six support occurrences'
);

select is(
  (
    select status || ':' ||
      (alias_result->'primary_closure'->>'live_closure_proof') || ':' ||
      (select count(*)::text
       from public.command_audit_log
       where actor_user_id = pg_temp.protected_actor_id())
    from util.dataset_alias_execution_requests
    where id = 'a2000000-0000-0000-0000-000000000001'
  ),
  'derivatives_pending:true:55',
  'executor persists its primary closure and exact 55-row audit set'
);

update util.dataset_alias_execution_requests
set
  status = 'completed',
  terminal_at = pg_catalog.clock_timestamp(),
  updated_at = pg_catalog.clock_timestamp()
where id = 'a2000000-0000-0000-0000-000000000001';

select ok(
  public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000001'
  ) @> '{"ok":true,"status":"passed","execution_status":"completed","primary_readback":{"row_count":52,"exchange_count":59,"alias_audit_count":55,"live_closure_proof":true},"derivative_readback":{"status":"completed","target_count":50,"causal_terminal_proof":true}}'::jsonb,
  'stored completion independently reads back live primary and derivative closure'
);

set local session_replication_role = replica;
update public.flows
set
  json = '{"drifted":true}'::jsonb,
  json_ordered = '{"drifted":true}'::json
where id = pg_temp.protected_entity_id('flows', 1)
  and version = '00.00.001';
set local session_replication_role = origin;

select ok(
  not coalesce((util.read_dataset_alias_execution_primary_closure(
    pg_temp.protected_actor_id(),
    pg_temp.protected_plan()
  )->>'live_closure_proof')::boolean, false)
  and public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000001'
  ) @> '{"status":"failed","execution_status":"completed","primary_readback":{"live_closure_proof":false}}'::jsonb,
  'one desired action drift makes both helper and stored-completion read fail closed'
);

set local session_replication_role = replica;
update public.flows
set
  json = pg_temp.protected_action_payload('flows', 1),
  json_ordered = pg_temp.protected_action_payload('flows', 1)::json
where id = pg_temp.protected_entity_id('flows', 1)
  and version = '00.00.001';
update public.unitgroups
set modified_at = modified_at + interval '1 second'
where id = pg_temp.protected_entity_id('source_unitgroups', 2)
  and version = '00.00.001';
set local session_replication_role = origin;

select ok(
  not coalesce((util.read_dataset_alias_execution_primary_closure(
    pg_temp.protected_actor_id(),
    pg_temp.protected_plan()
  )->>'live_closure_proof')::boolean, false)
  and public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000001'
  ) @> '{"status":"failed","execution_status":"completed","primary_readback":{"live_closure_proof":false}}'::jsonb,
  'one of six unchanged support occurrences drifting also fails closed'
);

set local session_replication_role = replica;
update public.unitgroups
set modified_at = '2026-07-15T00:00:00Z'::timestamptz
where id = pg_temp.protected_entity_id('source_unitgroups', 2)
  and version = '00.00.001';
set local session_replication_role = origin;

select is(
  public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000001'
  )->>'status',
  'passed',
  'restoring action and support closure restores independent readback proof'
);

select is(
  public.cmd_dataset_alias_execution_execute(
    'a2000000-0000-0000-0000-000000000001',
    (
      select convert_from(queued.body, 'UTF8')::jsonb->>'p_nonce'
      from net.http_request_queue as queued
      join util.dataset_alias_execution_requests as request
        on request.net_request_id = queued.id
      where request.id = 'a2000000-0000-0000-0000-000000000001'
    )
  )->>'code',
  'ALIAS_EXECUTION_ALREADY_STARTED',
  'a second service delivery cannot start the consumed executor again'
);

select is(
  (
    select count(*)::text
    from net.http_request_queue as queued
    join util.dataset_alias_execution_requests as request
      on request.net_request_id = queued.id
    where request.id = 'a2000000-0000-0000-0000-000000000001'
  ),
  '1',
  'all read, drift, expiry, and duplicate paths preserve the one-dispatch invariant'
);

-- Simulate an executor commit becoming visible after the expired-grace read
-- cached zero effect counts.  A VOLATILE helper provides the deterministic
-- snapshot transition without requiring a second test connection.
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
  'a2000000-0000-0000-0000-000000000002'::uuid,
  actor_user_id,
  actor_email,
  environment,
  project_ref,
  target_visibility,
  plan,
  freeze_envelope,
  approval_envelope,
  util.dataset_alias_execution_sha256('transition-plan'),
  'transition-read-test',
  util.dataset_alias_execution_sha256('transition-plan-request'),
  bindings,
  util.dataset_alias_execution_sha256('transition-bindings'),
  expected,
  util.dataset_alias_execution_sha256('transition-expected'),
  derivative_targets,
  util.dataset_alias_execution_sha256('transition-targets'),
  gate_expectations,
  util.dataset_alias_execution_sha256('transition-gates'),
  util.dataset_alias_execution_sha256('transition-failures'),
  util.dataset_alias_execution_sha256('transition-preflight-request'),
  util.dataset_alias_execution_sha256('transition-preflight-proof'),
  util.dataset_alias_execution_sha256('transition-freeze'),
  util.dataset_alias_execution_sha256('transition-approval'),
  util.dataset_alias_execution_sha256('transition-token'),
  pg_catalog.statement_timestamp() - interval '300 seconds',
  pg_catalog.statement_timestamp() - interval '120 seconds',
  pg_catalog.statement_timestamp() - interval '299 seconds',
  pg_catalog.statement_timestamp() - interval '300 seconds'
from util.dataset_alias_execution_preflights
where id = 'a2000000-0000-0000-0000-000000000001';

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
  dispatched_at,
  created_at,
  updated_at
) values (
  'a2000000-0000-0000-0000-000000000002',
  pg_temp.protected_actor_id(),
  util.dataset_alias_execution_sha256('transition-plan'),
  'transition-read-test',
  util.dataset_alias_execution_sha256('transition-plan-request'),
  util.dataset_alias_execution_sha256('transition-freeze'),
  util.dataset_alias_execution_sha256('transition-approval'),
  util.dataset_alias_execution_sha256('transition-approval-text'),
  util.dataset_alias_execution_sha256('transition-target-set'),
  util.dataset_alias_execution_sha256('transition-preflight-proof'),
  util.dataset_alias_execution_sha256('transition-admission'),
  '{}'::jsonb,
  util.dataset_alias_execution_sha256('{}'),
  util.dataset_alias_execution_sha256('transition-nonce'),
  1,
  1,
  9223372036854775000,
  'dispatched',
  pg_catalog.clock_timestamp() - interval '121 seconds',
  pg_catalog.clock_timestamp() - interval '121 seconds',
  pg_catalog.clock_timestamp() - interval '121 seconds',
  pg_catalog.clock_timestamp() - interval '121 seconds'
);

create or replace function util.read_dataset_alias_execution_primary_closure(
  p_actor uuid,
  p_plan jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $$
begin
  update util.dataset_alias_execution_requests
  set
    status = 'derivatives_pending',
    primary_committed_at = pg_catalog.clock_timestamp(),
    updated_at = pg_catalog.clock_timestamp()
  where id = 'a2000000-0000-0000-0000-000000000002'
    and actor_user_id = p_actor
    and status = 'dispatched';

  return jsonb_build_object(
    'ok', true,
    'schema_version', 'dataset-alias-primary-closure.v1',
    'row_count', 52,
    'exchange_count', 59,
    'live_closure_proof', true
  );
end;
$$;

create temporary table transition_readback_result as
select public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000002'
  ) as result;

select ok(
  (
    select result @> '{
    "ok": false,
    "code": "ALIAS_EXECUTION_READ_STATE_CHANGED",
    "status": 409,
    "execution_status": "derivatives_pending",
    "retry_allowed": false,
    "read_retry_allowed": true
  }'::jsonb
    from transition_readback_result
  )
  and (
    select status = 'derivatives_pending'
      and terminal_at is null
    from util.dataset_alias_execution_requests
    where id = 'a2000000-0000-0000-0000-000000000002'
  ),
  'expired-grace state transition defers cached evidence instead of misclassifying the committed execution'
);

-- A terminal parent can legitimately have no derivative children if the
-- one-shot executor failed or became indeterminate before atomic admission.
-- Both terminal states share the exact parseable not-started child envelope;
-- the parent status remains the authoritative failure classification.
update util.dataset_alias_execution_requests
set
  status = 'failed',
  terminal_at = pg_catalog.clock_timestamp(),
  terminal_proof = null,
  last_error = jsonb_build_object(
    'phase', 'executor',
    'code', 'TEST_EXECUTOR_FAILED_BEFORE_DERIVATIVES'
  ),
  updated_at = pg_catalog.clock_timestamp()
where id = 'a2000000-0000-0000-0000-000000000002';

create temporary table zero_child_failed_readback_result as
select public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000002'
  ) as result;

select ok(
  (
    select result @> '{
      "ok": true,
      "status": "failed",
      "execution_status": "failed",
      "retry_allowed": false
    }'::jsonb
    and result->'derivative_readback' = jsonb_build_object(
      'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
      'batch_id', 'a2000000-0000-0000-0000-000000000002'::uuid,
      'status', 'not_started',
      'code', 'DERIVATIVE_BATCH_NOT_STARTED',
      'proof_level', 'none',
      'proof_deferred', false,
      'target_count', 0,
      'flow_count', 0,
      'process_count', 0,
      'completed_count', 0,
      'nonterminal_count', 0,
      'failed_count', 0,
      'invalid_proof_count', null,
      'causal_terminal_proof', false,
      'targets', '[]'::jsonb
    )
    from zero_child_failed_readback_result
  ),
  'zero-child failed execution returns the exact not-started derivative contract'
);

update util.dataset_alias_execution_requests
set
  status = 'indeterminate',
  terminal_at = pg_catalog.clock_timestamp(),
  terminal_proof = null,
  last_error = jsonb_build_object(
    'phase', 'reconcile',
    'code', 'TEST_EXECUTOR_INDETERMINATE_BEFORE_DERIVATIVES'
  ),
  updated_at = pg_catalog.clock_timestamp()
where id = 'a2000000-0000-0000-0000-000000000002';

create temporary table zero_child_indeterminate_readback_result as
select public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000002'
  ) as result;

select ok(
  (
    select result @> '{
      "ok": true,
      "status": "indeterminate",
      "execution_status": "indeterminate",
      "retry_allowed": false
    }'::jsonb
    and result->'derivative_readback' = jsonb_build_object(
      'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
      'batch_id', 'a2000000-0000-0000-0000-000000000002'::uuid,
      'status', 'not_started',
      'code', 'DERIVATIVE_BATCH_NOT_STARTED',
      'proof_level', 'none',
      'proof_deferred', false,
      'target_count', 0,
      'flow_count', 0,
      'process_count', 0,
      'completed_count', 0,
      'nonterminal_count', 0,
      'failed_count', 0,
      'invalid_proof_count', null,
      'causal_terminal_proof', false,
      'targets', '[]'::jsonb
    )
    from zero_child_indeterminate_readback_result
  ),
  'zero-child indeterminate execution returns the exact not-started derivative contract'
);

-- Simulate another status reader classifying the parent while this reader is
-- assembling the expensive derivative proof.  The stale completed proof must
-- be discarded and the newly visible terminal ledger must remain untouched.
update util.dataset_alias_execution_requests
set
  status = 'derivatives_pending',
  terminal_at = null,
  terminal_proof = null,
  last_error = null,
  updated_at = pg_catalog.clock_timestamp()
where id = 'a2000000-0000-0000-0000-000000000001';

create or replace function util.read_dataset_derivative_rebuild_batch(
  p_actor_user_id uuid,
  p_batch_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $$
begin
  update util.dataset_alias_execution_requests
  set
    status = 'failed',
    terminal_at = pg_catalog.clock_timestamp(),
    last_error = jsonb_build_object(
      'phase', 'concurrent_reader',
      'code', 'TEST_CONCURRENT_READER_CLASSIFICATION'
    ),
    updated_at = pg_catalog.clock_timestamp()
  where id = p_batch_id
    and id = 'a2000000-0000-0000-0000-000000000001'
    and actor_user_id = p_actor_user_id
    and status = 'derivatives_pending';

  return jsonb_build_object(
    'ok', true,
    'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
    'batch_id', p_batch_id,
    'status', 'completed',
    'code', 'DERIVATIVE_BATCH_COMPLETED',
    'causal_terminal_proof', true,
    'target_count', 50,
    'flow_count', 23,
    'process_count', 27,
    'completed_count', 50,
    'nonterminal_count', 0,
    'failed_count', 0,
    'targets', '[]'::jsonb
  );
end;
$$;

create temporary table post_proof_transition_readback_result as
select public.cmd_dataset_alias_execution_read(
    'a2000000-0000-0000-0000-000000000001'
  ) as result;

select ok(
  (
    select result @> '{
      "ok": false,
      "code": "ALIAS_EXECUTION_READ_STATE_CHANGED",
      "status": 409,
      "execution_status": "failed",
      "retry_allowed": false,
      "read_retry_allowed": true
    }'::jsonb
    from post_proof_transition_readback_result
  )
  and (
    select status = 'failed'
      and terminal_at is not null
      and last_error->>'code' = 'TEST_CONCURRENT_READER_CLASSIFICATION'
    from util.dataset_alias_execution_requests
    where id = 'a2000000-0000-0000-0000-000000000001'
  ),
  'post-proof parent transition returns a read-only conflict without applying stale proof'
);

select * from finish();
rollback;
