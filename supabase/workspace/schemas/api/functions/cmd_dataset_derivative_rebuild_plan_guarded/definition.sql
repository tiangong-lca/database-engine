CREATE OR REPLACE FUNCTION "api"."cmd_dataset_derivative_rebuild_plan_guarded"("p_plan" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_command constant text := 'cmd_dataset_derivative_rebuild_plan_guarded';
  v_schema_version constant text := 'dataset-derivative-rebuild-plan.v1';
  v_plan_sha256 text;
  v_operation_id text;
  v_action jsonb;
  v_action_id text;
  v_target_id uuid;
  v_target_version text;
  v_expected_snapshot_sha256 text;
  v_reason_code text;
  v_plan_request_sha256 text;
  v_action_request_sha256 text;
  v_existing util.dataset_derivative_rebuild_requests%rowtype;
  v_active_request_id uuid;
  v_process public.processes%rowtype;
  v_snapshot jsonb;
  v_quarantine jsonb;
  v_request_id uuid := pg_catalog.gen_random_uuid();
  v_action_audit_id bigint;
  v_summary_audit_id bigint;
  v_now timestamp with time zone := pg_catalog.clock_timestamp();
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_plan is not null and pg_column_size(p_plan) > 65536 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_REQUEST_TOO_LARGE',
      'status', 413,
      'message', 'Derivative rebuild plan exceeds 64 KiB'
    );
  end if;

  if jsonb_typeof(p_plan) is distinct from 'object'
    or not (p_plan ?& array[
      'schema_version',
      'plan_sha256',
      'operation_id',
      'target_visibility',
      'actions'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_plan) as plan_key(key)
      where plan_key.key <> all (array[
        'schema_version',
        'plan_sha256',
        'operation_id',
        'target_visibility',
        'actions'
      ])
    )
    or p_plan->>'schema_version' is distinct from v_schema_version
    or jsonb_typeof(p_plan->'plan_sha256') is distinct from 'string'
    or (p_plan->>'plan_sha256') !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_plan->'operation_id') is distinct from 'string'
    or nullif(btrim(p_plan->>'operation_id'), '') is null
    or octet_length(p_plan->>'operation_id') > 512
    or p_plan->>'target_visibility' is distinct from 'owner_draft'
    or jsonb_typeof(p_plan->'actions') is distinct from 'array'
    or jsonb_array_length(p_plan->'actions') <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_INVALID_REQUEST',
      'status', 400,
      'message', 'Plan must match dataset-derivative-rebuild-plan.v1 exactly'
    );
  end if;

  v_plan_sha256 := p_plan->>'plan_sha256';
  v_operation_id := btrim(p_plan->>'operation_id');
  v_action := p_plan->'actions'->0;

  if jsonb_typeof(v_action) is distinct from 'object'
    or not (v_action ?& array[
      'action_id',
      'action',
      'table',
      'id',
      'version',
      'expected_state_code',
      'expected_snapshot_sha256',
      'components',
      'reason_code'
    ])
    or exists (
      select 1
      from jsonb_object_keys(v_action) as action_key(key)
      where action_key.key <> all (array[
        'action_id',
        'action',
        'table',
        'id',
        'version',
        'expected_state_code',
        'expected_snapshot_sha256',
        'components',
        'reason_code'
      ])
    )
    or jsonb_typeof(v_action->'action_id') is distinct from 'string'
    or nullif(btrim(v_action->>'action_id'), '') is null
    or octet_length(v_action->>'action_id') > 512
    or v_action->>'action' is distinct from 'rebuild_derivatives'
    or v_action->>'table' is distinct from 'processes'
    or jsonb_typeof(v_action->'id') is distinct from 'string'
    or (v_action->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or jsonb_typeof(v_action->'version') is distinct from 'string'
    or (v_action->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(v_action->'expected_state_code') is distinct from 'number'
    or v_action->>'expected_state_code' <> '0'
    or jsonb_typeof(v_action->'expected_snapshot_sha256') is distinct from 'string'
    or (v_action->>'expected_snapshot_sha256') !~ '^[a-f0-9]{64}$'
    or v_action->'components'
      is distinct from '["extracted_md", "embedding_ft"]'::jsonb
    or jsonb_typeof(v_action->'reason_code') is distinct from 'string'
    or nullif(btrim(v_action->>'reason_code'), '') is null
    or octet_length(v_action->>'reason_code') > 512 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_INVALID_ACTION',
      'status', 400,
      'message', 'Exactly one owner-draft process rebuild action is required'
    );
  end if;

  v_action_id := btrim(v_action->>'action_id');
  v_target_id := (v_action->>'id')::uuid;
  v_target_version := btrim(v_action->>'version');
  v_expected_snapshot_sha256 := v_action->>'expected_snapshot_sha256';
  v_reason_code := btrim(v_action->>'reason_code');
  v_plan_request_sha256 := util.dataset_derivative_rebuild_sha256(p_plan::text);
  v_action_request_sha256 := util.dataset_derivative_rebuild_sha256(v_action::text);

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      v_actor::text || ':' || v_plan_request_sha256,
      0
    )
  );

  select request.*
  into v_existing
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = v_actor
    and request.plan_request_sha256 = v_plan_request_sha256;

  if v_existing.id is not null then
    if v_existing.plan_sha256 is distinct from v_plan_sha256
      or v_existing.operation_id is distinct from v_operation_id
      or v_existing.action_id is distinct from v_action_id
      or v_existing.target_id is distinct from v_target_id
      or v_existing.target_version is distinct from v_target_version
      or v_existing.expected_snapshot_sha256
        is distinct from v_expected_snapshot_sha256
      or v_existing.action_request_sha256
        is distinct from v_action_request_sha256 then
      return jsonb_build_object(
        'ok', false,
        'code', 'DERIVATIVE_PLAN_REPLAY_CONFLICT',
        'status', 409,
        'message', 'Existing admission does not match the exact plan'
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'command', v_command,
      'schema_version', v_schema_version,
      'plan_sha256', v_existing.plan_sha256,
      'operation_id', v_existing.operation_id,
      'target_visibility', v_existing.target_visibility,
      'plan_request_sha256', v_existing.plan_request_sha256,
      'action_count', 1,
      'accepted_count', 1,
      'summary_audit_id', v_existing.summary_audit_id::text,
      'request_id', v_existing.id::text,
      'status', 'queued',
      'action_request_sha256', v_existing.action_request_sha256,
      'database_audit_id', v_existing.action_audit_id::text,
      'idempotent_replay', true
    );
  end if;

  -- SHARE ROW EXCLUSIVE serializes statement snapshots around fence creation.
  -- A process writer either finishes before this frozen snapshot or begins
  -- after the nonterminal request/fence is committed and visible.
  lock table public.processes in share row exclusive mode;

  select process.*
  into v_process
  from public.processes as process
  where process.id = v_target_id
    and btrim(process.version::text) = v_target_version
    and process.user_id = v_actor
    and process.state_code = 0
  for update;

  if v_process.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_SCOPE_MISMATCH',
      'status', 409,
      'message', 'Action does not identify an exact current-owner draft process'
    );
  end if;

  begin
    v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception
    when others then
      return jsonb_build_object(
        'ok', false,
        'code', 'DERIVATIVE_PLAN_PRIMARY_NOT_READY',
        'status', 409,
        'message', 'Owner-draft process primary snapshot is incomplete'
      );
  end;

  if v_snapshot->>'json_sha256'
      is distinct from v_snapshot->>'json_ordered_sha256'
    or v_snapshot->>'snapshot_sha256'
      is distinct from v_expected_snapshot_sha256 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_SNAPSHOT_DRIFT',
      'status', 409,
      'message', 'Action snapshot changed before admission'
    );
  end if;

  select request.id
  into v_active_request_id
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = 'processes'
    and request.target_id = v_target_id
    and request.target_version = v_target_version
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_active_request_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_TARGET_BUSY',
      'status', 409,
      'message', 'Another derivative rebuild already fences this process'
    );
  end if;

  v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
    v_target_id,
    v_target_version
  );

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    v_command,
    v_actor,
    'processes',
    v_target_id,
    v_target_version,
    jsonb_build_object(
      'record_type', 'action',
      'schema_version', v_schema_version,
      'request_id', v_request_id,
      'plan_sha256', v_plan_sha256,
      'operation_id', v_operation_id,
      'action_id', v_action_id,
      'target_visibility', 'owner_draft',
      'expected_snapshot_sha256', v_expected_snapshot_sha256,
      'plan_request_sha256', v_plan_request_sha256,
      'action_request_sha256', v_action_request_sha256,
      'reason_code', v_reason_code,
      'components', jsonb_build_array('extracted_md', 'embedding_ft'),
      'hash_algorithm', 'postgres-jsonb-text-sha256'
    )
  )
  returning id into v_action_audit_id;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    v_command,
    v_actor,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', 'plan_summary',
      'schema_version', v_schema_version,
      'request_id', v_request_id,
      'plan_sha256', v_plan_sha256,
      'operation_id', v_operation_id,
      'target_visibility', 'owner_draft',
      'plan_request_sha256', v_plan_request_sha256,
      'action_request_sha256', v_action_request_sha256,
      'action_count', 1,
      'accepted_count', 1,
      'action_audit_id', v_action_audit_id::text,
      'hash_algorithm', 'postgres-jsonb-text-sha256'
    )
  )
  returning id into v_summary_audit_id;

  insert into util.dataset_derivative_rebuild_requests (
    id,
    actor_user_id,
    plan_sha256,
    operation_id,
    action_id,
    target_id,
    target_version,
    expected_snapshot_sha256,
    expected_modified_at,
    expected_json_sha256,
    expected_json_ordered_sha256,
    before_extracted_md_sha256,
    before_embedding_ft_sha256,
    before_embedding_ft_at,
    plan_request_sha256,
    action_request_sha256,
    reason_code,
    status,
    phase,
    admitted_at,
    drain_not_before,
    action_audit_id,
    summary_audit_id,
    quarantined_http_requests,
    quarantined_embedding_jobs,
    quarantined_pending_jobs
  ) values (
    v_request_id,
    v_actor,
    v_plan_sha256,
    v_operation_id,
    v_action_id,
    v_target_id,
    v_target_version,
    v_expected_snapshot_sha256,
    (v_snapshot->>'modified_at')::timestamp with time zone,
    v_snapshot->>'json_sha256',
    v_snapshot->>'json_ordered_sha256',
    v_snapshot->>'extracted_md_sha256',
    v_snapshot->>'embedding_ft_sha256',
    (v_snapshot->>'embedding_ft_at')::timestamp with time zone,
    v_plan_request_sha256,
    v_action_request_sha256,
    v_reason_code,
    'queued',
    'admitted',
    v_now,
    v_now + interval '420 seconds',
    v_action_audit_id,
    v_summary_audit_id,
    coalesce((v_quarantine->>'http_requests')::integer, 0),
    coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
    coalesce((v_quarantine->>'pending_jobs')::integer, 0)
  );

  return jsonb_build_object(
    'ok', true,
    'command', v_command,
    'schema_version', v_schema_version,
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'target_visibility', 'owner_draft',
    'plan_request_sha256', v_plan_request_sha256,
    'action_count', 1,
    'accepted_count', 1,
    'summary_audit_id', v_summary_audit_id::text,
    'request_id', v_request_id::text,
    'status', 'queued',
    'action_request_sha256', v_action_request_sha256,
    'database_audit_id', v_action_audit_id::text,
    'idempotent_replay', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_LOCK_BUSY',
      'status', 409,
      'message', 'Process write fence could not be acquired'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_CONCURRENT_CONFLICT',
      'status', 409,
      'message', 'A concurrent admission won the exact plan or target'
    );
end;
$_$;

ALTER FUNCTION "api"."cmd_dataset_derivative_rebuild_plan_guarded"("p_plan" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_derivative_rebuild_plan_guarded"("p_plan" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_derivative_rebuild_plan_guarded"("p_plan" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_derivative_rebuild_plan_guarded"("p_plan" "jsonb") TO "authenticated";
