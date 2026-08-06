CREATE OR REPLACE FUNCTION "api"."cmd_dataset_alias_execution_read"("p_request_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '2s'
    SET "statement_timeout" TO '60s'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_preflight util.dataset_alias_execution_preflights%rowtype;
  v_request util.dataset_alias_execution_requests%rowtype;
  v_gate_receipts jsonb := '[]'::jsonb;
  v_gate_count integer := 0;
  v_alias_audit_count integer := 0;
  v_derivative_child_count integer := 0;
  v_derivative_flow_count integer := 0;
  v_derivative_process_count integer := 0;
  v_primary_closure jsonb;
  v_primary_closure_ok boolean := false;
  v_active_dispatch_grace boolean := false;
  v_initial_request_status text;
  v_initial_request_updated_at timestamp with time zone;
  v_proof_request_status text;
  v_proof_request_updated_at timestamp with time zone;
  v_request_changed_during_read boolean := false;
  v_batch_proof_read boolean := false;
  v_terminal_update_count integer := 0;
  v_terminal_update_status text;
  v_batch_proof jsonb;
  v_category text;
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

  if p_request_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_READ_INVALID_REQUEST',
      'status', 400,
      'message', 'Exact protected execution request ID is required'
    );
  end if;

  select preflight.*
  into v_preflight
  from util.dataset_alias_execution_preflights as preflight
  where preflight.id = p_request_id
    and preflight.actor_user_id = v_actor;

  if v_preflight.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_REQUEST_NOT_FOUND',
      'status', 404,
      'message', 'No actor-owned protected preflight or execution exists'
    );
  end if;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'gate', receipt.gate_name,
          'expected_sha256', receipt.expected_sha256,
          'observed_sha256', receipt.observed_sha256,
          'status', receipt.status,
          'captured_at', receipt.captured_at,
          'receipt_sha256', receipt.receipt_sha256
        ) order by receipt.captured_at, receipt.gate_name
      ),
      '[]'::jsonb
    ),
    count(*)::integer
  into v_gate_receipts, v_gate_count
  from util.dataset_alias_execution_gate_receipts as receipt
  where receipt.preflight_id = p_request_id
    and receipt.actor_user_id = v_actor;

  select request.*
  into v_request
  from util.dataset_alias_execution_requests as request
  where request.id = p_request_id
    and request.actor_user_id = v_actor;

  if v_request.id is null then
    return jsonb_build_object(
      'ok', true,
      'command', 'cmd_dataset_alias_execution_read',
      'schema_version', 'dataset-alias-execution-status.v1',
      'request_id', p_request_id,
      'status', 'indeterminate',
      'execution_status', 'not_admitted',
      'code', case
        when v_preflight.consumed_at is null
          then 'ALIAS_EXECUTION_NOT_ADMITTED'
        else 'ALIAS_EXECUTION_ADMISSION_LEDGER_MISSING'
      end,
      'retry_allowed', false,
      'actor_user_id', v_actor,
      'environment', v_preflight.environment,
      'project_ref', v_preflight.project_ref,
      'plan_sha256', v_preflight.plan_sha256,
      'operation_id', v_preflight.operation_id,
      'plan_request_sha256', v_preflight.plan_request_sha256,
      'preflight_proof_sha256', v_preflight.preflight_proof_sha256,
      'preflight_completed_at', v_preflight.completed_at,
      'preflight_expires_at', v_preflight.expires_at,
      'preflight_consumed_at', v_preflight.consumed_at,
      'gate_count', v_gate_count,
      'gates', v_gate_receipts
    );
  end if;

  v_initial_request_status := v_request.status;
  v_initial_request_updated_at := v_request.updated_at;

  v_active_dispatch_grace :=
    v_request.status in ('dispatching', 'dispatched', 'running')
    and v_now <= v_request.admitted_at + interval '120 seconds';

  if v_active_dispatch_grace then
    -- Do not run the heavyweight live closure while the one-shot executor may
    -- be committing.  Apart from wasting work, a status lock/readback race
    -- must never delay or misclassify the only authorized mutation attempt.
    v_primary_closure := jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-alias-primary-closure.v1',
      'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_PENDING',
      'live_closure_proof', false
    );
  else

  select count(*)::integer
  into v_alias_audit_count
  from private.command_audit_log as audit
  where audit.actor_user_id = v_actor
    and (
      (
        audit.command = 'cmd_dataset_alias_batch_guarded'
        and audit.payload->>'plan_sha256' = v_request.plan_sha256
        and audit.payload->>'operation_id' = v_request.operation_id
        and audit.payload->>'record_type' in ('row', 'batch_summary')
      )
      or (
        audit.command = 'cmd_dataset_alias_plan_guarded'
        and audit.payload->>'plan_request_sha256' =
          v_request.plan_request_sha256
        and audit.payload->>'record_type' = 'plan_summary'
      )
    );

  select
    count(*)::integer,
    count(*) filter (where target_table = 'flows')::integer,
    count(*) filter (where target_table = 'processes')::integer
  into
    v_derivative_child_count,
    v_derivative_flow_count,
    v_derivative_process_count
  from util.dataset_derivative_rebuild_requests as child
  where child.actor_user_id = v_actor
    and child.batch_id = p_request_id;

  v_primary_closure :=
    util.read_dataset_alias_execution_primary_closure(
      v_actor,
      v_preflight.plan
    );
  v_primary_closure_ok := coalesce(
    (v_primary_closure->>'live_closure_proof')::boolean,
    false
  );

  if v_request.status in ('dispatching', 'dispatched', 'running') then
    if v_alias_audit_count = 55
      and v_derivative_child_count = 50
      and v_derivative_flow_count = 23
      and v_derivative_process_count = 27
      and v_primary_closure_ok then
      update util.dataset_alias_execution_requests
      set
        status = 'derivatives_pending',
        primary_committed_at = coalesce(primary_committed_at, updated_at),
        updated_at = v_now
      where id = p_request_id
        and status in ('dispatching', 'dispatched', 'running');
    elsif (
        v_alias_audit_count > 0
        or v_derivative_child_count > 0
      ) and not v_primary_closure_ok then
      update util.dataset_alias_execution_requests
      set
        status = 'indeterminate',
        terminal_at = v_now,
        last_error = jsonb_build_object(
          'phase', 'reconcile',
          'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_MISMATCH',
          'primary_closure', v_primary_closure,
          'retry_allowed', false
        ),
        updated_at = v_now
      where id = p_request_id
        and status in ('dispatching', 'dispatched', 'running');
    elsif v_now > v_request.admitted_at + interval '120 seconds' then
      update util.dataset_alias_execution_requests
      set
        status = 'indeterminate',
        terminal_at = v_now,
        last_error = jsonb_build_object(
          'phase', 'reconcile',
          'code', 'ALIAS_EXECUTION_DISPATCH_OUTCOME_INDETERMINATE',
          'alias_audit_count', v_alias_audit_count,
          'derivative_child_count', v_derivative_child_count,
          'retry_allowed', false
        ),
        updated_at = v_now
      where id = p_request_id
        and status in ('dispatching', 'dispatched', 'running');
    end if;
  end if;

  select request.*
  into v_request
  from util.dataset_alias_execution_requests as request
  where request.id = p_request_id
    and request.actor_user_id = v_actor;

  v_request_changed_during_read :=
    v_request.status is distinct from v_initial_request_status
    or v_request.updated_at is distinct from v_initial_request_updated_at;

  if v_request_changed_during_read then
    -- VOLATILE PL/pgSQL statements can observe different READ COMMITTED
    -- snapshots.  If the executor or this reconciliation pass advanced the
    -- ledger after the first read, none of the evidence cached above is safe
    -- to use for another monotonic classification.  Return an explicit
    -- read-only conflict so the caller can poll again from the new state;
    -- execution admission and dispatch remain permanently non-retryable.
    return jsonb_build_object(
      'ok', false,
      'command', 'cmd_dataset_alias_execution_read',
      'schema_version', 'dataset-alias-execution-status.v1',
      'request_id', p_request_id,
      'code', 'ALIAS_EXECUTION_READ_STATE_CHANGED',
      'status', 409,
      'execution_status', v_request.status,
      'retry_allowed', false,
      'read_retry_allowed', true,
      'message', 'Execution state changed during readback; poll status again without redispatching'
    );
  end if;

  v_proof_request_status := v_request.status;
  v_proof_request_updated_at := v_request.updated_at;

  if v_derivative_child_count > 0
    or v_request.status in ('derivatives_pending', 'completed') then
    v_batch_proof_read := true;
    v_batch_proof := util.read_dataset_derivative_rebuild_batch(
      v_actor,
      p_request_id
    );

    select request.*
    into v_request
    from util.dataset_alias_execution_requests as request
    where request.id = p_request_id
      and request.actor_user_id = v_actor;

    if v_request.status is distinct from v_proof_request_status
      or v_request.updated_at is distinct from v_proof_request_updated_at then
      -- The derivative proof can be substantially more expensive than the
      -- parent-ledger read.  A different reader or the executor may classify
      -- the request while that proof is being assembled, so the cached proof
      -- must not be applied to the newly visible parent state.
      return jsonb_build_object(
        'ok', false,
        'command', 'cmd_dataset_alias_execution_read',
        'schema_version', 'dataset-alias-execution-status.v1',
        'request_id', p_request_id,
        'code', 'ALIAS_EXECUTION_READ_STATE_CHANGED',
        'status', 409,
        'execution_status', v_request.status,
        'retry_allowed', false,
        'read_retry_allowed', true,
        'message', 'Execution state changed during readback; poll status again without redispatching'
      );
    end if;
  end if;

  if v_request.status = 'derivatives_pending' then
    if v_alias_audit_count <> 55
      or v_derivative_child_count <> 50
      or v_derivative_flow_count <> 23
      or v_derivative_process_count <> 27
      or not v_primary_closure_ok then
      update util.dataset_alias_execution_requests
      set
        status = 'indeterminate',
        terminal_at = v_now,
        terminal_proof = jsonb_build_object(
          'primary_closure', v_primary_closure,
          'derivative_closure', v_batch_proof
        ),
        last_error = jsonb_build_object(
          'phase', 'readback',
          'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_MISMATCH',
          'alias_audit_count', v_alias_audit_count,
          'derivative_child_count', v_derivative_child_count,
          'primary_closure', v_primary_closure
        ),
        updated_at = v_now
      where id = p_request_id
        and status = 'derivatives_pending';
      get diagnostics v_terminal_update_count = row_count;
      if v_terminal_update_count = 1 then
        v_terminal_update_status := 'indeterminate';
      end if;
    elsif v_batch_proof->>'status' = 'completed'
      and coalesce((v_batch_proof->>'causal_terminal_proof')::boolean, false) then
      update util.dataset_alias_execution_requests
      set
        status = 'completed',
        terminal_at = v_now,
        terminal_proof = jsonb_build_object(
          'primary_closure', v_primary_closure,
          'derivative_closure', v_batch_proof
        ),
        updated_at = v_now
      where id = p_request_id
        and status = 'derivatives_pending';
      get diagnostics v_terminal_update_count = row_count;
      if v_terminal_update_count = 1 then
        v_terminal_update_status := 'completed';
      end if;
    elsif v_batch_proof->>'status' = 'failed' then
      update util.dataset_alias_execution_requests
      set
        status = 'failed',
        terminal_at = v_now,
        terminal_proof = jsonb_build_object(
          'primary_closure', v_primary_closure,
          'derivative_closure', v_batch_proof
        ),
        last_error = jsonb_build_object(
          'phase', 'derivative_readback',
          'code', coalesce(
            v_batch_proof->>'code',
            'ALIAS_EXECUTION_DERIVATIVE_CLOSURE_FAILED'
          )
        ),
        updated_at = v_now
      where id = p_request_id
        and status = 'derivatives_pending';
      get diagnostics v_terminal_update_count = row_count;
      if v_terminal_update_count = 1 then
        v_terminal_update_status := 'failed';
      end if;
    end if;
  end if;

  select request.*
  into v_request
  from util.dataset_alias_execution_requests as request
  where request.id = p_request_id
    and request.actor_user_id = v_actor;

  if v_batch_proof_read
    and (
      v_request.status is distinct from v_proof_request_status
      or v_request.updated_at is distinct from v_proof_request_updated_at
    )
    and not (
      v_terminal_update_count = 1
      and v_request.status is not distinct from v_terminal_update_status
      and v_request.updated_at is not distinct from v_now
    ) then
    -- A conditional terminal update with ROW_COUNT = 1 is this invocation's
    -- own monotonic classification.  Any other parent transition invalidates
    -- the cached derivative proof and must be retried as read-only polling.
    return jsonb_build_object(
      'ok', false,
      'command', 'cmd_dataset_alias_execution_read',
      'schema_version', 'dataset-alias-execution-status.v1',
      'request_id', p_request_id,
      'code', 'ALIAS_EXECUTION_READ_STATE_CHANGED',
      'status', 409,
      'execution_status', v_request.status,
      'retry_allowed', false,
      'read_retry_allowed', true,
      'message', 'Execution state changed during readback; poll status again without redispatching'
    );
  end if;

  end if;

  v_category := case v_request.status
    when 'completed' then 'passed'
    when 'failed' then 'failed'
    when 'indeterminate' then 'indeterminate'
    else 'pending'
  end;

  -- A stored completion is not allowed to hide later live-state drift during
  -- an independent readback.  The immutable ledger remains completed, but the
  -- fresh response fails closed if its current causal proof no longer passes.
  if v_request.status = 'completed'
    and (
      not v_primary_closure_ok
      or v_batch_proof is null
      or v_batch_proof->>'status' is distinct from 'completed'
      or coalesce((v_batch_proof->>'causal_terminal_proof')::boolean, false)
        is not true
    ) then
    v_category := 'failed';
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_alias_execution_read',
    'schema_version', 'dataset-alias-execution-status.v1',
    'request_id', p_request_id,
    'status', v_category,
    'execution_status', v_request.status,
    'retry_allowed', false,
    'actor_user_id', v_actor,
    'environment', v_preflight.environment,
    'project_ref', v_preflight.project_ref,
    'target_visibility', v_preflight.target_visibility,
    'plan_sha256', v_request.plan_sha256,
    'operation_id', v_request.operation_id,
    'plan_request_sha256', v_request.plan_request_sha256,
    'freeze_sha256', v_request.freeze_sha256,
    'approval_identity_sha256', v_request.approval_identity_sha256,
    'approval_text_sha256', v_request.approval_text_sha256,
    'derivative_target_set_sha256', v_request.derivative_target_set_sha256,
    'server_derivative_targets_sha256',
      v_preflight.derivative_targets_sha256,
    'preflight_proof_sha256', v_request.preflight_proof_sha256,
    'admission_request_sha256', v_request.admission_request_sha256,
    'gate_results_sha256', v_request.gate_results_sha256,
    'attempt_count', v_request.attempt_count,
    'dispatch_count', v_request.dispatch_count,
    'net_request_id', v_request.net_request_id::text,
    'preflight_completed_at', v_preflight.completed_at,
    'preflight_expires_at', v_preflight.expires_at,
    'preflight_consumed_at', v_preflight.consumed_at,
    'admitted_at', v_request.admitted_at,
    'dispatched_at', v_request.dispatched_at,
    'started_at', v_request.started_at,
    'primary_committed_at', v_request.primary_committed_at,
    'terminal_at', v_request.terminal_at,
    'gate_count', v_gate_count,
    'gates', v_gate_receipts,
    'primary_readback', jsonb_build_object(
      'row_count', case
        when v_alias_audit_count = 55 and v_primary_closure_ok then 52
        else null
      end,
      'exchange_count', case
        when v_alias_audit_count = 55 and v_primary_closure_ok then 59
        else null
      end,
      'alias_audit_count', v_alias_audit_count,
      'live_closure_proof', v_primary_closure_ok,
      'closure', v_primary_closure
    ),
    'derivative_readback', coalesce(
      v_batch_proof,
      jsonb_build_object(
        'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
        'batch_id', p_request_id,
        'status', 'not_started',
        'code', 'DERIVATIVE_BATCH_NOT_STARTED',
        'proof_level', 'none',
        'proof_deferred', false,
        'target_count', v_derivative_child_count,
        'flow_count', v_derivative_flow_count,
        'process_count', v_derivative_process_count,
        'completed_count', 0,
        'nonterminal_count', 0,
        'failed_count', 0,
        'invalid_proof_count', null,
        'causal_terminal_proof', false,
        'targets', '[]'::jsonb
      )
    ),
    'error', v_request.last_error
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_READ_LOCK_BUSY',
      'status', 'indeterminate',
      'message', 'Protected execution status row is busy; readback did not retry or redispatch'
    );
end;
$$;

ALTER FUNCTION "api"."cmd_dataset_alias_execution_read"("p_request_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_alias_execution_read"("p_request_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_alias_execution_read"("p_request_id" "uuid") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_alias_execution_read"("p_request_id" "uuid") TO "authenticated";
