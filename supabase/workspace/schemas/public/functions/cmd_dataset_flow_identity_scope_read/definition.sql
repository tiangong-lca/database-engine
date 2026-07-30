CREATE OR REPLACE FUNCTION "public"."cmd_dataset_flow_identity_scope_read"("p_scope_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '90s'
    AS $$
declare
  v_command constant text := 'cmd_dataset_flow_identity_scope_read';
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_core jsonb;
  v_whole_result jsonb;
  v_whole jsonb;
  v_processes jsonb;
  v_status text;
  v_live_drift boolean;
  v_terminal_conflict boolean;
  v_compensation_required boolean;
  v_result jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_READ_INVALID_REQUEST', 'status', 400,
      'message', 'Scope ID is required'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_scope.receipt_proof_sha256 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  v_core := private.dataset_flow_identity_scope_read_core_v1(p_scope_id);
  if v_core->>'code' in ('AUTH_REQUIRED', 'FLOW_IDENTITY_SCOPE_NOT_FOUND') then
    return v_core;
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, false
  );
  v_whole := v_whole_result->'whole_scope_proof';
  if v_whole is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
      'message', 'Whole-scope proof could not be constructed'
    );
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'ordinal', ledger.ordinal,
    'id', ledger.process_id,
    'version', ledger.process_version,
    'status', ledger.status,
    'process_request_sha256', ledger.process_request_sha256,
    'process_intent_proof_sha256', ledger.process_intent_proof_sha256,
    'rewrite_count', ledger.rewrite_count,
    'audit_id', case when ledger.audit_id is null
      then null else ledger.audit_id::text end,
    'before_payload_sha256', ledger.before_payload_sha256,
    'before_exchange_set_sha256',
      ledger.manifest->>'before_exchange_set_sha256',
    'desired_payload_sha256', ledger.manifest->>'desired_payload_sha256',
    'desired_exchange_set_sha256',
      ledger.manifest->>'desired_exchange_set_sha256',
    'after_payload_sha256', ledger.after_payload_sha256,
    'after_exchange_set_sha256', ledger.after_exchange_set_sha256,
    'derivative_batch_id', ledger.derivative_batch_id,
    'derivative_request_id', child.id,
    'derivative_status', case when ledger.derivative_batch_id is null
      then null else coalesce(child.status, 'missing') end,
    'causal_terminal_proof', false,
    'completed_at', ledger.completed_at,
    'last_error', coalesce(ledger.last_error, child.last_error)
  ) order by ledger.ordinal), '[]'::jsonb)
  into v_processes
  from util.dataset_flow_identity_process_ledger as ledger
  left join util.dataset_derivative_rebuild_requests as child
    on child.actor_user_id = v_actor
    and child.batch_id = ledger.derivative_batch_id
    and child.target_table = 'processes'
    and child.target_id = ledger.process_id
    and child.target_version = ledger.process_version
  where ledger.scope_id = p_scope_id;
  v_terminal_conflict := v_scope.status in ('cancelled', 'failed');
  v_live_drift := v_terminal_conflict
    or coalesce((v_whole_result->>'ok')::boolean, false) is false;
  v_status := case when v_live_drift then 'live_drift'
    else v_core->>'status' end;
  v_compensation_required := not v_live_drift
    and coalesce((v_core->>'derivative_failed_count')::integer, 0) > 0;
  v_result := jsonb_build_object(
    'ok', v_status not in ('failed', 'live_drift')
      and not v_compensation_required,
    'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-status.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_status,
    'process_count', (v_core->>'process_count')::integer,
    'completed_process_count',
      (v_core->>'completed_process_count')::integer,
    'pending_process_count', (v_core->>'pending_process_count')::integer,
    'failed_process_count', (v_core->>'failed_process_count')::integer,
    'next_ordinal', (v_core->>'next_ordinal')::integer,
    'rewrite_count', (v_core->>'rewrite_count')::integer,
    'completed_rewrite_count',
      (v_core->>'completed_rewrite_count')::integer,
    'primary_complete', (v_core->>'primary_complete')::boolean,
    'cancellable', (v_core->>'cancellable')::boolean,
    'strict_continuation_required',
      (v_core->>'strict_continuation_required')::boolean,
    'primary_current', (v_whole->>'primary_current')::boolean,
    'live_guard_current',
      (v_whole->>'audit_current')::boolean
      and (v_whole->>'source_guards_current')::boolean
      and (v_whole->>'support_guards_current')::boolean
      and (v_whole->>'target_guards_current')::boolean
      and (v_whole->>'protected_closure_current')::boolean
      and (v_whole->>'occurrence_closure_current')::boolean,
    'derivatives_current', case when v_live_drift then false
      else (v_whole->>'derivatives_current')::boolean end,
    'derivative_pending_count',
      (v_core->>'derivative_pending_count')::integer,
    'derivative_failed_count',
      (v_core->>'derivative_failed_count')::integer,
    'derivative_set_proof', v_whole_result->'derivative_set_proof',
    'derivative_proof_set_sha256',
      v_whole_result #>> '{derivative_set_proof,proof_sha256}',
    'compensation_required', v_compensation_required,
    'automatic_retry', false,
    'compensation_targets', case when v_compensation_required
      then v_core->'compensation_targets' else '[]'::jsonb end,
    'protected_closure_current',
      (v_whole->>'protected_closure_current')::boolean,
    'protected_closure_proof', v_core->'protected_closure_proof',
    'processes', v_processes,
    'terminal_proof_sha256', case when v_status = 'completed'
      then v_scope.terminal_proof_sha256 else null end,
    'completed_at', case when v_status = 'completed'
      then v_scope.completed_at else null end,
    'whole_scope_proof', v_whole,
    'whole_scope_proof_sha256',
      v_whole_result->>'whole_scope_proof_sha256'
  );
  if v_live_drift then
    v_result := v_result || jsonb_build_object(
      'code', case when v_terminal_conflict
        then 'FLOW_IDENTITY_SCOPE_TERMINAL_CONFLICT'
        else 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT' end
    );
  elsif v_compensation_required then
    v_result := v_result || jsonb_build_object(
      'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
    );
  end if;
  return v_result;
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_flow_identity_scope_read"("p_scope_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_read"("p_scope_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_read"("p_scope_id" "uuid") TO "authenticated";
