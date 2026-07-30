CREATE OR REPLACE FUNCTION "public"."cmd_dataset_flow_identity_process_rewrite_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '90s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_process_rewrite_guarded';
  v_actor uuid := auth.uid();
  v_request jsonb;
  v_request_sha256 text;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_ledger util.dataset_flow_identity_process_ledger%rowtype;
  v_internal jsonb;
  v_internal_sha256 text;
  v_core_result jsonb;
  v_live jsonb;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
  v_completed_process_count integer;
  v_next_ordinal integer;
  v_primary_complete boolean;
  v_invocation_id uuid;
  v_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_execution_permit jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if p_scope_id is null or v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'scope_proof_sha256', 'ordinal',
      'process_intent_proof_sha256', 'process_request_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-process-rewrite.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or v_request->>'process_intent_proof_sha256' !~ '^[a-f0-9]{64}$'
    or v_request->>'process_request_sha256' !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(v_request->'ordinal') <> 'number'
    or (v_request->>'ordinal')::numeric <= 0
    or (v_request->>'ordinal')::numeric > 2147483647 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 thin process request schema mismatch'
    );
  end if;
  v_request_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    v_request - 'process_request_sha256'
  );
  if v_request_sha256 is distinct from
    v_request->>'process_request_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REQUEST_HASH_MISMATCH',
      'status', 409, 'message', 'Thin process request hash mismatch'
    );
  end if;

  -- Active execution is O(1): the owner fence precedes the scope fence.
  -- The full public target/support activation set is acquired only once by
  -- fresh preflight; repeating it for every ordinal would be O(P^2).
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_SCOPE_BUSY', 'status', 409,
      'message', 'Another process transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null
    or v_scope.scope_proof_sha256
      is distinct from v_request->>'scope_proof_sha256'
    or v_scope.status in ('failed', 'cancelled') then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Scope proof is invalid or scope is not executable'
    );
  end if;
  v_invocation_id := private.dataset_flow_identity_validate_wrapper_permit_v1(
    v_actor, p_scope_id, p_authorization, 'process'
  );
  if v_invocation_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_WRAPPER_PERMIT_REQUIRED', 'status', 409,
      'message', 'A fresh one-wrapper execution permit is required'
    );
  end if;
  select invocation.* into strict v_invocation
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = v_invocation_id
    and invocation.scope_id = v_scope.id
    and invocation.actor_user_id = v_actor
  for update;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_scope.receipt_proof_sha256 then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  select ledger.* into v_ledger
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id
    and ledger.ordinal = (v_request->>'ordinal')::integer
  for update;
  if v_ledger.scope_id is null
    or v_ledger.process_intent_proof_sha256
      is distinct from v_request->>'process_intent_proof_sha256' then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_INTENT_PROOF_MISMATCH', 'status', 409,
      'message', 'Thin request does not bind the receipt process intent'
    );
  end if;
  if v_ledger.status = 'completed' then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_ALREADY_COMPLETED_READ_SCOPE',
      'status', 409,
      'message', 'Completed process proof is available only through scope read'
    );
  end if;
  -- Full-scope reproof is reserved for ambiguous replay/recovery.  A normal
  -- pending ordinal is protected by the active row fences and is revalidated
  -- against its exact process/mapping/support guards inside the private core.
  if v_ledger.status = 'completed' then
    v_live := private.dataset_flow_identity_whole_scope_proof_v2(
      v_actor, v_scope.receipt_id, v_scope.id, true
    );
    if coalesce((v_live->>'ok')::boolean, false) is false then
      perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
        v_invocation_id
      );
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
        'message', 'Whole-scope replay proof drifted'
      );
    end if;
  end if;

  v_internal := jsonb_build_object(
    'schema_version', 'dataset-flow-identity-process-rewrite.v1',
    'request_id', v_request->>'request_id',
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'ordinal', v_ledger.ordinal,
    'process', v_ledger.manifest,
    'rewrites', v_ledger.manifest->'rewrites',
    'collision_ledger', v_ledger.manifest->'collision_ledger',
    'collision_ledger_sha256',
      v_ledger.manifest->>'collision_ledger_sha256',
    'process_request_sha256', ''
  );
  v_internal_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    v_internal - 'process_request_sha256'
  );
  v_internal := jsonb_set(
    v_internal, '{process_request_sha256}', to_jsonb(v_internal_sha256), false
  );
  perform set_config(
    'app.dataset_flow_identity_v2_process_request_sha256',
    v_request_sha256, true
  );
  v_core_result := private.dataset_flow_identity_process_rewrite_core_v1(
    p_scope_id, v_internal
  );
  if coalesce((v_core_result->>'ok')::boolean, false) is false then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return v_core_result;
  end if;
  update util.dataset_flow_identity_process_ledger as ledger
  set wrapper_invocation_id = v_invocation.id,
    permit_generation_before = v_invocation.generation
  where ledger.scope_id = v_scope.id
    and ledger.ordinal = (v_request->>'ordinal')::integer
    and ledger.wrapper_invocation_id is null
    and ledger.permit_generation_before is null
  returning ledger.* into v_ledger;
  if not found then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PROCESS_INVOCATION_BINDING_FAILED';
  end if;
  if v_ledger.status <> 'completed'
    or v_ledger.process_request_sha256 is distinct from v_request_sha256
    or v_ledger.after_payload_sha256
      is distinct from v_ledger.manifest->>'desired_payload_sha256'
    or v_ledger.after_exchange_set_sha256
      is distinct from v_ledger.manifest->>'desired_exchange_set_sha256'
    or v_ledger.audit_id is null or v_ledger.derivative_batch_id is null then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PROCESS_POST_CORE_PROOF_MISMATCH';
  end if;
  select audit.payload into v_audit_payload
  from public.command_audit_log as audit
  where audit.id = v_ledger.audit_id and audit.actor_user_id = v_actor
    and audit.command = v_command and audit.target_table = 'processes'
    and audit.target_id = v_ledger.process_id
    and audit.target_version = v_ledger.process_version;
  if v_audit_payload is null then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PROCESS_V2_AUDIT_MISSING';
  end if;
  v_expected_audit_payload := jsonb_build_object(
      'record_type', 'process_rewrite',
      'schema_version', 'dataset-flow-identity-process-rewrite.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'scope_id', v_scope.id,
      'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'wrapper_invocation_id', v_invocation.id,
      'wrapper_approval_kind', v_invocation.approval_kind,
      'wrapper_approval_identity_sha256',
        v_invocation.approval_identity_sha256,
      'wrapper_admission_request_sha256',
        v_invocation.admission_request_sha256,
      'permit_generation_before', v_invocation.generation,
      'ordinal', v_ledger.ordinal,
      'process_intent_proof_sha256',
        v_ledger.process_intent_proof_sha256,
      'process_request_sha256', v_ledger.process_request_sha256,
      'process_template_sha256', v_ledger.process_template_sha256,
      'rewrite_set_sha256', v_ledger.manifest->>'rewrite_set_sha256',
      'collision_ledger_sha256',
        v_ledger.manifest->>'collision_ledger_sha256',
      'before_payload_sha256', v_ledger.before_payload_sha256,
      'before_exchange_set_sha256',
        v_ledger.manifest->>'before_exchange_set_sha256',
      'desired_payload_sha256',
        v_ledger.manifest->>'desired_payload_sha256',
      'desired_exchange_set_sha256',
        v_ledger.manifest->>'desired_exchange_set_sha256',
      'after_payload_sha256', v_ledger.after_payload_sha256,
      'after_exchange_set_sha256', v_ledger.after_exchange_set_sha256,
      'rewrite_count', v_ledger.rewrite_count,
      'derivative_batch_id', v_ledger.derivative_batch_id,
      'derivative_reason_code', 'FLOW_IDENTITY_SCOPE:'
        || v_scope.id::text || ':' || v_ledger.ordinal::text,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
  if coalesce((v_core_result->>'replay')::boolean, false) is false then
    update public.command_audit_log
    set payload = v_expected_audit_payload
    where id = v_ledger.audit_id and actor_user_id = v_actor
      and command = v_command and target_table = 'processes'
    returning payload into v_audit_payload;
    if v_audit_payload is distinct from v_expected_audit_payload then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PROCESS_V2_AUDIT_PROMOTION_FAILED';
    end if;
  elsif v_audit_payload is distinct from v_expected_audit_payload then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_V2_AUDIT_DRIFT', 'status', 409,
      'message', 'The authoritative v2 process audit drifted'
    );
  end if;
  if coalesce((v_core_result->>'replay')::boolean, false) then
    select
      count(*) filter (where ledger.status = 'completed')::integer,
      min(ledger.ordinal) filter (where ledger.status = 'pending')::integer
    into v_completed_process_count, v_next_ordinal
    from util.dataset_flow_identity_process_ledger as ledger
    where ledger.scope_id = v_scope.id;
  else
    v_completed_process_count := v_ledger.ordinal;
    v_next_ordinal := case when v_ledger.ordinal < v_scope.process_count
      then v_ledger.ordinal + 1 else null end;
  end if;
  v_primary_complete := v_next_ordinal is null
    and v_completed_process_count = v_scope.process_count;
  v_execution_permit :=
    private.dataset_flow_identity_rotate_wrapper_permit_v1(
      v_invocation_id, 'process', false
    );
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-process-rewrite-result.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_scope.receipt_id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'invocation_id', v_invocation.id,
    'permit_generation_before', v_invocation.generation,
    'ordinal', v_ledger.ordinal,
    'process_id', v_ledger.process_id,
    'process_version', v_ledger.process_version,
    'process_request_sha256', v_ledger.process_request_sha256,
    'process_intent_proof_sha256',
      v_ledger.process_intent_proof_sha256,
    'before_payload_sha256', v_ledger.before_payload_sha256,
    'before_exchange_set_sha256',
      v_ledger.manifest->>'before_exchange_set_sha256',
    'desired_payload_sha256',
      v_ledger.manifest->>'desired_payload_sha256',
    'desired_exchange_set_sha256',
      v_ledger.manifest->>'desired_exchange_set_sha256',
    'after_payload_sha256', v_ledger.after_payload_sha256,
    'after_exchange_set_sha256', v_ledger.after_exchange_set_sha256,
    'rewrite_count', v_ledger.rewrite_count,
    'audit_id', v_ledger.audit_id::text,
    'derivative_batch_id', v_ledger.derivative_batch_id,
    'completed_process_count', v_completed_process_count,
    'next_ordinal', v_next_ordinal,
    'primary_complete', v_primary_complete,
    'status', v_ledger.status,
    'replay', coalesce((v_core_result->>'replay')::boolean, false),
    'execution_permit', v_execution_permit
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_PROCESS_LOCK_BUSY', 'status', 409,
    'message', 'Process transaction could not acquire its deterministic fence'
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_flow_identity_process_rewrite_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_flow_identity_process_rewrite_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_flow_identity_process_rewrite_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") TO "authenticated";
