CREATE OR REPLACE FUNCTION "api"."cmd_dataset_flow_identity_scope_finalize_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '180s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_finalize_guarded';
  v_actor uuid := auth.uid();
  v_request jsonb;
  v_request_sha256 text;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_whole_result jsonb;
  v_whole jsonb;
  v_internal jsonb;
  v_core jsonb;
  v_derivative_targets jsonb;
  v_derivative_target_set_sha256 text;
  v_terminal_proof_sha256 text;
  v_final_audit_id bigint;
  v_final_audit_payload jsonb;
  v_expected_final_audit_payload jsonb;
  v_live_drift boolean;
  v_live_guard_current boolean;
  v_was_completed boolean;
  v_result jsonb;
  v_invocation_id uuid;
  v_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_execution_permit jsonb;
  v_permit_consumed boolean := false;
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
      'schema_version', 'request_id', 'scope_proof_sha256', 'expected'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-finalize.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'expected', array[
        'process_count', 'rewrite_count', 'completed_process_count'
      ]
    )
    or exists (
      select 1 from unnest(array[
        'process_count', 'rewrite_count', 'completed_process_count'
      ]) as field(name)
      where jsonb_typeof(v_request->'expected'->field.name) <> 'number'
        or (v_request->'expected'->>field.name)::numeric <= 0
        or (v_request->'expected'->>field.name)::numeric > 2147483647
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 thin finalize request schema mismatch'
    );
  end if;
  v_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null
    or v_scope.scope_proof_sha256
      is distinct from v_request->>'scope_proof_sha256'
    or v_scope.status in ('failed', 'cancelled')
    or (v_request #>> '{expected,process_count}')::integer
      <> v_scope.process_count
    or (v_request #>> '{expected,rewrite_count}')::integer
      <> v_scope.rewrite_count
    or (v_request #>> '{expected,completed_process_count}')::integer
      <> v_scope.process_count then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Finalize request does not bind the actor scope and counts'
    );
  end if;
  v_invocation_id := private.dataset_flow_identity_validate_wrapper_permit_v1(
    v_actor, p_scope_id, p_authorization, 'finalize'
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
  v_was_completed := v_scope.status = 'completed';
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
      'code', 'FLOW_IDENTITY_FINALIZE_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, true
  );
  v_whole := v_whole_result->'whole_scope_proof';
  v_live_drift := coalesce((v_whole_result->>'ok')::boolean, false) is false;
  v_live_guard_current := coalesce(
      (v_whole->>'audit_current')::boolean, false
    ) and coalesce((v_whole->>'source_guards_current')::boolean, false
    ) and coalesce((v_whole->>'support_guards_current')::boolean, false)
    and coalesce((v_whole->>'target_guards_current')::boolean, false)
    and coalesce((v_whole->>'protected_closure_current')::boolean, false)
    and coalesce((v_whole->>'occurrence_closure_current')::boolean, false);
  select coalesce(jsonb_agg(jsonb_build_object(
    'ordinal', ledger.ordinal,
    'id', ledger.process_id,
    'version', ledger.process_version,
    'desired_json_ordered_sha256',
      ledger.manifest->>'desired_payload_sha256',
    'baseline_snapshot_sha256',
      ledger.manifest->>'derivative_baseline_snapshot_sha256'
  ) order by ledger.ordinal), '[]'::jsonb)
  into v_derivative_targets
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  v_derivative_target_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_derivative_targets);
  if not v_live_drift then
    v_internal := jsonb_build_object(
      'schema_version', 'dataset-flow-identity-scope-finalize.v1',
      'request_id', v_request->>'request_id',
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'expected', jsonb_build_object(
        'process_count', v_scope.process_count,
        'rewrite_count', v_scope.rewrite_count,
        'completed_process_count', v_scope.process_count,
        'primary_closure_sha256', v_whole->>'primary_closure_sha256',
        'protected_closure_sha256', v_scope.protected_closure_sha256,
        'derivative_target_set_sha256', v_derivative_target_set_sha256
      )
    );
    perform set_config(
      'app.dataset_flow_identity_v2_finalize_request_sha256',
      v_request_sha256, true
    );
    v_core := private.dataset_flow_identity_scope_finalize_core_v1(
      p_scope_id, v_internal
    );
    if v_core->>'status' not in ('derivatives_pending', 'completed') then
      v_core := jsonb_build_object(
        'ok', false,
        'status', 'failed',
        'code', 'FLOW_IDENTITY_FINALIZE_FAILED',
        'compensation_required', false,
        'compensation_targets', '[]'::jsonb
      );
    end if;
    select scope.* into v_scope
    from util.dataset_flow_identity_scopes as scope
    where scope.id = p_scope_id and scope.actor_user_id = v_actor
    for update;
    -- A fresh completion is promoted using the already current pre-core proof.
    -- Re-entering whole-scope proof before the v2 terminal audit exists would
    -- deterministically mark audit_current false.  Pending and replay paths do
    -- not have that transitional state and are re-read immediately.
    if v_core->>'status' <> 'completed' or v_was_completed then
      v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
        v_actor, v_receipt.id, v_scope.id, true
      );
      v_whole := v_whole_result->'whole_scope_proof';
      v_live_drift :=
        coalesce((v_whole_result->>'ok')::boolean, false) is false;
      v_live_guard_current := coalesce(
          (v_whole->>'audit_current')::boolean, false
        ) and coalesce((v_whole->>'source_guards_current')::boolean, false
        ) and coalesce((v_whole->>'support_guards_current')::boolean, false)
        and coalesce((v_whole->>'target_guards_current')::boolean, false)
        and coalesce((v_whole->>'protected_closure_current')::boolean, false)
        and coalesce((v_whole->>'occurrence_closure_current')::boolean, false);
    end if;
  end if;
  -- Completed replay is dynamic, not a trust in the stored scope status.  A
  -- stale/failed derivative downgrades to pending/compensation and must never
  -- rewrite the terminal proof or terminal audit.
  if not v_live_drift and v_core->>'status' = 'completed'
    and (
      coalesce((v_whole->>'derivatives_current')::boolean, false) is false
      or coalesce((v_whole->>'causal_terminal_proof')::boolean, false) is false
    ) then
    v_core := jsonb_build_object(
      'ok', coalesce((v_whole_result #>>
        '{derivative_set_proof,failed_count}')::integer, 0) = 0,
      'status', 'derivatives_pending',
      'code', case when coalesce((v_whole_result #>>
          '{derivative_set_proof,failed_count}')::integer, 0) > 0
        then 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
        else 'FLOW_IDENTITY_DERIVATIVES_PENDING' end,
      'compensation_required', coalesce((v_whole_result #>>
        '{derivative_set_proof,failed_count}')::integer, 0) > 0,
      'compensation_targets', coalesce(
        v_whole_result #> '{derivative_set_proof,compensation_targets}',
        '[]'::jsonb
      )
    );
  end if;
  if not v_live_drift and v_core->>'status' = 'completed'
      and not v_was_completed then
    v_terminal_proof_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
        'schema_version', 'dataset-flow-identity-terminal-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'plan_sha256', v_scope.plan_sha256,
        'final_request_sha256', v_request_sha256,
        'wrapper_invocation_id', v_invocation.id,
        'wrapper_approval_kind', v_invocation.approval_kind,
        'wrapper_approval_identity_sha256',
          v_invocation.approval_identity_sha256,
        'wrapper_admission_request_sha256',
          v_invocation.admission_request_sha256,
        'permit_generation_before', v_invocation.generation,
        'whole_scope_proof_sha256',
          v_whole_result->>'whole_scope_proof_sha256',
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'derivative_target_set_sha256', v_derivative_target_set_sha256,
        'derivative_proof_set_sha256',
          v_whole->>'derivative_proof_set_sha256'
      ));
    update util.dataset_flow_identity_scopes
    set terminal_proof_sha256 = v_terminal_proof_sha256,
      final_wrapper_invocation_id = v_invocation.id,
      final_permit_generation_before = v_invocation.generation,
      updated_at = clock_timestamp()
    where id = v_scope.id;
    v_scope.terminal_proof_sha256 := v_terminal_proof_sha256;
    select audit.id, audit.payload
    into v_final_audit_id, v_final_audit_payload
    from private.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
    order by audit.id desc limit 1;
    if v_final_audit_id is null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_FINALIZE_V2_AUDIT_MISSING';
    end if;
    v_expected_final_audit_payload := jsonb_build_object(
        'record_type', 'scope_terminal',
        'schema_version', 'dataset-flow-identity-scope-finalize.v2',
        'proof_domain', 'dataset-flow-identity-db-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'operation_id', v_scope.operation_id,
        'plan_sha256', v_scope.plan_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'final_request_sha256', v_request_sha256,
        'wrapper_invocation_id', v_invocation.id,
        'wrapper_approval_kind', v_invocation.approval_kind,
        'wrapper_approval_identity_sha256',
          v_invocation.approval_identity_sha256,
        'wrapper_admission_request_sha256',
          v_invocation.admission_request_sha256,
        'permit_generation_before', v_invocation.generation,
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'primary_closure_sha256', v_whole->>'primary_closure_sha256',
        'derivative_target_set_sha256', v_derivative_target_set_sha256,
        'derivative_proof_set_sha256',
          v_whole->>'derivative_proof_set_sha256',
        'whole_scope_proof_sha256',
          v_whole_result->>'whole_scope_proof_sha256',
        'terminal_proof_sha256', v_terminal_proof_sha256,
        'process_count', v_scope.process_count,
        'rewrite_count', v_scope.rewrite_count,
        'hash_algorithm', 'restricted-safe-json-v2-sha256'
      );
    update private.command_audit_log
    set payload = v_expected_final_audit_payload
    where id = v_final_audit_id and actor_user_id = v_actor
      and command = v_command and target_table is null
    returning payload into v_final_audit_payload;
    if v_final_audit_payload
        is distinct from v_expected_final_audit_payload then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_FINALIZE_V2_AUDIT_PROMOTION_FAILED';
    end if;
    -- Terminal consumption is part of the same transaction and precedes the
    -- durable post-promotion proof.  The proof therefore requires the exact
    -- N -> N+1 terminal rotation and one successful finalize post.
    v_execution_permit :=
      private.dataset_flow_identity_rotate_wrapper_permit_v1(
        v_invocation_id, 'finalize', true
      );
    v_permit_consumed := true;
    v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
      v_actor, v_receipt.id, v_scope.id, true
    );
    v_whole := v_whole_result->'whole_scope_proof';
    v_live_drift :=
      coalesce((v_whole_result->>'ok')::boolean, false) is false;
    v_live_guard_current := coalesce(
        (v_whole->>'audit_current')::boolean, false
      ) and coalesce((v_whole->>'source_guards_current')::boolean, false
      ) and coalesce((v_whole->>'support_guards_current')::boolean, false)
      and coalesce((v_whole->>'target_guards_current')::boolean, false)
      and coalesce((v_whole->>'protected_closure_current')::boolean, false)
      and coalesce((v_whole->>'occurrence_closure_current')::boolean, false);
    if v_live_drift then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_FINALIZE_POST_PROMOTION_DRIFT';
    end if;
  else
    select audit.id into v_final_audit_id
    from private.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
    order by audit.id desc limit 1;
  end if;
  v_result := jsonb_build_object(
    'ok', not v_live_drift
      and coalesce(v_core->>'status', '') <> 'failed'
      and coalesce((v_core->>'compensation_required')::boolean, false) is false,
    'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-finalize-result.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'invocation_id', v_invocation.id,
    'permit_generation_before', v_invocation.generation,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', case when v_live_drift then 'live_drift'
      else v_core->>'status' end,
    'process_count', v_scope.process_count,
    'completed_process_count',
      (v_request #>> '{expected,completed_process_count}')::integer,
    'rewrite_count', v_scope.rewrite_count,
    'primary_closure_sha256', v_whole->>'primary_closure_sha256',
    'protected_closure_sha256', v_receipt.protected_closure_sha256,
    'derivative_target_set_sha256', v_derivative_target_set_sha256,
    'derivative_proof_set_sha256',
      v_whole->>'derivative_proof_set_sha256',
    'primary_current', (v_whole->>'primary_current')::boolean,
    'live_guard_current', v_live_guard_current,
    'derivatives_current', case when v_live_drift then false
      else (v_whole->>'derivatives_current')::boolean end,
    'terminal_proof_sha256', case
      when not v_live_drift and v_core->>'status' = 'completed'
      then v_scope.terminal_proof_sha256 else null end,
    'whole_scope_proof', v_whole,
    'whole_scope_proof_sha256',
      v_whole_result->>'whole_scope_proof_sha256',
    'audit_id', case when v_final_audit_id is null
      then null else v_final_audit_id::text end,
    'replay', v_was_completed
  );
  if coalesce((v_result->>'ok')::boolean, false) then
    if not v_permit_consumed then
      v_execution_permit :=
        private.dataset_flow_identity_rotate_wrapper_permit_v1(
          v_invocation_id, 'finalize', false
        );
    end if;
    v_result := v_result || jsonb_build_object(
      'execution_permit', v_execution_permit
    );
  else
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
  end if;
  if v_live_drift then
    return v_result || jsonb_build_object(
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT',
      'compensation_required', false,
      'automatic_retry', false,
      'compensation_targets', '[]'::jsonb
    );
  elsif v_core->>'status' = 'derivatives_pending' then
    return v_result || jsonb_build_object(
      'code', v_core->>'code',
      'compensation_required',
        coalesce((v_core->>'compensation_required')::boolean, false),
      'automatic_retry', false,
      'compensation_targets',
        coalesce(v_core->'compensation_targets', '[]'::jsonb)
    );
  elsif v_core->>'status' = 'failed' then
    return v_result || jsonb_build_object(
      'code', 'FLOW_IDENTITY_FINALIZE_FAILED',
      'compensation_required', false,
      'automatic_retry', false,
      'compensation_targets', '[]'::jsonb
    );
  end if;
  return v_result;
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_FINALIZE_LOCK_BUSY', 'status', 409,
    'message', 'Finalization could not acquire its deterministic fence'
  );
end;
$_$;

ALTER FUNCTION "api"."cmd_dataset_flow_identity_scope_finalize_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_flow_identity_scope_finalize_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_flow_identity_scope_finalize_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_flow_identity_scope_finalize_guarded"("p_scope_id" "uuid", "p_request" "jsonb", "p_authorization" "jsonb") TO "authenticated";
