CREATE OR REPLACE FUNCTION "public"."cmd_dataset_flow_identity_scope_lookup"("p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '180s'
    AS $_$
declare
  v_command constant text := 'cmd_dataset_flow_identity_scope_lookup';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_whole_result jsonb;
  v_audit_id bigint;
  v_next_ordinal integer;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if v_request is null or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'receipt_id', 'receipt_proof_sha256',
      'environment', 'project_ref', 'actor', 'target_visibility',
      'user_state_claim', 'operation_id', 'plan_sha256', 'freeze_sha256',
      'policy_approval_text_sha256', 'execution_approval_request_sha256',
      'execution_approval_text_sha256',
      'execution_approval_identity_sha256', 'toolchain_evidence_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-lookup.v1'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'receipt_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or v_request->>'user_state_claim'
      <> 'authenticated_actor_state_100_plus_own_state_0'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}')) is distinct from v_actor_email
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or exists (
      select 1 from unnest(array[
        'receipt_proof_sha256', 'plan_sha256', 'freeze_sha256',
        'policy_approval_text_sha256', 'execution_approval_request_sha256',
        'execution_approval_text_sha256',
        'execution_approval_identity_sha256', 'toolchain_evidence_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_INVALID_REQUEST', 'status', 400,
      'message', 'Scope lookup request schema mismatch'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.actor_user_id = v_actor
    and scope.request_id = (v_request->>'request_id')::uuid
    and scope.receipt_id = (v_request->>'receipt_id')::uuid
    and scope.receipt_proof_sha256 = v_request->>'receipt_proof_sha256'
    and scope.environment = v_request->>'environment'
    and scope.project_ref = v_request->>'project_ref'
    and scope.target_visibility = v_request->>'target_visibility'
    and scope.user_state_claim = v_request->>'user_state_claim'
    and scope.operation_id = v_request->>'operation_id'
    and scope.plan_sha256 = v_request->>'plan_sha256'
    and scope.freeze_sha256 = v_request->>'freeze_sha256'
    and scope.policy_approval_text_sha256 =
      v_request->>'policy_approval_text_sha256'
    and scope.execution_approval_request_sha256 =
      v_request->>'execution_approval_request_sha256'
    and scope.approval_text_sha256 =
      v_request->>'execution_approval_text_sha256'
    and scope.approval_identity_sha256 =
      v_request->>'execution_approval_identity_sha256'
    and scope.toolchain_evidence_sha256 =
      v_request->>'toolchain_evidence_sha256';
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_NOT_FOUND', 'status', 404,
      'message', 'No exactly bound actor-owned Step 3 scope exists'
    );
  end if;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor
    and receipt.receipt_proof_sha256 = v_scope.receipt_proof_sha256;
  if v_receipt.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, false
  );
  if coalesce((v_whole_result->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
      'message', 'Located scope no longer matches live database state'
    );
  end if;
  select audit.id into v_audit_id
  from public.command_audit_log as audit
  where audit.command = 'cmd_dataset_flow_identity_scope_preflight_guarded'
    and audit.actor_user_id = v_actor and audit.target_table is null
    and audit.payload->>'scope_id' = v_scope.id::text;
  if v_audit_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_AUDIT_DRIFT', 'status', 409,
      'message', 'Authoritative scope-seal audit is missing'
    );
  end if;
  select coalesce(min(ledger.ordinal) filter (
    where ledger.status = 'pending'
  ), v_scope.process_count + 1)
  into v_next_ordinal
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-lookup-result.v1',
    'read_only', true,
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_scope.status,
    'process_count', v_scope.process_count,
    'mapping_count', v_scope.mapping_count,
    'support_snapshot_count', v_receipt.support_count,
    'source_universe_count', 305,
    'rewrite_count', v_scope.rewrite_count,
    'next_ordinal', v_next_ordinal,
    'audit_id', v_audit_id::text,
    'whole_scope_proof_sha256',
      v_whole_result->>'whole_scope_proof_sha256',
    'execution_permit', null
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_flow_identity_scope_lookup"("p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_lookup"("p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_lookup"("p_request" "jsonb") TO "authenticated";
