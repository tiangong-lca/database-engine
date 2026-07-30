CREATE OR REPLACE FUNCTION "public"."cmd_dataset_flow_identity_scope_cancel_guarded"("p_scope_id" "uuid", "p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '30s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_cancel_guarded';
  v_actor uuid := auth.uid();
  v_request jsonb;
  v_request_sha256 text;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_ledger_count integer;
  v_completed_count integer;
  v_nonpending_count integer;
  v_inactive_count integer;
  v_primary_proof_count integer;
  v_process_audit_count integer;
  v_permit_count integer;
  v_audit_id bigint;
  v_audit_count integer;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
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
      'schema_version', 'request_id', 'receipt_id', 'receipt_proof_sha256',
      'operation_id', 'plan_sha256', 'scope_proof_sha256', 'reason',
      'evidence_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-cancel.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'receipt_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or nullif(btrim(v_request->>'reason'), '') is null
    or octet_length(v_request->>'reason') > 512
    or exists (
      select 1 from unnest(array[
        'receipt_proof_sha256', 'plan_sha256', 'scope_proof_sha256',
        'evidence_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 cancel request schema mismatch'
    );
  end if;
  v_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);

  -- Match the v2 execution lock order.  The owner fence excludes direct
  -- owner-row writes while the scope lock excludes a concurrent ordinal.
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
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
    or v_receipt.id::text is distinct from v_request->>'receipt_id'
    or v_receipt.receipt_proof_sha256
      is distinct from v_request->>'receipt_proof_sha256'
    or v_scope.receipt_proof_sha256
      is distinct from v_request->>'receipt_proof_sha256'
    or v_scope.operation_id is distinct from v_request->>'operation_id'
    or v_scope.plan_sha256 is distinct from v_request->>'plan_sha256'
    or v_scope.scope_proof_sha256
      is distinct from v_request->>'scope_proof_sha256'
    or v_scope.target_visibility <> 'owner_draft' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Cancel request does not bind the actor scope and receipt'
    );
  end if;

  v_expected_audit_payload := jsonb_build_object(
    'record_type', 'scope_cancel',
    'schema_version', 'dataset-flow-identity-scope-cancel.v2',
    'proof_domain', 'dataset-flow-identity-db-proof.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'cancel_request_sha256', v_request_sha256,
    'reason', btrim(v_request->>'reason'),
    'evidence_sha256', v_request->>'evidence_sha256',
    'completed_process_count', 0,
    'process_count', v_scope.process_count,
    'rewrite_count', v_scope.rewrite_count,
    'hash_algorithm', 'restricted-safe-json-v2-sha256'
  );
  if v_scope.status = 'cancelled' then
    if v_scope.cancel_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CANCEL_REPLAY_MISMATCH', 'status', 409,
        'message', 'Cancelled scope is bound to a different request'
      );
    end if;
    select count(*)::integer,
      jsonb_agg(audit.payload order by audit.id)->0,
      min(audit.id)
    into v_audit_count, v_audit_payload, v_audit_id
    from public.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
      and audit.payload->>'cancel_request_sha256' = v_request_sha256;
    if v_audit_count <> 1
      or v_audit_payload is distinct from v_expected_audit_payload then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CANCEL_AUDIT_DRIFT', 'status', 409,
        'message', 'The authoritative v2 cancel audit is missing or drifted'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', 'dataset-flow-identity-scope-cancel-result.v2',
      'scope_id', v_scope.id, 'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'operation_id', v_scope.operation_id, 'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'cancel_request_sha256', v_request_sha256,
      'status', 'cancelled', 'completed_process_count', 0,
      'audit_id', v_audit_id::text, 'replay', true
    );
  end if;
  if v_scope.status <> 'sealed' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_REQUIRES_ZERO_WRITE_SEAL', 'status', 409,
      'message', 'Only an untouched sealed scope can be cancelled',
      'scope_status', v_scope.status, 'automatic_retry', false
    );
  end if;

  select count(*)::integer,
    count(*) filter (where ledger.status = 'completed')::integer,
    count(*) filter (where ledger.status <> 'pending')::integer,
    count(*) filter (where not ledger.active)::integer,
    count(*) filter (
      where ledger.audit_id is not null
        or ledger.after_payload_sha256 is not null
        or ledger.after_exchange_set_sha256 is not null
        or ledger.derivative_batch_id is not null
        or ledger.process_request_sha256 is not null
        or ledger.completed_at is not null
    )::integer
  into v_ledger_count, v_completed_count, v_nonpending_count,
    v_inactive_count, v_primary_proof_count
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  select count(*)::integer into v_process_audit_count
  from public.command_audit_log as audit
  where audit.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
    and audit.actor_user_id = v_actor
    and audit.target_table = 'processes'
    and audit.payload->>'scope_id' = v_scope.id::text;
  select count(*)::integer into v_permit_count
  from util.dataset_flow_identity_mutation_permits as permit
  where permit.scope_id = v_scope.id;
  if v_ledger_count <> v_scope.process_count
    or v_completed_count <> 0 or v_nonpending_count <> 0
    or v_inactive_count <> 0 or v_primary_proof_count <> 0
    or v_process_audit_count <> 0 or v_permit_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_PRIMARY_WRITE_PROOF_EXISTS', 'status', 409,
      'message', 'Scope ledger or audit proves a primary write attempt',
      'completed_process_count', v_completed_count,
      'automatic_retry', false
    );
  end if;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null, v_expected_audit_payload
  ) returning id into v_audit_id;
  update util.dataset_flow_identity_process_ledger
  set active = false where scope_id = v_scope.id;
  update util.dataset_flow_identity_scopes
  set status = 'cancelled', cancel_request_sha256 = v_request_sha256,
    last_error = jsonb_build_object(
      'code', 'FLOW_IDENTITY_SCOPE_CANCELLED',
      'schema_version', 'dataset-flow-identity-scope-cancel.v2',
      'cancel_request_sha256', v_request_sha256,
      'reason', btrim(v_request->>'reason'),
      'evidence_sha256', v_request->>'evidence_sha256'
    ), updated_at = clock_timestamp()
  where id = v_scope.id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-cancel-result.v2',
    'scope_id', v_scope.id, 'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'operation_id', v_scope.operation_id, 'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'cancel_request_sha256', v_request_sha256,
    'status', 'cancelled', 'completed_process_count', 0,
    'audit_id', v_audit_id::text, 'replay', false
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_CANCEL_LOCK_BUSY', 'status', 409,
    'message', 'Cancellation could not acquire its deterministic fence'
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_flow_identity_scope_cancel_guarded"("p_scope_id" "uuid", "p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_cancel_guarded"("p_scope_id" "uuid", "p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_cancel_guarded"("p_scope_id" "uuid", "p_request" "jsonb") TO "authenticated";
