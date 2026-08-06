CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_scope_cancel_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '30s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_cancel_guarded';
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_request_sha256 text;
  v_completed_count integer;
  v_audit_id bigint;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null
    or p_request is null
    or not private.dataset_flow_identity_exact_keys(
      p_request,
      array[
        'schema_version', 'request_id', 'scope_proof_sha256',
        'reason', 'evidence_sha256'
      ]
    )
    or p_request->>'schema_version'
      <> 'dataset-flow-identity-scope-cancel.v1'
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or p_request->>'evidence_sha256' !~ '^[a-f0-9]{64}$'
    or nullif(btrim(p_request->>'reason'), '') is null
    or octet_length(p_request->>'reason') > 512 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_INVALID_REQUEST', 'status', 400,
      'message', 'Cancel request schema mismatch'
    );
  end if;
  if not pg_try_advisory_xact_lock(
    hashtextextended('dataset-flow-identity:' || p_scope_id::text, 0)
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction currently owns this scope'
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
  if v_scope.scope_proof_sha256
      is distinct from p_request->>'scope_proof_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Cancel request does not match the sealed scope'
    );
  end if;
  v_request_sha256 := util.dataset_flow_identity_sha256(p_request);
  if v_scope.status = 'cancelled' then
    return jsonb_build_object(
      'ok', v_scope.cancel_request_sha256 = v_request_sha256,
      'command', v_command,
      'code', case when v_scope.cancel_request_sha256 = v_request_sha256
        then 'FLOW_IDENTITY_SCOPE_CANCELLED'
        else 'FLOW_IDENTITY_CANCEL_REPLAY_MISMATCH' end,
      'status', case when v_scope.cancel_request_sha256 = v_request_sha256
        then 'cancelled' else 'conflict' end,
      'scope_id', p_scope_id, 'replay', true
    );
  end if;
  select count(*) filter (where ledger.status = 'completed')::integer
  into v_completed_count
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = p_scope_id;
  if v_scope.status = 'completed' or v_completed_count > 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PARTIAL_SCOPE_MUST_CONTINUE', 'status', 409,
      'message', 'A scope with any committed primary process cannot cancel',
      'completed_process_count', v_completed_count,
      'automatic_retry', false
    );
  end if;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null,
    jsonb_build_object(
      'record_type', 'scope_cancel',
      'schema_version', p_request->>'schema_version',
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'cancel_request_sha256', v_request_sha256,
      'reason', btrim(p_request->>'reason'),
      'evidence_sha256', p_request->>'evidence_sha256',
      'completed_process_count', 0,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_audit_id;
  update util.dataset_flow_identity_process_ledger
  set active = false
  where scope_id = p_scope_id;
  update util.dataset_flow_identity_scopes
  set status = 'cancelled', cancel_request_sha256 = v_request_sha256,
    last_error = jsonb_build_object(
      'code', 'FLOW_IDENTITY_SCOPE_CANCELLED',
      'reason', btrim(p_request->>'reason'),
      'evidence_sha256', p_request->>'evidence_sha256'
    ), updated_at = clock_timestamp()
  where id = p_scope_id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-cancel-result.v1',
    'scope_id', p_scope_id, 'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'status', 'cancelled', 'completed_process_count', 0,
    'audit_id', v_audit_id::text, 'replay', false
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_CANCEL_LOCK_BUSY', 'status', 409,
    'message', 'Cancel could not acquire its bounded lock'
  );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_scope_cancel_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_scope_cancel_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_scope_cancel_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") TO "api_internal_executor";
