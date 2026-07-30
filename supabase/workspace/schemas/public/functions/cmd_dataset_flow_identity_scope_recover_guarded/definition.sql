CREATE OR REPLACE FUNCTION "public"."cmd_dataset_flow_identity_scope_recover_guarded"("p_scope_id" "uuid", "p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '180s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_recover_guarded';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_wire_request_sha256 text;
  v_approved_at timestamp with time zone;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_existing util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_invocation_id uuid;
  v_whole_result jsonb;
  v_completed_count integer;
  v_next_ordinal integer;
  v_remaining_count integer;
  v_mode text;
  v_token text;
  v_audit_id bigint;
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
      'schema_version', 'request_id', 'approved_at_utc',
      'environment', 'project_ref', 'actor', 'target_visibility',
      'user_state_claim', 'operation_id', 'plan_sha256', 'freeze_sha256',
      'original_execution_approval_identity_sha256', 'scope_proof_sha256',
      'observed_scope_status', 'observed_completed_process_count',
      'observed_next_ordinal', 'observed_whole_scope_proof_sha256',
      'recovery_mode', 'recovery_reason', 'toolchain_evidence_sha256',
      'maximum_wrapper_invocations', 'maximum_process_posts',
      'maximum_finalize_posts', 'maximum_cli_apply_spawns',
      'approval_reusable', 'automatic_retry',
      'recovery_approval_request_sha256',
      'recovery_approval_text_sha256',
      'recovery_approval_identity_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-recovery.v1'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or v_request->>'user_state_claim'
      <> 'authenticated_actor_state_100_plus_own_state_0'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}')) is distinct from v_actor_email
    or v_request->>'observed_scope_status' not in (
      'sealed', 'running', 'primary_complete', 'derivatives_pending'
    )
    or v_request->>'recovery_mode' not in (
      'resume_and_finalize', 'finalize_only'
    )
    or v_request->>'recovery_reason' not in (
      'wrapper_exited_without_permit', 'process_response_ambiguous',
      'process_domain_rejected', 'finalize_response_ambiguous',
      'derivatives_became_ready_after_wrapper_exit'
    )
    or jsonb_typeof(v_request->'observed_completed_process_count') <> 'number'
    or (v_request->>'observed_completed_process_count')::numeric < 0
    or (v_request->>'observed_completed_process_count')::numeric > 2147483647
    or jsonb_typeof(v_request->'observed_next_ordinal') <> 'number'
    or (v_request->>'observed_next_ordinal')::numeric < 1
    or (v_request->>'observed_next_ordinal')::numeric > 2147483647
    or (v_request->>'observed_next_ordinal')::numeric <>
      trunc((v_request->>'observed_next_ordinal')::numeric)
    or jsonb_typeof(v_request->'maximum_wrapper_invocations') <> 'number'
    or (v_request->>'maximum_wrapper_invocations')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_process_posts') <> 'number'
    or (v_request->>'maximum_process_posts')::numeric < 0
    or (v_request->>'maximum_process_posts')::numeric > 2147483647
    or jsonb_typeof(v_request->'maximum_finalize_posts') <> 'number'
    or (v_request->>'maximum_finalize_posts')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_cli_apply_spawns') <> 'number'
    or (v_request->>'maximum_cli_apply_spawns')::numeric <> 1
    or v_request->'approval_reusable' is distinct from 'false'::jsonb
    or v_request->'automatic_retry' is distinct from 'false'::jsonb
    or exists (
      select 1 from unnest(array[
        'plan_sha256', 'freeze_sha256',
        'original_execution_approval_identity_sha256', 'scope_proof_sha256',
        'observed_whole_scope_proof_sha256', 'toolchain_evidence_sha256',
        'recovery_approval_request_sha256',
        'recovery_approval_text_sha256',
        'recovery_approval_identity_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_INVALID_REQUEST', 'status', 400,
      'message', 'Recovery approval request schema mismatch'
    );
  end if;
  begin
    v_approved_at := (v_request->>'approved_at_utc')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_INVALID_APPROVAL_TIME', 'status', 400,
      'message', 'Recovery approval timestamp is invalid'
    );
  end;
  v_wire_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);
  if cardinality(array[
      v_request->>'recovery_approval_request_sha256',
      v_request->>'recovery_approval_text_sha256',
      v_request->>'recovery_approval_identity_sha256'
    ]) <> (
      select count(distinct value)
      from unnest(array[
        v_request->>'recovery_approval_request_sha256',
        v_request->>'recovery_approval_text_sha256',
        v_request->>'recovery_approval_identity_sha256'
      ]) as approval(value)
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_APPROVAL_HASH_MISMATCH', 'status', 409,
      'message', 'Recovery approval artifact hashes must be distinct'
    );
  end if;

  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_SCOPE_BUSY', 'status', 409,
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
  if v_scope.status in ('completed', 'failed', 'cancelled')
    or v_scope.scope_proof_sha256 is distinct from
      v_request->>'scope_proof_sha256'
    or v_scope.environment is distinct from v_request->>'environment'
    or v_scope.project_ref is distinct from v_request->>'project_ref'
    or v_scope.target_visibility is distinct from
      v_request->>'target_visibility'
    or v_scope.user_state_claim is distinct from
      v_request->>'user_state_claim'
    or v_scope.operation_id is distinct from v_request->>'operation_id'
    or v_scope.plan_sha256 is distinct from v_request->>'plan_sha256'
    or v_scope.freeze_sha256 is distinct from v_request->>'freeze_sha256'
    or v_scope.approval_identity_sha256 is distinct from
      v_request->>'original_execution_approval_identity_sha256'
    or v_approved_at < v_scope.sealed_at
    or exists (
      select 1 from unnest(array[
        v_request->>'recovery_approval_request_sha256',
        v_request->>'recovery_approval_text_sha256',
        v_request->>'recovery_approval_identity_sha256'
      ]) as recovery(value)
      where recovery.value = any(array[
        v_scope.policy_approval_text_sha256,
        v_scope.execution_approval_request_sha256,
        v_scope.approval_text_sha256,
        v_scope.approval_identity_sha256
      ])
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_SCOPE_BINDING_MISMATCH', 'status', 409,
      'message', 'Recovery approval does not bind this active scope'
    );
  end if;

  select invocation.* into v_existing
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.actor_user_id = v_actor
    and array[
      v_request->>'recovery_approval_request_sha256',
      v_request->>'recovery_approval_text_sha256',
      v_request->>'recovery_approval_identity_sha256'
    ] && array[
      invocation.approval_request_sha256,
      invocation.approval_text_sha256,
      invocation.approval_identity_sha256
    ]
  order by invocation.admitted_at, invocation.id
  limit 1;
  if v_existing.id is not null then
    if v_existing.scope_id is distinct from v_scope.id
      or v_existing.approval_kind <> 'recovery'
      or v_existing.approval_request_sha256 is distinct from
        v_request->>'recovery_approval_request_sha256'
      or v_existing.approval_text_sha256 is distinct from
        v_request->>'recovery_approval_text_sha256'
      or v_existing.approval_identity_sha256 is distinct from
        v_request->>'recovery_approval_identity_sha256'
      or v_existing.admission_request_sha256 is distinct from
        v_wire_request_sha256
      or v_existing.baseline_whole_scope_proof_sha256 is distinct from
        v_request->>'observed_whole_scope_proof_sha256' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_RECOVERY_APPROVAL_REUSE_MISMATCH',
        'status', 409,
        'message', 'Recovery approval was already consumed differently'
      );
    end if;
    select audit.id, audit.payload into v_audit_id, v_audit_payload
    from public.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
      and audit.payload->>'recovery_approval_identity_sha256'
        = v_existing.approval_identity_sha256;
    if v_audit_id is null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_RECOVERY_REPLAY_AUDIT_MISSING';
    end if;
    v_expected_audit_payload := jsonb_build_object(
      'record_type', 'scope_recovery_admission',
      'schema_version', 'dataset-flow-identity-scope-recovery.v1',
      'scope_id', v_scope.id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'freeze_sha256', v_scope.freeze_sha256,
      'original_execution_approval_identity_sha256',
        v_scope.approval_identity_sha256,
      'observed_whole_scope_proof_sha256',
        v_request->>'observed_whole_scope_proof_sha256',
      'observed_scope_status', v_request->>'observed_scope_status',
      'observed_completed_process_count',
        (v_request->>'observed_completed_process_count')::integer,
      'observed_next_ordinal',
        (v_request->>'observed_next_ordinal')::integer,
      'recovery_mode', v_request->>'recovery_mode',
      'recovery_reason', v_request->>'recovery_reason',
      'toolchain_evidence_sha256', v_request->>'toolchain_evidence_sha256',
      'recovery_approval_request_sha256',
        v_request->>'recovery_approval_request_sha256',
      'recovery_approval_text_sha256',
        v_request->>'recovery_approval_text_sha256',
      'recovery_approval_identity_sha256',
        v_request->>'recovery_approval_identity_sha256',
      'recovery_wire_request_sha256', v_wire_request_sha256,
      'invocation_id', v_existing.id,
      'maximum_wrapper_invocations', 1,
      'maximum_process_posts',
        (v_request->>'maximum_process_posts')::integer,
      'maximum_finalize_posts', 1,
      'maximum_cli_apply_spawns', 1,
      'approval_reusable', false,
      'automatic_retry', false,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
    if v_audit_payload is distinct from v_expected_audit_payload then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_RECOVERY_AUDIT_DRIFT', 'status', 409,
        'message', 'Authoritative recovery admission audit drifted'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', 'dataset-flow-identity-scope-recovery-result.v1',
      'scope_id', v_scope.id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', v_audit_payload->>'observed_scope_status',
      'completed_process_count',
        (v_audit_payload->>'observed_completed_process_count')::integer,
      'next_ordinal', (v_audit_payload->>'observed_next_ordinal')::integer,
      'whole_scope_proof_sha256',
        v_existing.baseline_whole_scope_proof_sha256,
      'recovery_wire_request_sha256',
        v_existing.admission_request_sha256,
      'recovery_approval_identity_sha256',
        v_existing.approval_identity_sha256,
      'invocation_id', v_existing.id,
      'audit_id', case when v_audit_id is null then null else v_audit_id::text end,
      'replay', true,
      'execution_permit', null
    );
  end if;

  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing'
    );
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, true
  );
  if coalesce((v_whole_result->>'ok')::boolean, false) is false
    or v_whole_result->>'whole_scope_proof_sha256' is distinct from
      v_request->>'observed_whole_scope_proof_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_LIVE_PROOF_MISMATCH', 'status', 409,
      'message', 'Current whole-scope proof does not match the approval'
    );
  end if;
  select count(*) filter (where ledger.status = 'completed')::integer,
    coalesce(
      min(ledger.ordinal) filter (where ledger.status = 'pending')::integer,
      v_scope.process_count + 1
    )
  into v_completed_count, v_next_ordinal
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  v_remaining_count := v_scope.process_count - v_completed_count;
  v_mode := case when v_remaining_count = 0
    then 'finalize_only' else 'resume_and_finalize' end;
  if v_scope.status is distinct from v_request->>'observed_scope_status'
    or v_completed_count is distinct from
      (v_request->>'observed_completed_process_count')::integer
    or (v_request->>'observed_next_ordinal')::integer <> v_next_ordinal
    or v_mode is distinct from v_request->>'recovery_mode'
    or v_remaining_count is distinct from
      (v_request->>'maximum_process_posts')::integer then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_PROGRESS_MISMATCH', 'status', 409,
      'message', 'Recovery approval does not match current durable progress'
    );
  end if;

  update util.dataset_flow_identity_wrapper_invocations
  set status = 'superseded', generation = generation + 1,
    token_sha256 = private.dataset_flow_identity_permit_token_sha256_v1(
      pg_catalog.encode(extensions.gen_random_bytes(32), 'hex')
    ),
    updated_at = clock_timestamp(), closed_at = clock_timestamp()
  where scope_id = v_scope.id and status = 'active';
  v_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
  insert into util.dataset_flow_identity_wrapper_invocations (
    scope_id, actor_user_id, approval_kind,
    approval_request_sha256, approval_text_sha256,
    approval_identity_sha256, admission_request_sha256,
    baseline_whole_scope_proof_sha256,
    token_sha256, maximum_process_posts, maximum_finalize_posts
  ) values (
    v_scope.id, v_actor, 'recovery',
    v_request->>'recovery_approval_request_sha256',
    v_request->>'recovery_approval_text_sha256',
    v_request->>'recovery_approval_identity_sha256',
    v_wire_request_sha256,
    v_request->>'observed_whole_scope_proof_sha256',
    private.dataset_flow_identity_permit_token_sha256_v1(v_token),
    v_remaining_count, 1
  ) returning id into v_invocation_id;
  v_audit_payload := jsonb_build_object(
    'record_type', 'scope_recovery_admission',
    'schema_version', 'dataset-flow-identity-scope-recovery.v1',
    'scope_id', v_scope.id,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'freeze_sha256', v_scope.freeze_sha256,
    'original_execution_approval_identity_sha256',
      v_scope.approval_identity_sha256,
    'observed_whole_scope_proof_sha256',
      v_request->>'observed_whole_scope_proof_sha256',
    'observed_scope_status', v_scope.status,
    'observed_completed_process_count', v_completed_count,
    'observed_next_ordinal', v_next_ordinal,
    'recovery_mode', v_mode,
    'recovery_reason', v_request->>'recovery_reason',
    'toolchain_evidence_sha256', v_request->>'toolchain_evidence_sha256',
    'recovery_approval_request_sha256',
      v_request->>'recovery_approval_request_sha256',
    'recovery_approval_text_sha256',
      v_request->>'recovery_approval_text_sha256',
    'recovery_approval_identity_sha256',
      v_request->>'recovery_approval_identity_sha256',
    'recovery_wire_request_sha256', v_wire_request_sha256,
    'invocation_id', v_invocation_id,
    'maximum_wrapper_invocations', 1,
    'maximum_process_posts', v_remaining_count,
    'maximum_finalize_posts', 1,
    'maximum_cli_apply_spawns', 1,
    'approval_reusable', false,
    'automatic_retry', false,
    'hash_algorithm', 'restricted-safe-json-v2-sha256'
  );
  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null, v_audit_payload
  ) returning id into v_audit_id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-recovery-result.v1',
    'scope_id', v_scope.id,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_scope.status,
    'completed_process_count', v_completed_count,
    'next_ordinal', v_next_ordinal,
    'whole_scope_proof_sha256',
      v_request->>'observed_whole_scope_proof_sha256',
    'recovery_wire_request_sha256', v_wire_request_sha256,
    'recovery_approval_identity_sha256',
      v_request->>'recovery_approval_identity_sha256',
    'invocation_id', v_invocation_id,
    'audit_id', v_audit_id::text,
    'replay', false,
    'execution_permit', jsonb_build_object(
      'schema_version', 'dataset-flow-identity-execution-permit.v1',
      'invocation_id', v_invocation_id,
      'generation', 0,
      'token', v_token
    )
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_RECOVERY_LOCK_BUSY', 'status', 409,
    'message', 'Recovery could not acquire its deterministic fence'
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_flow_identity_scope_recover_guarded"("p_scope_id" "uuid", "p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_recover_guarded"("p_scope_id" "uuid", "p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_flow_identity_scope_recover_guarded"("p_scope_id" "uuid", "p_request" "jsonb") TO "authenticated";
