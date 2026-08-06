CREATE OR REPLACE FUNCTION "api"."cmd_dataset_flow_identity_scope_preflight_guarded"("p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '180s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_preflight_guarded';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_request_sha256 text;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_internal jsonb;
  v_core_result jsonb;
  v_live jsonb;
  v_mappings jsonb;
  v_supports jsonb;
  v_processes jsonb;
  v_scope_proof_sha256 text;
  v_audit_id bigint;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
  v_invocation_id uuid;
  v_permit_token text;
  v_execution_permit jsonb := null;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'receipt_id', 'receipt_proof_sha256',
      'environment', 'project_ref', 'actor', 'target_visibility',
      'user_state_claim',
      'operation_id', 'plan_sha256', 'freeze_sha256',
      'policy_approval_text_sha256', 'execution_approval_request_sha256',
      'execution_approval_text_sha256',
      'execution_approval_identity_sha256', 'toolchain_evidence_sha256',
      'maximum_wrapper_invocations', 'maximum_process_posts',
      'maximum_finalize_posts', 'maximum_cli_apply_spawns',
      'approval_reusable', 'automatic_retry'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-preflight.v2'
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
    or lower(btrim(v_request #>> '{actor,email}'))
      is distinct from v_actor_email
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or jsonb_typeof(v_request->'maximum_wrapper_invocations') <> 'number'
    or (v_request->>'maximum_wrapper_invocations')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_process_posts') <> 'number'
    or (v_request->>'maximum_process_posts')::numeric <= 0
    or (v_request->>'maximum_process_posts')::numeric > 2147483647
    or jsonb_typeof(v_request->'maximum_finalize_posts') <> 'number'
    or (v_request->>'maximum_finalize_posts')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_cli_apply_spawns') <> 'number'
    or (v_request->>'maximum_cli_apply_spawns')::numeric <> 1
    or v_request->'approval_reusable' is distinct from 'false'::jsonb
    or v_request->'automatic_retry' is distinct from 'false'::jsonb
    or exists (
      select 1 from unnest(array[
        'receipt_proof_sha256', 'plan_sha256', 'freeze_sha256',
        'policy_approval_text_sha256', 'execution_approval_request_sha256',
        'execution_approval_text_sha256',
        'execution_approval_identity_sha256', 'toolchain_evidence_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    )
    or (
      select count(distinct value)
      from unnest(array[
        v_request->>'policy_approval_text_sha256',
        v_request->>'execution_approval_request_sha256',
        v_request->>'execution_approval_text_sha256',
        v_request->>'execution_approval_identity_sha256'
      ]) as approval(value)
    ) <> 4 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 preflight request schema mismatch'
    );
  end if;
  v_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);

  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = (v_request->>'receipt_id')::uuid
    and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_request->>'receipt_proof_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_RECEIPT_INVALID', 'status', 409,
      'message', 'Capture receipt is missing, foreign, or forged'
    );
  end if;
  if v_receipt.environment is distinct from v_request->>'environment'
    or v_receipt.project_ref is distinct from v_request->>'project_ref'
    or v_receipt.target_visibility
      is distinct from v_request->>'target_visibility'
    or v_receipt.operation_id is distinct from v_request->>'operation_id'
    or v_receipt.policy_approval_text_sha256
      is distinct from v_request->>'policy_approval_text_sha256'
    or v_receipt.artifact_evidence->>'toolchain_evidence_sha256'
      is distinct from v_request->>'toolchain_evidence_sha256'
    or (v_request->>'maximum_process_posts')::integer
      is distinct from v_receipt.process_count then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_RECEIPT_BINDING_MISMATCH',
      'status', 409,
      'message', 'Preflight identity does not exactly bind the receipt'
    );
  end if;
  -- The owner fence is held through activation.  A concurrent row UPDATE
  -- blocked by the deterministic FOR SHARE proof resumes into this VOLATILE
  -- BEFORE trigger under READ COMMITTED, where the trigger's SQL sees the
  -- newly committed active scope and rejects the write.
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));

  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.actor_user_id = v_actor
    and scope.operation_id = v_receipt.operation_id;
  if v_scope.id is not null then
    if v_scope.receipt_id is distinct from v_receipt.id
      or v_scope.preflight_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_OPERATION_REUSE_MISMATCH',
        'status', 409, 'message', 'Operation is already sealed differently'
      );
    end if;
    if v_scope.status in ('cancelled', 'failed') then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_SCOPE_TERMINAL_CONFLICT',
        'status', 409, 'scope_status', v_scope.status,
        'message', 'A cancelled or failed scope cannot be replayed as v2'
      );
    end if;
  else
    if exists (
      select 1
      from util.dataset_flow_identity_wrapper_invocations as invocation
      where invocation.actor_user_id = v_actor
        and array[
          v_request->>'execution_approval_request_sha256',
          v_request->>'execution_approval_text_sha256',
          v_request->>'execution_approval_identity_sha256'
        ] && array[
          invocation.approval_request_sha256,
          invocation.approval_text_sha256,
          invocation.approval_identity_sha256
        ]
    ) then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_APPROVAL_REUSE_MISMATCH',
        'status', 409,
        'message', 'Execution approval hash was already consumed'
      );
    end if;
    if v_receipt.expires_at <= clock_timestamp() then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_RECEIPT_EXPIRED', 'status', 409,
        'message', 'Capture receipt expired before first consumption'
      );
    end if;
    v_live := private.dataset_flow_identity_whole_scope_proof_v2(
      v_actor, v_receipt.id, null, true
    );
    if coalesce((v_live->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_LIVE_DRIFT', 'status', 409,
        'message', 'Receipt baselines drifted before scope sealing'
      );
    end if;

    select coalesce(jsonb_agg(guard.mapping order by guard.ordinal), '[]'::jsonb)
    into v_mappings
    from util.dataset_flow_identity_capture_mapping_guards as guard
    where guard.receipt_id = v_receipt.id;
    select coalesce(jsonb_agg(guard.guard order by guard.ordinal), '[]'::jsonb)
    into v_supports
    from util.dataset_flow_identity_capture_support_guards as guard
    where guard.receipt_id = v_receipt.id;
    select coalesce(jsonb_agg(intent.manifest order by intent.ordinal), '[]'::jsonb)
    into v_processes
    from util.dataset_flow_identity_capture_process_intents as intent
    where intent.receipt_id = v_receipt.id;

    v_internal := jsonb_build_object(
      'schema_version', 'dataset-flow-identity-scope-preflight.v1',
      'request_id', v_request->>'request_id',
      'environment', v_receipt.environment,
      'project_ref', v_receipt.project_ref,
      'actor', jsonb_build_object(
        'user_id', v_actor, 'email', v_actor_email
      ),
      'target_visibility', 'owner_draft',
      'operation_id', v_receipt.operation_id,
      'plan_sha256', v_request->>'plan_sha256',
      'freeze_sha256', v_request->>'freeze_sha256',
      'approval_identity_sha256',
        v_request->>'execution_approval_identity_sha256',
      'approval_text_sha256',
        v_request->>'execution_approval_text_sha256',
      'toolchain_evidence_sha256',
        v_request->>'toolchain_evidence_sha256',
      'compatibility_policy', v_receipt.compatibility_policy,
      'support_snapshot_set_sha256',
        v_receipt.support_snapshot_set_sha256,
      'support_snapshots', v_supports,
      'source_universe_sha256', v_receipt.source_universe_sha256,
      'source_universe_count', 305,
      'mapping_set_sha256', v_receipt.mapping_set_sha256,
      'process_manifest_sha256', v_receipt.process_manifest_sha256,
      'protected_closure_sha256', v_receipt.protected_closure_sha256,
      'mappings', v_mappings,
      'processes', v_processes,
      'protected_closure', v_receipt.protected_closure
    );
    perform set_config(
      'app.dataset_flow_identity_receipt_id', v_receipt.id::text, true
    );
    perform set_config(
      'app.dataset_flow_identity_receipt_proof_sha256',
      v_receipt.receipt_proof_sha256, true
    );
    perform set_config(
      'app.dataset_flow_identity_policy_approval_sha256',
      v_receipt.policy_approval_text_sha256, true
    );
    perform set_config(
      'app.dataset_flow_identity_execution_request_sha256',
      v_request->>'execution_approval_request_sha256', true
    );
    perform set_config(
      'app.dataset_flow_identity_preflight_request_sha256',
      v_request_sha256, true
    );
    v_core_result :=
      private.dataset_flow_identity_scope_preflight_core_v1(v_internal);
    if coalesce((v_core_result->>'ok')::boolean, false) is false then
      return v_core_result;
    end if;
    select scope.* into v_scope
    from util.dataset_flow_identity_scopes as scope
    where scope.id = (v_core_result->>'scope_id')::uuid
      and scope.actor_user_id = v_actor;
    if v_scope.id is null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PREFLIGHT_SCOPE_MISSING_AFTER_CORE';
    end if;
    v_scope_proof_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
        'schema_version', 'dataset-flow-identity-scope-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'actor_user_id', v_actor,
        'environment', v_scope.environment,
        'project_ref', v_scope.project_ref,
        'operation_id', v_scope.operation_id,
        'plan_sha256', v_scope.plan_sha256,
        'freeze_sha256', v_scope.freeze_sha256,
        'policy_approval_text_sha256',
          v_scope.policy_approval_text_sha256,
        'execution_approval_request_sha256',
          v_scope.execution_approval_request_sha256,
        'execution_approval_text_sha256', v_scope.approval_text_sha256,
        'execution_approval_identity_sha256',
          v_scope.approval_identity_sha256,
        'toolchain_evidence_sha256', v_scope.toolchain_evidence_sha256,
        'preflight_request_sha256', v_scope.preflight_request_sha256,
        'capture_whole_scope_proof_sha256',
          v_receipt.whole_scope_proof_sha256
      ));
    update util.dataset_flow_identity_scopes
    set scope_proof_sha256 = v_scope_proof_sha256,
      updated_at = clock_timestamp()
    where id = v_scope.id;
    v_scope.scope_proof_sha256 := v_scope_proof_sha256;
  end if;

  select audit.id, audit.payload into v_audit_id, v_audit_payload
  from private.command_audit_log as audit
  where audit.command = v_command and audit.actor_user_id = v_actor
    and audit.target_table is null
    and audit.payload->>'scope_id' = v_scope.id::text
  order by audit.id desc limit 1;
  if v_audit_id is null then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PREFLIGHT_V2_AUDIT_MISSING';
  end if;
  v_expected_audit_payload := jsonb_build_object(
      'record_type', 'scope_seal',
      'schema_version', 'dataset-flow-identity-scope-preflight.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'scope_id', v_scope.id,
      'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'freeze_sha256', v_scope.freeze_sha256,
      'capture_request_sha256', v_receipt.capture_request_sha256,
      'capture_whole_scope_proof_sha256',
        v_receipt.whole_scope_proof_sha256,
      'source_guard_set_sha256', v_receipt.source_guard_set_sha256,
      'support_guard_set_sha256', v_receipt.support_snapshot_set_sha256,
      'target_guard_set_sha256', v_receipt.target_guard_set_sha256,
      'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
      'protected_closure_sha256', v_receipt.protected_closure_sha256,
      'policy_sha256', v_receipt.compatibility_policy->>'policy_sha256',
      'policy_approval_text_sha256',
        v_scope.policy_approval_text_sha256,
      'execution_approval_request_sha256',
        v_scope.execution_approval_request_sha256,
      'execution_approval_text_sha256', v_scope.approval_text_sha256,
      'execution_approval_identity_sha256',
        v_scope.approval_identity_sha256,
      'toolchain_evidence_sha256', v_scope.toolchain_evidence_sha256,
      'user_state_claim', v_scope.user_state_claim,
      'maximum_wrapper_invocations', 1,
      'maximum_process_posts', v_scope.process_count,
      'maximum_finalize_posts', 1,
      'maximum_cli_apply_spawns', 1,
      'approval_reusable', false,
      'automatic_retry', false,
      'preflight_request_sha256', v_scope.preflight_request_sha256,
      'scope_request_sha256', v_scope.scope_request_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'source_universe_count', 305,
      'mapping_count', v_scope.mapping_count,
      'process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
  if v_core_result is not null then
    update private.command_audit_log
    set payload = v_expected_audit_payload
    where id = v_audit_id and actor_user_id = v_actor
      and command = v_command and target_table is null
    returning payload into v_audit_payload;
  end if;
  -- Fresh execution promotes the core's v1 row first; replay never mutates it.
  -- In both cases an exact reread/compare precedes whole-scope proof so that
  -- audit_current cannot deterministically reject a just-created scope.
  if v_audit_payload is distinct from v_expected_audit_payload then
    if v_core_result is not null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PREFLIGHT_V2_AUDIT_PROMOTION_FAILED';
    end if;
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_V2_AUDIT_DRIFT', 'status', 409,
      'message', 'The authoritative v2 scope audit is missing or drifted'
    );
  end if;
  v_live := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, true
  );
  if coalesce((v_live->>'ok')::boolean, false) is false then
    if v_core_result is not null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PREFLIGHT_POST_SEAL_DRIFT';
    end if;
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
      'message', 'Sealed scope no longer matches live database state'
    );
  end if;
  if v_core_result is not null then
    v_permit_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
    insert into util.dataset_flow_identity_wrapper_invocations (
      scope_id, actor_user_id, approval_kind,
      approval_request_sha256, approval_text_sha256,
      approval_identity_sha256, admission_request_sha256,
      baseline_whole_scope_proof_sha256,
      token_sha256, maximum_process_posts, maximum_finalize_posts
    ) values (
      v_scope.id, v_actor, 'initial',
      v_scope.execution_approval_request_sha256,
      v_scope.approval_text_sha256,
      v_scope.approval_identity_sha256,
      v_scope.preflight_request_sha256,
      v_live->>'whole_scope_proof_sha256',
      private.dataset_flow_identity_permit_token_sha256_v1(v_permit_token),
      v_scope.process_count, 1
    ) returning id into v_invocation_id;
    v_execution_permit := jsonb_build_object(
      'schema_version', 'dataset-flow-identity-execution-permit.v1',
      'invocation_id', v_invocation_id,
      'generation', 0,
      'token', v_permit_token
    );
  end if;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-preflight-result.v2',
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'scope_id', v_scope.id,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_scope.status,
    'process_count', v_scope.process_count,
    'mapping_count', v_scope.mapping_count,
    'support_snapshot_count', v_receipt.support_count,
    'source_universe_count', 305,
    'rewrite_count', v_scope.rewrite_count,
    'next_ordinal', coalesce((
      select min(ledger.ordinal)
      from util.dataset_flow_identity_process_ledger as ledger
      where ledger.scope_id = v_scope.id and ledger.status = 'pending'
    ), v_scope.process_count + 1),
    'audit_id', v_audit_id::text,
    'replay', v_core_result is null,
    'execution_permit', v_execution_permit
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_PREFLIGHT_LOCK_BUSY', 'status', 409,
    'message', 'Scope seal could not acquire its deterministic fence'
  );
end;
$_$;

ALTER FUNCTION "api"."cmd_dataset_flow_identity_scope_preflight_guarded"("p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_flow_identity_scope_preflight_guarded"("p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_flow_identity_scope_preflight_guarded"("p_request" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_flow_identity_scope_preflight_guarded"("p_request" "jsonb") TO "authenticated";
