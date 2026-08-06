CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_whole_scope_proof_v2"("p_actor" "uuid", "p_receipt_id" "uuid", "p_scope_id" "uuid" DEFAULT NULL::"uuid", "p_lock_rows" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_supports jsonb;
  v_source_guards_current boolean := false;
  v_target_guards_current boolean := false;
  v_support_guards_current boolean := false;
  v_mapping_guards_current boolean := false;
  v_source_universe_current boolean := false;
  v_protected_proof jsonb;
  v_protected_current boolean := false;
  v_occurrence_current boolean := false;
  v_primary_current boolean := false;
  v_scope_audit_current boolean := false;
  v_audit_current boolean := false;
  v_terminal_audit_current boolean := true;
  v_derivative_proof jsonb;
  v_raw_derivatives_current boolean := false;
  v_derivatives_current boolean := false;
  v_expected_residue bigint := 0;
  v_observed_residue bigint := 0;
  v_primary_entries jsonb := '[]'::jsonb;
  v_primary_closure_sha256 text;
  v_derivative_targets jsonb;
  v_derivative_target_set_sha256 text;
  v_expected_terminal_proof_sha256 text;
  v_expected_terminal_audit_payload jsonb;
  v_terminal_audit_payload jsonb;
  v_terminal_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_terminal_audit_count integer := 0;
  v_proof jsonb;
  v_proof_sha256 text;
  v_completed integer := 0;
  v_process_count integer := 0;
begin
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = p_receipt_id and receipt.actor_user_id = p_actor;
  if v_receipt.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_RECEIPT_NOT_FOUND'
    );
  end if;
  if p_scope_id is not null then
    select scope.* into v_scope
    from util.dataset_flow_identity_scopes as scope
    where scope.id = p_scope_id and scope.actor_user_id = p_actor
      and scope.receipt_id = p_receipt_id;
    if v_scope.id is null then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND'
      );
    end if;
  end if;

  if p_lock_rows then
    perform 1
    from public.flows as flow
    join (
      select guard.source_id as id, guard.source_version as version
      from util.dataset_flow_identity_capture_source_guards as guard
      where guard.receipt_id = p_receipt_id
      union
      select guard.target_id, guard.target_version
      from util.dataset_flow_identity_capture_target_guards as guard
      where guard.receipt_id = p_receipt_id
    ) as wanted
      on wanted.id = flow.id and wanted.version = btrim(flow.version::text)
    order by flow.id, btrim(flow.version::text)
    for share of flow;
    perform 1
    from public.flowproperties as support
    join util.dataset_flow_identity_capture_support_guards as guard
      on guard.receipt_id = p_receipt_id
      and guard.support_table = 'flowproperties'
      and guard.support_id = support.id
      and guard.support_version = btrim(support.version::text)
    order by support.id, btrim(support.version::text)
    for share of support;
    perform 1
    from public.unitgroups as support
    join util.dataset_flow_identity_capture_support_guards as guard
      on guard.receipt_id = p_receipt_id
      and guard.support_table = 'unitgroups'
      and guard.support_id = support.id
      and guard.support_version = btrim(support.version::text)
    order by support.id, btrim(support.version::text)
    for share of support;
    perform 1
    from public.processes as process
    join (
      select intent.process_id as id, intent.process_version as version
      from util.dataset_flow_identity_capture_process_intents as intent
      where intent.receipt_id = p_receipt_id
      union
      select (occurrence.value->>'process_id')::uuid,
        occurrence.value->>'process_version'
      from jsonb_array_elements(
        coalesce(v_receipt.protected_closure->'pending', '[]'::jsonb)
        || coalesce(v_receipt.protected_closure->'blockers', '[]'::jsonb)
      ) as entry(value)
      cross join lateral jsonb_array_elements(
        entry.value->'occurrences'
      ) as occurrence(value)
    ) as wanted
      on wanted.id = process.id
      and wanted.version = btrim(process.version::text)
    order by process.id, btrim(process.version::text)
    for share of process;
  end if;

  select coalesce(jsonb_agg(guard.guard order by guard.ordinal), '[]'::jsonb)
  into v_supports
  from util.dataset_flow_identity_capture_support_guards as guard
  where guard.receipt_id = p_receipt_id;
  v_support_guards_current := coalesce((
    util.dataset_flow_identity_validate_support_set(
      p_actor, v_supports, v_receipt.support_snapshot_set_sha256
    )->>'ok'
  )::boolean, false);
  select count(*) = 305 and bool_and(coalesce((
    util.dataset_flow_identity_validate_flow_guard(
      p_actor, guard.guard, false, v_supports
    )->>'ok'
  )::boolean, false))
  into v_source_guards_current
  from util.dataset_flow_identity_capture_source_guards as guard
  where guard.receipt_id = p_receipt_id;
  select count(*) = v_receipt.target_count and bool_and(coalesce((
    util.dataset_flow_identity_validate_flow_guard(
      p_actor, guard.guard, true, v_supports
    )->>'ok'
  )::boolean, false))
  into v_target_guards_current
  from util.dataset_flow_identity_capture_target_guards as guard
  where guard.receipt_id = p_receipt_id;
  select count(*) = v_receipt.mapping_count and bool_and(coalesce((
    util.dataset_flow_identity_validate_mapping(
      p_actor, guard.mapping, v_receipt.compatibility_policy,
      v_supports, guard.ordinal
    )->>'ok'
  )::boolean, false))
  into v_mapping_guards_current
  from util.dataset_flow_identity_capture_mapping_guards as guard
  where guard.receipt_id = p_receipt_id;
  v_source_universe_current := coalesce((
    util.dataset_flow_identity_source_universe(
      p_actor, v_receipt.source_universe, v_receipt.source_universe_sha256
    )->>'ok'
  )::boolean, false);
  v_protected_proof := util.dataset_flow_identity_protected_closure(
    p_actor, v_receipt.protected_closure
  );
  v_protected_current := coalesce(
    (v_protected_proof->>'ok')::boolean, false
  );

  if p_scope_id is null then
    select
      count(*)::integer,
      count(*) filter (
        where process.id is not null
          and util.dataset_flow_identity_sha256(process.json_ordered::jsonb)
            = intent.manifest->>'before_payload_sha256'
          and util.dataset_flow_identity_sha256(
            private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
          ) = intent.manifest->>'before_exchange_set_sha256'
      )::integer
    into v_process_count, v_completed
    from util.dataset_flow_identity_capture_process_intents as intent
    left join public.processes as process
      on process.id = intent.process_id
      and btrim(process.version::text) = intent.process_version
      and process.user_id = p_actor and process.state_code = 0
      and process.json::jsonb = process.json_ordered::jsonb
    where intent.receipt_id = p_receipt_id;
    v_primary_current := v_process_count = v_receipt.process_count
      and v_completed = v_receipt.process_count;
    v_audit_current := true;
    v_expected_residue := v_receipt.rewrite_count;
  else
    select count(*) = 1 and bool_and(
      audit.payload = jsonb_build_object(
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
      )
    )
    into v_scope_audit_current
    from private.command_audit_log as audit
    where audit.command = 'cmd_dataset_flow_identity_scope_preflight_guarded'
      and audit.actor_user_id = p_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = p_scope_id::text;

    with live as (
      select ledger.*, process.id as live_id,
        util.dataset_flow_identity_sha256(process.json_ordered::jsonb)
          as live_payload_sha256,
        util.dataset_flow_identity_sha256(
          private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
        ) as live_exchange_sha256,
        audit.id as live_audit_id,
        audit.payload as live_audit_payload,
        invocation.id as live_invocation_id,
        invocation.generation as live_invocation_generation,
        invocation.approval_kind as live_approval_kind,
        invocation.approval_identity_sha256
          as live_approval_identity_sha256,
        invocation.admission_request_sha256
          as live_admission_request_sha256
      from util.dataset_flow_identity_process_ledger as ledger
      left join public.processes as process
        on process.id = ledger.process_id
        and btrim(process.version::text) = ledger.process_version
        and process.user_id = p_actor and process.state_code = 0
        and process.json is not null and process.json_ordered is not null
        and process.json::jsonb = process.json_ordered::jsonb
      left join private.command_audit_log as audit
        on audit.id = ledger.audit_id
        and audit.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
        and audit.actor_user_id = p_actor
        and audit.target_table = 'processes'
        and audit.target_id = ledger.process_id
        and audit.target_version = ledger.process_version
        and audit.payload->>'scope_id' = p_scope_id::text
        and audit.payload->>'process_request_sha256'
          = ledger.process_request_sha256
      left join util.dataset_flow_identity_wrapper_invocations as invocation
        on invocation.id = ledger.wrapper_invocation_id
        and invocation.scope_id = ledger.scope_id
        and invocation.actor_user_id = p_actor
      where ledger.scope_id = p_scope_id
    )
    select
      count(*)::integer,
      count(*) filter (where status = 'completed')::integer,
      bool_and(
        live_id is not null and case when status = 'completed'
          then live_payload_sha256 = manifest->>'desired_payload_sha256'
            and live_exchange_sha256 = manifest->>'desired_exchange_set_sha256'
          else live_payload_sha256 = manifest->>'before_payload_sha256'
            and live_exchange_sha256 = manifest->>'before_exchange_set_sha256'
        end
      ),
      bool_and(case when status = 'completed' then
        live_audit_id is not null
        and live_invocation_id is not null
        and permit_generation_before is not null
        and live_invocation_generation > permit_generation_before
        and live_audit_payload = jsonb_build_object(
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
          'wrapper_invocation_id', live_invocation_id,
          'wrapper_approval_kind', live_approval_kind,
          'wrapper_approval_identity_sha256',
            live_approval_identity_sha256,
          'wrapper_admission_request_sha256',
            live_admission_request_sha256,
          'permit_generation_before', permit_generation_before,
          'ordinal', ordinal,
          'process_intent_proof_sha256', process_intent_proof_sha256,
          'process_request_sha256', process_request_sha256,
          'process_template_sha256', process_template_sha256,
          'rewrite_set_sha256', manifest->>'rewrite_set_sha256',
          'collision_ledger_sha256', manifest->>'collision_ledger_sha256',
          'before_payload_sha256', before_payload_sha256,
          'before_exchange_set_sha256',
            manifest->>'before_exchange_set_sha256',
          'desired_payload_sha256', manifest->>'desired_payload_sha256',
          'desired_exchange_set_sha256',
            manifest->>'desired_exchange_set_sha256',
          'after_payload_sha256', after_payload_sha256,
          'after_exchange_set_sha256', after_exchange_set_sha256,
          'rewrite_count', rewrite_count,
          'derivative_batch_id', derivative_batch_id,
          'derivative_reason_code', 'FLOW_IDENTITY_SCOPE:'
            || v_scope.id::text || ':' || ordinal::text,
          'hash_algorithm', 'restricted-safe-json-v2-sha256'
        )
        else live_audit_id is null end),
      coalesce(sum(rewrite_count) filter (where status = 'pending'), 0)::bigint,
      coalesce(jsonb_agg(jsonb_build_object(
        'ordinal', ordinal,
        'id', process_id,
        'version', process_version,
        'json_ordered_sha256', after_payload_sha256,
        'exchange_set_sha256', after_exchange_set_sha256,
        'audit_id', case when audit_id is null then null else audit_id::text end,
        'wrapper_invocation_id', wrapper_invocation_id,
        'permit_generation_before', permit_generation_before
      ) order by ordinal) filter (where status = 'completed'), '[]'::jsonb)
    into v_process_count, v_completed, v_primary_current,
      v_audit_current, v_expected_residue, v_primary_entries
    from live;
    v_audit_current := coalesce(v_scope_audit_current, false)
      and coalesce(v_audit_current, false);
  end if;
  select count(*)::bigint into v_observed_residue
  from public.processes as process
  cross join lateral jsonb_array_elements(
    private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
  ) as exchange(value)
  join util.dataset_flow_identity_capture_mapping_guards as mapping
    on mapping.receipt_id = p_receipt_id
    and exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
      = mapping.source_id::text
    and exchange.value #>> '{referenceToFlowDataSet,@version}'
      = mapping.source_version
  where process.user_id = p_actor and process.state_code = 0;
  v_occurrence_current := v_observed_residue = v_expected_residue
    and v_protected_current;
  v_primary_closure_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_primary_entries);
  if p_scope_id is null then
    v_derivative_proof := jsonb_build_object(
      'proof_sha256', repeat('0', 64), 'causal_terminal_proof', false
    );
  else
    v_derivative_proof :=
      util.read_dataset_flow_identity_derivative_set(p_actor, p_scope_id);
  end if;
  v_raw_derivatives_current := p_scope_id is not null
    and v_completed = v_process_count
    and coalesce((v_derivative_proof->>'causal_terminal_proof')::boolean, false);
  -- A derivative can be terminal while a primary row or immutable guard has
  -- drifted.  Never expose that raw child terminality as scope currency.
  v_derivatives_current := v_raw_derivatives_current
    and coalesce(v_primary_current, false)
    and coalesce(v_audit_current, false)
    and coalesce(v_source_guards_current, false)
    and coalesce(v_source_universe_current, false)
    and coalesce(v_mapping_guards_current, false)
    and coalesce(v_support_guards_current, false)
    and coalesce(v_target_guards_current, false)
    and v_protected_current and v_occurrence_current;

  v_proof := jsonb_build_object(
    'schema_version', 'dataset-flow-identity-whole-scope-proof.v2',
    'scope_id', p_scope_id,
    'receipt_id', p_receipt_id,
    'primary_current', coalesce(v_primary_current, false),
    'audit_current', coalesce(v_audit_current, false),
    'source_guards_current', coalesce(v_source_guards_current, false)
      and coalesce(v_source_universe_current, false)
      and coalesce(v_mapping_guards_current, false),
    'support_guards_current', coalesce(v_support_guards_current, false),
    'target_guards_current', coalesce(v_target_guards_current, false),
    'approved_reference_residue_count', v_observed_residue,
    'protected_closure_current', v_protected_current,
    'occurrence_closure_current', v_occurrence_current,
    'derivatives_current', v_derivatives_current,
    'primary_closure_sha256', v_primary_closure_sha256,
    'source_guard_set_sha256', v_receipt.source_guard_set_sha256,
    'support_guard_set_sha256', v_receipt.support_snapshot_set_sha256,
    'target_guard_set_sha256', v_receipt.target_guard_set_sha256,
    'protected_closure_sha256', v_receipt.protected_closure_sha256,
    'derivative_proof_set_sha256',
      coalesce(v_derivative_proof->>'proof_sha256', repeat('0', 64)),
    'causal_terminal_proof', false,
    'proof_sha256', ''
  );
  v_proof := jsonb_set(
    v_proof, '{causal_terminal_proof}',
    to_jsonb(
      coalesce(v_primary_current, false)
      and coalesce(v_audit_current, false)
      and coalesce(v_source_guards_current, false)
      and coalesce(v_source_universe_current, false)
      and coalesce(v_mapping_guards_current, false)
      and coalesce(v_support_guards_current, false)
      and coalesce(v_target_guards_current, false)
      and v_protected_current and v_occurrence_current
      and v_derivatives_current and v_observed_residue = 0
    ), false
  );
  v_proof_sha256 := util.dataset_flow_identity_restricted_sha256_v2(v_proof);
  if p_scope_id is not null and v_scope.status = 'completed' then
    select invocation.* into v_terminal_invocation
    from util.dataset_flow_identity_wrapper_invocations as invocation
    where invocation.id = v_scope.final_wrapper_invocation_id
      and invocation.scope_id = v_scope.id
      and invocation.actor_user_id = p_actor;
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
    where ledger.scope_id = p_scope_id;
    v_derivative_target_set_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(v_derivative_targets);
    select count(*)::integer, jsonb_agg(audit.payload order by audit.id)->0
    into v_terminal_audit_count, v_terminal_audit_payload
    from private.command_audit_log as audit
    where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
      and audit.actor_user_id = p_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = p_scope_id::text;
    v_expected_terminal_proof_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
        'schema_version', 'dataset-flow-identity-terminal-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'plan_sha256', v_scope.plan_sha256,
        'final_request_sha256', v_scope.final_request_sha256,
        'wrapper_invocation_id', v_terminal_invocation.id,
        'wrapper_approval_kind', v_terminal_invocation.approval_kind,
        'wrapper_approval_identity_sha256',
          v_terminal_invocation.approval_identity_sha256,
        'wrapper_admission_request_sha256',
          v_terminal_invocation.admission_request_sha256,
        'permit_generation_before',
          v_scope.final_permit_generation_before,
        'whole_scope_proof_sha256',
          v_terminal_audit_payload->>'whole_scope_proof_sha256',
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'derivative_target_set_sha256', v_derivative_target_set_sha256,
        'derivative_proof_set_sha256',
          v_terminal_audit_payload->>'derivative_proof_set_sha256'
      ));
    v_expected_terminal_audit_payload := jsonb_build_object(
      'record_type', 'scope_terminal',
      'schema_version', 'dataset-flow-identity-scope-finalize.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'scope_id', v_scope.id,
      'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'final_request_sha256', v_scope.final_request_sha256,
      'wrapper_invocation_id', v_terminal_invocation.id,
      'wrapper_approval_kind', v_terminal_invocation.approval_kind,
      'wrapper_approval_identity_sha256',
        v_terminal_invocation.approval_identity_sha256,
      'wrapper_admission_request_sha256',
        v_terminal_invocation.admission_request_sha256,
      'permit_generation_before', v_scope.final_permit_generation_before,
      'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
      'protected_closure_sha256', v_receipt.protected_closure_sha256,
      'primary_closure_sha256', v_primary_closure_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivative_proof_set_sha256',
        v_terminal_audit_payload->>'derivative_proof_set_sha256',
      'whole_scope_proof_sha256',
        v_terminal_audit_payload->>'whole_scope_proof_sha256',
      'terminal_proof_sha256', v_expected_terminal_proof_sha256,
      'process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
    -- Completion-time derivative/whole-proof hashes are historical facts.
    -- Bind them through the terminal proof, but do not compare them with the
    -- current derivative proof: a later stale child must become compensation,
    -- not masquerade as primary/audit drift.
    v_terminal_audit_current := v_terminal_audit_count = 1
      and v_terminal_invocation.id is not null
      and v_scope.final_permit_generation_before is not null
      and v_terminal_invocation.status = 'completed'
      and v_terminal_invocation.successful_finalize_posts = 1
      and v_terminal_invocation.generation =
        v_scope.final_permit_generation_before + 1
      and v_terminal_audit_payload = v_expected_terminal_audit_payload
      and v_terminal_audit_payload->>'whole_scope_proof_sha256'
        ~ '^[a-f0-9]{64}$'
      and v_terminal_audit_payload->>'derivative_proof_set_sha256'
        ~ '^[a-f0-9]{64}$'
      and v_scope.terminal_proof_sha256
        is not distinct from v_expected_terminal_proof_sha256;
    if not v_terminal_audit_current then
      v_audit_current := false;
      v_derivatives_current := false;
      v_proof := jsonb_set(v_proof, '{audit_current}', 'false'::jsonb, false);
      v_proof := jsonb_set(
        v_proof, '{derivatives_current}', 'false'::jsonb, false
      );
      v_proof := jsonb_set(
        v_proof, '{causal_terminal_proof}', 'false'::jsonb, false
      );
      v_proof_sha256 :=
        util.dataset_flow_identity_restricted_sha256_v2(v_proof);
    end if;
  end if;
  v_proof := jsonb_set(
    v_proof, '{proof_sha256}', to_jsonb(v_proof_sha256), false
  );
  return jsonb_build_object(
    'ok', coalesce(v_primary_current, false)
      and coalesce(v_audit_current, false)
      and coalesce(v_source_guards_current, false)
      and coalesce(v_source_universe_current, false)
      and coalesce(v_mapping_guards_current, false)
      and coalesce(v_support_guards_current, false)
      and coalesce(v_target_guards_current, false)
      and v_protected_current and v_occurrence_current,
    'whole_scope_proof', v_proof,
    'whole_scope_proof_sha256', v_proof_sha256,
    'derivative_set_proof', v_derivative_proof
  );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_whole_scope_proof_v2"("p_actor" "uuid", "p_receipt_id" "uuid", "p_scope_id" "uuid", "p_lock_rows" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_whole_scope_proof_v2"("p_actor" "uuid", "p_receipt_id" "uuid", "p_scope_id" "uuid", "p_lock_rows" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_whole_scope_proof_v2"("p_actor" "uuid", "p_receipt_id" "uuid", "p_scope_id" "uuid", "p_lock_rows" boolean) TO "api_internal_executor";
