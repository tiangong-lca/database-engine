CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_scope_finalize_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '180s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_finalize_guarded';
  v_schema_version constant text :=
    'dataset-flow-identity-scope-finalize.v1';
  v_result_schema_version constant text :=
    'dataset-flow-identity-scope-finalize-result.v1';
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_mapping util.dataset_flow_identity_mappings%rowtype;
  v_mapping_validation jsonb;
  v_support_proof jsonb;
  v_universe_proof jsonb;
  v_protected_proof jsonb;
  v_derivative_set_proof jsonb;
  v_primary_entries jsonb;
  v_derivative_targets jsonb;
  v_primary_closure_sha256 text;
  v_derivative_target_set_sha256 text;
  v_request_sha256 text;
  v_terminal_proof_sha256 text;
  v_completed integer;
  v_rewrite_count integer;
  v_audit_count integer;
  v_primary_drift_count integer;
  v_approved_reference_residue bigint;
  v_final_audit_id bigint;
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
      array['schema_version', 'request_id', 'scope_proof_sha256', 'expected']
    )
    or p_request->>'schema_version' <> v_schema_version
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or not private.dataset_flow_identity_exact_keys(
      p_request->'expected',
      array[
        'process_count', 'rewrite_count', 'completed_process_count',
        'primary_closure_sha256', 'protected_closure_sha256',
        'derivative_target_set_sha256'
      ]
    )
    or p_request #>> '{expected,process_count}' !~ '^[1-9][0-9]*$'
    or p_request #>> '{expected,rewrite_count}' !~ '^[1-9][0-9]*$'
    or p_request #>> '{expected,completed_process_count}'
      !~ '^[1-9][0-9]*$'
    or p_request #>> '{expected,primary_closure_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_request #>> '{expected,protected_closure_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_request #>> '{expected,derivative_target_set_sha256}'
      !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 finalize request schema mismatch'
    );
  end if;
  if not pg_try_advisory_xact_lock(
    hashtextextended('dataset-flow-identity:' || p_scope_id::text, 0)
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_BUSY', 'status', 409,
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
  if v_scope.status = 'cancelled' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_CANCELLED', 'status', 409,
      'message', 'A cancelled zero-write scope cannot be finalized'
    );
  end if;
  if v_scope.scope_proof_sha256
      is distinct from p_request->>'scope_proof_sha256'
    or (p_request #>> '{expected,process_count}')::integer
      <> v_scope.process_count
    or (p_request #>> '{expected,completed_process_count}')::integer
      <> v_scope.process_count
    or (p_request #>> '{expected,rewrite_count}')::integer
      <> v_scope.rewrite_count
    or p_request #>> '{expected,protected_closure_sha256}'
      is distinct from v_scope.protected_closure_sha256 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Finalize request does not match the sealed scope'
    );
  end if;

  v_request_sha256 := coalesce(nullif(current_setting(
    'app.dataset_flow_identity_v2_finalize_request_sha256', true
  ), ''), util.dataset_flow_identity_restricted_sha256_v2(p_request));
  if v_scope.status = 'completed' then
    if v_scope.final_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_FINALIZE_REPLAY_MISMATCH', 'status', 409,
        'message', 'Completed scope was finalized by a different request'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', v_result_schema_version,
      'scope_id', v_scope.id, 'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', 'completed', 'process_count', v_scope.process_count,
      'completed_process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'primary_closure_sha256',
        p_request #>> '{expected,primary_closure_sha256}',
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'derivative_target_set_sha256',
        p_request #>> '{expected,derivative_target_set_sha256}',
      'derivatives_current', true,
      'terminal_proof_sha256', v_scope.terminal_proof_sha256,
      'replay', true
    );
  end if;

  v_support_proof := util.dataset_flow_identity_validate_support_set(
    v_actor, v_scope.support_snapshots,
    v_scope.support_snapshot_set_sha256
  );
  v_universe_proof := util.dataset_flow_identity_source_universe(
    v_actor, v_scope.source_universe, v_scope.source_universe_sha256
  );
  if coalesce((v_support_proof->>'ok')::boolean, false) is false
    or coalesce((v_universe_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_GUARD_DRIFT', 'status', 409,
      'message', 'Source universe or FP/UG support set drifted',
      'support_proof', v_support_proof,
      'source_universe_proof', v_universe_proof
    );
  end if;

  with primary_row as materialized (
    select
      ledger.*,
      process.id as live_id,
      util.dataset_flow_identity_sha256(process.json_ordered::jsonb)
        as live_payload_sha256,
      util.dataset_flow_identity_sha256(
        private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
      ) as live_exchange_sha256,
      process.model_id as live_model_id,
      process.rule_verification as live_rule_verification,
      audit.id as live_audit_id
    from util.dataset_flow_identity_process_ledger as ledger
    left join public.processes as process
      on process.id = ledger.process_id
      and btrim(process.version::text) = ledger.process_version
      and process.user_id = v_actor and process.state_code = 0
      and process.json is not null and process.json_ordered is not null
      and process.json::jsonb = process.json_ordered::jsonb
    left join private.command_audit_log as audit
      on audit.id = ledger.audit_id
      and audit.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
      and audit.actor_user_id = v_actor
      and audit.target_table = 'processes'
      and audit.target_id = ledger.process_id
      and audit.target_version = ledger.process_version
      and audit.payload->>'scope_id' = p_scope_id::text
      and audit.payload->>'process_request_sha256'
        = ledger.process_request_sha256
      and audit.payload->>'derivative_batch_id'
        = ledger.derivative_batch_id::text
      and audit.payload->>'derivative_reason_code'
        = 'FLOW_IDENTITY_SCOPE:' || p_scope_id::text || ':'
          || ledger.ordinal::text
    where ledger.scope_id = p_scope_id
  )
  select
    count(*) filter (where status = 'completed')::integer,
    coalesce(sum(rewrite_count) filter (where status = 'completed'), 0)::integer,
    count(*) filter (where status = 'completed'
      and live_audit_id is not null)::integer,
    count(*) filter (
      where status <> 'completed'
        or live_id is null
        or live_payload_sha256
          is distinct from manifest->>'desired_payload_sha256'
        or live_exchange_sha256
          is distinct from manifest->>'desired_exchange_set_sha256'
        or coalesce(to_jsonb(live_model_id), 'null'::jsonb)
          is distinct from manifest->'model_id'
        or coalesce(to_jsonb(live_rule_verification), 'null'::jsonb)
          is distinct from manifest->'rule_verification'
        or live_audit_id is null
    )::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ordinal,
      'id', process_id,
      'version', process_version,
      'json_ordered_sha256', after_payload_sha256,
      'exchange_set_sha256', after_exchange_set_sha256,
      'audit_id', audit_id::text,
      'wrapper_invocation_id', wrapper_invocation_id,
      'permit_generation_before', permit_generation_before
    ) order by ordinal), '[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ordinal,
      'id', process_id,
      'version', process_version,
      'desired_json_ordered_sha256', manifest->>'desired_payload_sha256',
      'baseline_snapshot_sha256',
        manifest->>'derivative_baseline_snapshot_sha256'
    ) order by ordinal), '[]'::jsonb)
  into v_completed, v_rewrite_count, v_audit_count,
    v_primary_drift_count, v_primary_entries, v_derivative_targets
  from primary_row;

  if v_completed <> v_scope.process_count
    or v_rewrite_count <> v_scope.rewrite_count
    or v_audit_count <> v_scope.process_count
    or v_primary_drift_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_PRIMARY_INCOMPLETE', 'status', 409,
      'message', 'Not every sealed process has exact live primary/audit proof',
      'primary_drift_count', v_primary_drift_count
    );
  end if;
  v_primary_closure_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    v_primary_entries
  );
  v_derivative_target_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    v_derivative_targets
  );
  if v_primary_closure_sha256 is distinct from
      p_request #>> '{expected,primary_closure_sha256}'
    or v_derivative_target_set_sha256 is distinct from
      p_request #>> '{expected,derivative_target_set_sha256}' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_CLOSURE_HASH_MISMATCH', 'status', 409,
      'message', 'Primary or derivative target closure hash is not exact',
      'primary_closure_sha256', v_primary_closure_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256
    );
  end if;

  for v_mapping in
    select mapping.*
    from util.dataset_flow_identity_mappings as mapping
    where mapping.scope_id = p_scope_id
    order by mapping.ordinal
  loop
    v_mapping_validation := util.dataset_flow_identity_validate_mapping(
      v_actor, v_mapping.mapping, v_scope.compatibility_policy,
      v_scope.support_snapshots, v_mapping.ordinal
    );
    if coalesce((v_mapping_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_FINALIZE_MAPPING_DRIFT', 'status', 409,
        'message', 'A source/public/support row changed after scope seal',
        'details', v_mapping_validation
      );
    end if;
  end loop;

  select count(*)::bigint into v_approved_reference_residue
  from public.processes as process
  cross join lateral jsonb_array_elements(
    private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
  ) as exchange(value)
  join util.dataset_flow_identity_mappings as mapping
    on mapping.scope_id = p_scope_id
    and exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
      = mapping.source_id::text
    and exchange.value #>> '{referenceToFlowDataSet,@version}'
      = mapping.source_version
  where process.user_id = v_actor and process.state_code = 0;
  v_protected_proof := util.dataset_flow_identity_protected_closure(
    v_actor, v_scope.protected_closure
  );
  if v_approved_reference_residue <> 0
    or coalesce((v_protected_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_REFERENCE_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Approved references remain or protected closure changed',
      'approved_reference_residue', v_approved_reference_residue,
      'protected_closure_proof', v_protected_proof
    );
  end if;

  v_derivative_set_proof :=
    util.read_dataset_flow_identity_derivative_set(v_actor, p_scope_id);
  if (v_derivative_set_proof->>'target_count')::integer
      is distinct from v_scope.process_count
    or coalesce(
      (v_derivative_set_proof->>'causal_terminal_proof')::boolean,
      false
    ) is false then
    return jsonb_build_object(
      'ok', (v_derivative_set_proof->>'failed_count')::integer = 0,
      'command', v_command,
      'schema_version', v_result_schema_version,
      'code', case when (v_derivative_set_proof->>'failed_count')::integer > 0
        then 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
        else 'FLOW_IDENTITY_DERIVATIVES_PENDING' end,
      'scope_id', p_scope_id,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', 'derivatives_pending',
      'process_count', v_scope.process_count,
      'completed_process_count', v_completed,
      'rewrite_count', v_scope.rewrite_count,
      'primary_closure_sha256', v_primary_closure_sha256,
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivatives_current', false,
      'derivatives_pending_count',
        (v_derivative_set_proof->>'pending_count')::integer,
      'derivatives_failed_count',
        (v_derivative_set_proof->>'failed_count')::integer,
      'compensation_required',
        (v_derivative_set_proof->>'failed_count')::integer > 0,
      'compensation_targets',
        v_derivative_set_proof->'compensation_targets',
      'derivative_proofs', v_derivative_set_proof->'targets',
      'derivative_proof_set_sha256',
        v_derivative_set_proof->>'proof_sha256',
      'terminal_proof_sha256', null,
      'automatic_retry', false,
      'replay', false
    );
  end if;

  v_terminal_proof_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    jsonb_build_object(
      'schema_version', 'dataset-flow-identity-terminal-proof.v1',
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'plan_sha256', v_scope.plan_sha256,
      'primary_closure_sha256', v_primary_closure_sha256,
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'protected_observed_sha256', v_protected_proof->>'observed_sha256',
      'source_universe_sha256', v_scope.source_universe_sha256,
      'support_snapshot_set_sha256', v_scope.support_snapshot_set_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivative_proof_set_sha256',
        v_derivative_set_proof->>'proof_sha256',
      'process_audit_count', v_audit_count,
      'rewrite_count', v_scope.rewrite_count
    )
  );
  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null,
    jsonb_build_object(
      'record_type', 'scope_terminal',
      'schema_version', v_schema_version,
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'final_request_sha256', v_request_sha256,
      'primary_closure_sha256', v_primary_closure_sha256,
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'source_universe_sha256', v_scope.source_universe_sha256,
      'support_snapshot_set_sha256', v_scope.support_snapshot_set_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivative_proof_set_sha256',
        v_derivative_set_proof->>'proof_sha256',
      'terminal_proof_sha256', v_terminal_proof_sha256,
      'process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'process_audit_count', v_audit_count,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_final_audit_id;
  update util.dataset_flow_identity_process_ledger
  set active = false where scope_id = p_scope_id;
  update util.dataset_flow_identity_scopes
  set status = 'completed', final_request_sha256 = v_request_sha256,
    terminal_proof_sha256 = v_terminal_proof_sha256,
    completed_at = clock_timestamp(), updated_at = clock_timestamp()
  where id = p_scope_id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', v_result_schema_version,
    'scope_id', p_scope_id, 'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', 'completed',
    'process_count', v_scope.process_count,
    'completed_process_count', v_completed,
    'rewrite_count', v_scope.rewrite_count,
    'primary_closure_sha256', v_primary_closure_sha256,
    'protected_closure_sha256', v_scope.protected_closure_sha256,
    'derivative_target_set_sha256', v_derivative_target_set_sha256,
    'derivative_proof_set_sha256',
      v_derivative_set_proof->>'proof_sha256',
    'derivatives_current', true,
    'terminal_proof_sha256', v_terminal_proof_sha256,
    'audit_id', v_final_audit_id::text,
    'replay', false
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_FINALIZE_LOCK_BUSY', 'status', 409,
    'message', 'Finalization could not acquire its bounded lock'
  );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_scope_finalize_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_scope_finalize_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_scope_finalize_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") TO "api_internal_executor";
