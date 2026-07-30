CREATE OR REPLACE FUNCTION "public"."cmd_dataset_alias_execution_gate_guarded"("p_request_id" "uuid", "p_preflight_token" "text", "p_gate_name" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '55s'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_preflight util.dataset_alias_execution_preflights%rowtype;
  v_expected_name text;
  v_expected_sha256 text;
  v_material jsonb;
  v_observed_sha256 text;
  v_receipt_material jsonb;
  v_receipt_sha256 text;
  v_captured_at timestamp with time zone;
  v_alias_result jsonb;
  v_batch_result jsonb;
  v_simulation_passed boolean := false;
  v_execution_count integer := 0;
  v_alias_audit_count integer := 0;
  v_derivative_child_count integer := 0;
  v_snapshot_drift_count integer := 0;
  v_active_rebuild_count integer := 0;
  v_http_count integer := 0;
  v_extraction_count integer := 0;
  v_embedding_count integer := 0;
  v_pending_count integer := 0;
  v_failure_material jsonb;
  v_failure_sha256 text;
  v_target jsonb;
  v_snapshot jsonb;
  v_existing_gate_count integer := 0;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_request_id is null
    or p_preflight_token is null
    or p_preflight_token !~ '^[a-f0-9]{64}$'
    or p_gate_name not in (
      'primary_support_plan',
      'execution_unused',
      'derivative_quiescence'
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_INVALID_REQUEST',
      'status', 400,
      'message', 'Exact request ID, preflight token, and known gate name are required'
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      v_actor::text || ':' || p_request_id::text || ':' || p_gate_name,
      0
    )
  );

  select preflight.*
  into v_preflight
  from util.dataset_alias_execution_preflights as preflight
  where preflight.id = p_request_id
    and preflight.actor_user_id = v_actor
  for update;

  if v_preflight.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_NOT_FOUND',
      'status', 404,
      'message', 'No actor-owned protected preflight exists for this request ID'
    );
  end if;

  if v_preflight.consumed_at is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_ATTEMPT_ALREADY_CONSUMED',
      'status', 409,
      'message', 'Admission already consumed this preflight; gates are read-only history now'
    );
  end if;

  if pg_catalog.clock_timestamp() > v_preflight.expires_at then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_EXPIRED',
      'status', 409,
      'message', 'The 180-second server preflight window expired before all gates completed'
    );
  end if;

  if util.dataset_alias_execution_sha256(p_preflight_token)
      is distinct from v_preflight.token_sha256 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_TOKEN_MISMATCH',
      'status', 403,
      'message', 'Preflight token does not match the durable server record'
    );
  end if;

  if exists (
    select 1
    from util.dataset_alias_execution_gate_receipts as receipt
    where receipt.preflight_id = p_request_id
      and receipt.gate_name = p_gate_name
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_ALREADY_CAPTURED',
      'status', 409,
      'message', 'Each live gate is captured at most once; freeze again after a lost gate response'
    );
  end if;

  select count(*)::integer
  into v_existing_gate_count
  from util.dataset_alias_execution_gate_receipts as receipt
  where receipt.preflight_id = p_request_id
    and receipt.actor_user_id = v_actor;

  if (
      p_gate_name = 'primary_support_plan'
      and v_existing_gate_count <> 0
    ) or (
      p_gate_name = 'execution_unused'
      and (
        v_existing_gate_count <> 1
        or not exists (
          select 1
          from util.dataset_alias_execution_gate_receipts as receipt
          where receipt.preflight_id = p_request_id
            and receipt.actor_user_id = v_actor
            and receipt.gate_name = 'primary_support_plan'
        )
      )
    ) or (
      p_gate_name = 'derivative_quiescence'
      and (
        v_existing_gate_count <> 2
        or not exists (
          select 1
          from util.dataset_alias_execution_gate_receipts as receipt
          where receipt.preflight_id = p_request_id
            and receipt.actor_user_id = v_actor
            and receipt.gate_name = 'primary_support_plan'
        )
        or not exists (
          select 1
          from util.dataset_alias_execution_gate_receipts as receipt
          where receipt.preflight_id = p_request_id
            and receipt.actor_user_id = v_actor
            and receipt.gate_name = 'execution_unused'
        )
      )
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_ORDER_MISMATCH',
      'status', 409,
      'message', 'Live gates must be captured exactly once in primary/support, execution-unused, derivative-quiescence order'
    );
  end if;

  v_expected_name := case p_gate_name
    when 'primary_support_plan' then 'primary_support_plan_sha256'
    when 'execution_unused' then 'execution_unused_sha256'
    when 'derivative_quiescence' then 'derivative_quiescence_sha256'
  end;
  v_expected_sha256 := v_preflight.gate_expectations->>v_expected_name;

  if p_gate_name = 'primary_support_plan' then
    begin
      v_alias_result := public.cmd_dataset_alias_plan_guarded(v_preflight.plan);
      if coalesce((v_alias_result->>'ok')::boolean, false) is not true
        or coalesce((v_alias_result->>'idempotent_replay')::boolean, true)
        or v_alias_result->>'row_count' is distinct from '52'
        or v_alias_result->>'exchange_count' is distinct from '59' then
        raise exception using
          errcode = 'P0001',
          message = 'Primary/support simulation rejected';
      end if;

      v_batch_result := util.admit_dataset_derivative_rebuild_batch(
        v_actor,
        p_request_id,
        v_preflight.plan_sha256,
        v_preflight.operation_id,
        'PROTECTED_ALIAS_DERIVATIVE_CLOSURE',
        v_preflight.derivative_targets
      );

      if coalesce((v_batch_result->>'ok')::boolean, false) is not true
        or v_batch_result->>'target_count' is distinct from '50'
        or coalesce(v_batch_result->>'flow_count', v_batch_result->>'flows')
          is distinct from '23'
        or coalesce(v_batch_result->>'process_count', v_batch_result->>'processes')
          is distinct from '27' then
        raise exception using
          errcode = 'P0001',
          message = 'Derivative batch simulation rejected';
      end if;

      raise exception using
        errcode = 'P0002',
        message = 'Protected primary/support gate rollback';
    exception
      when sqlstate 'P0002' then
        v_simulation_passed := true;
      when others then
        v_simulation_passed := false;
    end;

    if not v_simulation_passed then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_PRIMARY_SUPPORT_GATE_FAILED',
        'status', 409,
        'message', 'Primary/support plan drifted after preflight'
      );
    end if;

    v_material := jsonb_build_object(
      'schema_version', 'dataset-alias-execution-gate-material.v1',
      'gate', p_gate_name,
      'request_id', p_request_id,
      'actor_user_id', v_actor,
      'plan_request_sha256', v_preflight.plan_request_sha256,
      'derivative_targets_sha256', v_preflight.derivative_targets_sha256,
      'plan_rows', 52,
      'plan_exchanges', 59,
      'alias_audits', 55,
      'derivative_targets', 50,
      'rollback_simulation_passed', true
    );
  elsif p_gate_name = 'execution_unused' then
    select count(*)::integer
    into v_execution_count
    from util.dataset_alias_execution_requests as request
    where request.actor_user_id = v_actor
      and request.approval_identity_sha256 =
        v_preflight.bindings->>'approval_identity_sha256';

    select count(*)::integer
    into v_alias_audit_count
    from public.command_audit_log as audit
    where audit.actor_user_id = v_actor
      and (
        (
          audit.command = 'cmd_dataset_alias_batch_guarded'
          and audit.payload->>'plan_sha256' = v_preflight.plan_sha256
          and audit.payload->>'operation_id' = v_preflight.operation_id
        )
        or (
          audit.command = 'cmd_dataset_alias_plan_guarded'
          and audit.payload->>'plan_request_sha256' =
            v_preflight.plan_request_sha256
        )
      );

    select count(*)::integer
    into v_derivative_child_count
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = v_actor
      and request.batch_id = p_request_id;

    if v_execution_count <> 0
      or v_alias_audit_count <> 0
      or v_derivative_child_count <> 0 then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_UNUSED_GATE_FAILED',
        'status', 409,
        'message', 'The sealed execution identity already has durable effects'
      );
    end if;

    v_material := jsonb_build_object(
      'schema_version', 'dataset-alias-execution-gate-material.v1',
      'gate', p_gate_name,
      'request_id', p_request_id,
      'actor_user_id', v_actor,
      'plan_request_sha256', v_preflight.plan_request_sha256,
      'sealed_execution_rows', v_execution_count,
      'alias_audit_rows', v_alias_audit_count,
      'derivative_child_rows', v_derivative_child_count
    );
  else
    for v_target in
      select target_item.value
      from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
    loop
      begin
        v_snapshot := util.dataset_derivative_rebuild_snapshot(
          v_target->>'table',
          (v_target->>'id')::uuid,
          v_target->>'version'
        );
      exception
        when others then
          v_snapshot := null;
      end;

      if v_snapshot is null
        or v_snapshot->>'user_id' is distinct from v_actor::text
        or v_snapshot->>'state_code' is distinct from '0'
        or v_snapshot->>'snapshot_sha256'
          is distinct from v_target->>'baseline_snapshot_sha256' then
        v_snapshot_drift_count := v_snapshot_drift_count + 1;
      end if;
    end loop;

    select count(*)::integer
    into v_active_rebuild_count
    from util.dataset_derivative_rebuild_requests as request
    where request.status not in ('completed', 'stale', 'failed')
      and exists (
        select 1
        from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
        where request.target_table = target_item.value->>'table'
          and request.target_id = (target_item.value->>'id')::uuid
          and request.target_version = target_item.value->>'version'
      );

    select count(*)::integer
    into v_http_count
    from net.http_request_queue as request
    where exists (
      select 1
      from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
      where util.dataset_derivative_rebuild_http_body_matches(
        request.body,
        target_item.value->>'table',
        (target_item.value->>'id')::uuid,
        target_item.value->>'version'
      )
    );

    select count(*)::integer
    into v_extraction_count
    from pgmq.q_dataset_extraction_jobs as job
    where exists (
      select 1
      from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
      where job.message->>'schema' = 'public'
        and job.message->>'table' = target_item.value->>'table'
        and job.message->>'id' = target_item.value->>'id'
        and btrim(job.message->>'version') = target_item.value->>'version'
    );

    select count(*)::integer
    into v_embedding_count
    from pgmq.q_embedding_jobs as job
    where exists (
      select 1
      from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
      where job.message->>'schema' = 'public'
        and job.message->>'table' = target_item.value->>'table'
        and job.message->>'id' = target_item.value->>'id'
        and btrim(job.message->>'version') = target_item.value->>'version'
        and job.message->>'embeddingColumn' = 'embedding_ft'
    );

    select count(*)::integer
    into v_pending_count
    from util.pending_embedding_jobs as pending
    where pending.schema_name = 'public'
      and pending.embedding_column = 'embedding_ft'
      and pending.status = 'pending'
      and exists (
        select 1
        from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
        where pending.table_name = target_item.value->>'table'
          and pending.record_id = target_item.value->>'id'
          and btrim(pending.record_version) = target_item.value->>'version'
      );

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'id', failure.id,
          'queue_name', failure.queue_name,
          'msg_id', failure.msg_id,
          'read_count', failure.read_count,
          'reason', failure.reason,
          'message', failure.message,
          'failed_at', failure.failed_at
        ) order by failure.id
      ),
      '[]'::jsonb
    )
    into v_failure_material
    from util.embedding_job_failures as failure
    where exists (
      select 1
      from jsonb_array_elements(v_preflight.derivative_targets) as target_item(value)
      where failure.message->>'table' = target_item.value->>'table'
        and failure.message->>'id' = target_item.value->>'id'
        and btrim(failure.message->>'version') = target_item.value->>'version'
    );
    v_failure_sha256 :=
      util.dataset_alias_execution_sha256(v_failure_material::text);

    if v_snapshot_drift_count <> 0
      or v_active_rebuild_count <> 0
      or v_http_count <> 0
      or v_extraction_count <> 0
      or v_embedding_count <> 0
      or v_pending_count <> 0
      or v_failure_sha256 is distinct from v_preflight.failure_baseline_sha256 then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_DERIVATIVE_QUIESCENCE_GATE_FAILED',
        'status', 409,
        'message', 'Derivative baselines, queues, fences, or failure ledger drifted after preflight'
      );
    end if;

    v_material := jsonb_build_object(
      'schema_version', 'dataset-alias-execution-gate-material.v1',
      'gate', p_gate_name,
      'request_id', p_request_id,
      'actor_user_id', v_actor,
      'derivative_targets_sha256', v_preflight.derivative_targets_sha256,
      'snapshot_drift_count', v_snapshot_drift_count,
      'active_rebuild_count', v_active_rebuild_count,
      'http_request_count', v_http_count,
      'extraction_job_count', v_extraction_count,
      'embedding_job_count', v_embedding_count,
      'pending_embedding_count', v_pending_count,
      'failure_baseline_sha256', v_failure_sha256
    );
  end if;

  v_captured_at := pg_catalog.clock_timestamp();
  if v_captured_at > v_preflight.expires_at then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_WINDOW_EXPIRED',
      'status', 409,
      'message', 'The live gate completed after the 180-second server window'
    );
  end if;

  v_observed_sha256 := util.dataset_alias_execution_sha256(v_material::text);
  if v_observed_sha256 is distinct from v_expected_sha256 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_EVIDENCE_MISMATCH',
      'status', 409,
      'message', 'The live gate evidence does not match the server-owned preflight expectation'
    );
  end if;

  v_receipt_material := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-gate-receipt.v1',
    'request_id', p_request_id,
    'actor_user_id', v_actor,
    'preflight_proof_sha256', v_preflight.preflight_proof_sha256,
    'gate', p_gate_name,
    'expected_sha256', v_expected_sha256,
    'observed_sha256', v_observed_sha256,
    'status', 'passed',
    'captured_at', v_captured_at
  );
  v_receipt_sha256 :=
    util.dataset_alias_execution_sha256(v_receipt_material::text);

  insert into util.dataset_alias_execution_gate_receipts (
    preflight_id,
    actor_user_id,
    gate_name,
    expected_sha256,
    observed_sha256,
    material,
    status,
    captured_at,
    receipt_sha256
  ) values (
    p_request_id,
    v_actor,
    p_gate_name,
    v_expected_sha256,
    v_observed_sha256,
    v_material,
    'passed',
    v_captured_at,
    v_receipt_sha256
  );

  return v_receipt_material || jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_alias_execution_gate_guarded',
    'receipt_sha256', v_receipt_sha256
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_LOCK_BUSY',
      'status', 409,
      'message', 'Protected live gate could not acquire its bounded locks'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_ALREADY_CAPTURED',
      'status', 409,
      'message', 'A concurrent call already captured this live gate'
    );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_alias_execution_gate_guarded"("p_request_id" "uuid", "p_preflight_token" "text", "p_gate_name" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_alias_execution_gate_guarded"("p_request_id" "uuid", "p_preflight_token" "text", "p_gate_name" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_alias_execution_gate_guarded"("p_request_id" "uuid", "p_preflight_token" "text", "p_gate_name" "text") TO "authenticated";
