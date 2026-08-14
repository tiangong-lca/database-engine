CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_process_rewrite_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_process_rewrite_guarded';
  v_schema_version constant text :=
    'dataset-flow-identity-process-rewrite.v1';
  v_result_schema_version constant text :=
    'dataset-flow-identity-process-rewrite-result.v1';
  v_top_keys constant text[] := array[
    'schema_version', 'request_id', 'scope_proof_sha256', 'ordinal',
    'process', 'rewrites', 'collision_ledger', 'collision_ledger_sha256',
    'process_request_sha256'
  ];
  v_rewrite_keys constant text[] := array[
    'ordinal', 'exchange_index', 'internal_id', 'direction', 'mapping_id',
    'source_reference', 'target_reference', 'before_reference_sha256',
    'after_reference_sha256'
  ];
  v_reference_keys constant text[] := array[
    '@refObjectId', '@type', '@uri', '@version',
    'common:shortDescription'
  ];
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_ledger util.dataset_flow_identity_process_ledger%rowtype;
  v_process public.processes%rowtype;
  v_committed public.processes%rowtype;
  v_mapping util.dataset_flow_identity_mappings%rowtype;
  v_rewrite jsonb;
  v_mapping_validation jsonb;
  v_process_validation jsonb;
  v_before_payload jsonb;
  v_desired_payload jsonb;
  v_before_exchanges jsonb;
  v_after_exchanges jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_before_reference jsonb;
  v_after_reference jsonb;
  v_collision_ledger jsonb;
  v_internal_request_sha256 text;
  v_request_sha256 text;
  v_before_payload_sha256 text;
  v_after_payload_sha256 text;
  v_after_exchange_sha256 text;
  v_baseline_snapshot jsonb;
  v_after_snapshot jsonb;
  v_derivative_targets jsonb;
  v_derivative_result jsonb;
  v_derivative_batch_id uuid;
  v_derivative_reason_code text;
  v_audit_id bigint;
  v_next_ordinal integer;
  v_remaining integer;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;

  if p_scope_id is null
    or p_request is null
    or pg_column_size(p_request) > 8388608
    or not private.dataset_flow_identity_exact_keys(p_request, v_top_keys)
    or p_request->>'schema_version' <> v_schema_version
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or p_request->>'process_request_sha256' !~ '^[a-f0-9]{64}$'
    or p_request->>'collision_ledger_sha256' !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_request->'ordinal') <> 'number'
    or (p_request->>'ordinal')::integer <= 0
    or jsonb_typeof(p_request->'rewrites') <> 'array'
    or jsonb_array_length(p_request->'rewrites') <= 0
    or jsonb_array_length(p_request->'rewrites') > 10000 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_INVALID_REQUEST',
      'status', 400, 'message', 'Step 3 process request schema mismatch'
    );
  end if;

  v_internal_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    p_request - 'process_request_sha256'
  );
  if v_internal_request_sha256 is distinct from
    p_request->>'process_request_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'collision_ledger'
    )
      is distinct from p_request->>'collision_ledger_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REQUEST_HASH_MISMATCH',
      'status', 409, 'message', 'Process or collision request hash mismatch'
    );
  end if;
  v_request_sha256 := coalesce(nullif(current_setting(
    'app.dataset_flow_identity_v2_process_request_sha256', true
  ), ''), v_internal_request_sha256);
  if v_request_sha256 !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REQUEST_HASH_MISMATCH',
      'status', 409, 'message', 'External v2 request hash is invalid'
    );
  end if;

  if not pg_try_advisory_xact_lock(
    hashtextextended('dataset-flow-identity:' || p_scope_id::text, 0)
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_SCOPE_BUSY', 'status', 409,
      'message', 'Another process transaction currently owns this scope'
    );
  end if;

  select scope.*
  into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id
    and scope.actor_user_id = v_actor
  for update;

  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  if v_scope.scope_proof_sha256 is distinct from
      p_request->>'scope_proof_sha256'
    or v_scope.status in ('failed', 'cancelled') then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Scope proof is invalid or scope is failed'
    );
  end if;

  select ledger.*
  into v_ledger
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = p_scope_id
    and ledger.ordinal = (p_request->>'ordinal')::integer
  for update;

  if v_ledger.scope_id is null
    or p_request->'process' is distinct from v_ledger.manifest
    or p_request->'rewrites' is distinct from v_ledger.manifest->'rewrites'
    or p_request->'collision_ledger'
      is distinct from v_ledger.manifest->'collision_ledger'
    or p_request #>> '{process,process_template_sha256}'
      is distinct from v_ledger.process_template_sha256 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_MISMATCH', 'status', 409,
      'message', 'Request does not match the sealed process manifest'
    );
  end if;

  if v_ledger.status = 'completed' then
    select process.*
    into v_committed
    from public.processes as process
    where process.id = v_ledger.process_id
      and btrim(process.version::text) = v_ledger.process_version
      and process.user_id = v_actor
      and process.state_code = 0;
    if v_ledger.process_request_sha256 is distinct from v_request_sha256
      or v_committed.id is null
      or util.dataset_flow_identity_sha256(v_committed.json_ordered::jsonb)
        is distinct from v_ledger.after_payload_sha256
      or util.dataset_flow_identity_sha256(
        private.dataset_flow_identity_exchanges(v_committed.json_ordered::jsonb)
      ) is distinct from v_ledger.after_exchange_set_sha256
      or not exists (
        select 1 from private.command_audit_log as audit
        where audit.id = v_ledger.audit_id
          and audit.actor_user_id = v_actor
          and audit.command = v_command
          and audit.payload->>'process_request_sha256' = v_request_sha256
          and audit.payload->>'derivative_batch_id'
            = v_ledger.derivative_batch_id::text
          and audit.payload->>'derivative_reason_code'
            = 'FLOW_IDENTITY_SCOPE:' || p_scope_id::text || ':'
              || v_ledger.ordinal::text
      )
      or not exists (
        select 1
        from util.dataset_derivative_rebuild_requests as child
        where child.actor_user_id = v_actor
          and child.batch_id = v_ledger.derivative_batch_id
          and child.target_table = 'processes'
          and child.target_id = v_ledger.process_id
          and child.target_version = v_ledger.process_version
          and child.reason_code = 'FLOW_IDENTITY_SCOPE:'
            || p_scope_id::text || ':' || v_ledger.ordinal::text
          and child.expected_json_ordered_sha256
            = util.dataset_derivative_rebuild_sha256(
              v_committed.json_ordered::jsonb::text
            )
      ) then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_REPLAY_PROOF_MISMATCH', 'status', 409,
        'message', 'Completed process no longer has exact audit/live proof'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', v_result_schema_version,
      'scope_id', p_scope_id, 'ordinal', v_ledger.ordinal,
      'process_id', v_ledger.process_id,
      'process_version', v_ledger.process_version,
      'process_request_sha256', v_request_sha256,
      'before_payload_sha256', v_ledger.before_payload_sha256,
      'after_payload_sha256', v_ledger.after_payload_sha256,
      'rewrite_count', v_ledger.rewrite_count,
      'audit_id', v_ledger.audit_id::text,
      'derivative_batch_id', v_ledger.derivative_batch_id,
      'status', 'completed', 'replay', true
    );
  elsif v_ledger.status <> 'pending' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_NOT_EXECUTABLE', 'status', 409,
      'message', 'Process ledger is not pending or exactly replayable'
    );
  end if;

  select min(ledger.ordinal)
  into v_next_ordinal
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = p_scope_id and ledger.status = 'pending';
  if v_next_ordinal is distinct from v_ledger.ordinal then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_ORDINAL_MISMATCH', 'status', 409,
      'message', 'Processes must execute in sealed ordinal order',
      'next_ordinal', v_next_ordinal
    );
  end if;

  if jsonb_array_length(p_request->'rewrites') <> v_ledger.rewrite_count
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'rewrites'
    )
      is distinct from v_ledger.manifest->>'rewrite_set_sha256'
    or p_request->>'collision_ledger_sha256'
      is distinct from v_ledger.manifest->>'collision_ledger_sha256'
    or (
      select min((rewrite.value->>'ordinal')::integer) = 1
        and max((rewrite.value->>'ordinal')::integer) = v_ledger.rewrite_count
        and count(distinct (rewrite.value->>'ordinal')::integer)
          = v_ledger.rewrite_count
        and count(distinct (rewrite.value->>'exchange_index')::integer)
          = v_ledger.rewrite_count
      from jsonb_array_elements(p_request->'rewrites') as rewrite(value)
    ) is not true then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REWRITE_SET_MISMATCH', 'status', 409,
      'message', 'Rewrite set does not match the sealed manifest'
    );
  end if;

  v_process_validation := util.dataset_flow_identity_validate_process_guard(
    v_actor, v_ledger.manifest
  );
  if coalesce((v_process_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_BASELINE_DRIFT', 'status', 409,
      'message', 'Owner-draft process drifted after scope seal',
      'details', v_process_validation
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = v_ledger.process_id
    and btrim(process.version::text) = v_ledger.process_version
    and process.user_id = v_actor
    and process.state_code = 0
  for update;

  v_before_payload := v_process.json_ordered::jsonb;
  v_before_exchanges := private.dataset_flow_identity_exchanges(v_before_payload);
  v_after_exchanges := v_before_exchanges;
  v_before_payload_sha256 := util.dataset_flow_identity_sha256(v_before_payload);
  begin
    v_baseline_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception when others then
    v_baseline_snapshot := null;
  end;
  if v_before_payload_sha256 is distinct from
      v_ledger.manifest->>'before_payload_sha256'
    or v_baseline_snapshot is null
    or v_baseline_snapshot->>'snapshot_sha256' is distinct from
      v_ledger.manifest->>'derivative_baseline_snapshot_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_LOCKED_BASELINE_DRIFT', 'status', 409,
      'message', 'Locked process no longer matches its baseline'
    );
  end if;

  for v_rewrite in
    select rewrite.value
    from jsonb_array_elements(p_request->'rewrites')
      with ordinality as rewrite(value, ordinality)
    order by rewrite.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(
        v_rewrite, v_rewrite_keys
      )
      or jsonb_typeof(v_rewrite->'ordinal') <> 'number'
      or jsonb_typeof(v_rewrite->'exchange_index') <> 'number'
      or (v_rewrite->>'exchange_index')::integer < 0
      or (v_rewrite->>'exchange_index')::integer
        >= jsonb_array_length(v_before_exchanges)
      or nullif(v_rewrite->>'internal_id', '') is null
      or v_rewrite->>'direction' not in ('Input', 'Output')
      or v_rewrite->>'mapping_id' !~ '^[a-f0-9]{64}$'
      or v_rewrite->>'before_reference_sha256' !~ '^[a-f0-9]{64}$'
      or v_rewrite->>'after_reference_sha256' !~ '^[a-f0-9]{64}$'
      or not private.dataset_flow_identity_exact_keys(
        v_rewrite->'source_reference', v_reference_keys
      )
      or not private.dataset_flow_identity_exact_keys(
        v_rewrite->'target_reference', v_reference_keys
      ) then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_REWRITE_SCHEMA_MISMATCH', 'status', 400,
        'message', 'A rewrite locator or reference is invalid'
      );
    end if;

    select mapping.*
    into v_mapping
    from util.dataset_flow_identity_mappings as mapping
    where mapping.scope_id = p_scope_id
      and mapping.mapping_id = v_rewrite->>'mapping_id';
    if v_mapping.scope_id is null then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_MAPPING_NOT_SEALED', 'status', 409,
        'message', 'Rewrite mapping is outside the sealed scope'
      );
    end if;

    v_mapping_validation := util.dataset_flow_identity_validate_mapping(
      v_actor, v_mapping.mapping, v_scope.compatibility_policy,
      v_scope.support_snapshots,
      v_mapping.ordinal
    );
    if coalesce((v_mapping_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_MAPPING_DRIFT', 'status', 409,
        'message', 'A source/public/support mapping guard drifted',
        'details', v_mapping_validation
      );
    end if;

    v_before_exchange := v_after_exchanges ->
      (v_rewrite->>'exchange_index')::integer;
    v_before_reference := private.dataset_flow_identity_reference(
      v_before_exchange
    );
    if v_before_exchange->>'@dataSetInternalID'
        is distinct from v_rewrite->>'internal_id'
      or v_before_exchange->>'exchangeDirection'
        is distinct from v_rewrite->>'direction'
      or v_before_reference is distinct from v_rewrite->'source_reference'
      or util.dataset_flow_identity_sha256(v_before_reference)
        is distinct from v_rewrite->>'before_reference_sha256'
      or v_before_reference->>'@refObjectId'
        is distinct from v_mapping.source_id::text
      or v_before_reference->>'@version'
        is distinct from v_mapping.source_version
      or v_rewrite->'target_reference'
        is distinct from v_mapping.mapping #> '{target,reference}'
      or util.dataset_flow_identity_sha256(v_rewrite->'target_reference')
        is distinct from v_rewrite->>'after_reference_sha256' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_EXCHANGE_LOCATOR_DRIFT', 'status', 409,
        'message', 'An exchange locator or exact reference drifted'
      );
    end if;

    v_after_exchange := jsonb_set(
      v_before_exchange,
      '{referenceToFlowDataSet}',
      (v_before_exchange->'referenceToFlowDataSet')
        || v_rewrite->'target_reference',
      false
    );
    v_after_reference := private.dataset_flow_identity_reference(
      v_after_exchange
    );
    if v_after_reference is distinct from v_rewrite->'target_reference'
      or v_after_exchange - 'referenceToFlowDataSet'
        is distinct from v_before_exchange - 'referenceToFlowDataSet'
      or (v_after_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription'
        is distinct from
        (v_before_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_FIVE_FIELD_BOUNDARY_FAILED',
        'status', 409,
        'message', 'Rewrite would change a non-approved exchange field'
      );
    end if;

    v_after_exchanges := jsonb_set(
      v_after_exchanges,
      array[(v_rewrite->>'exchange_index')::integer::text],
      v_after_exchange,
      false
    );
  end loop;

  v_collision_ledger := private.dataset_flow_identity_collision_ledger(
    v_after_exchanges, p_request->'rewrites'
  );
  if v_collision_ledger is distinct from p_request->'collision_ledger' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_COLLISION_LEDGER_MISMATCH', 'status', 409,
      'message', 'Post-rewrite collision ledger is not exact'
    );
  end if;

  v_desired_payload := private.dataset_flow_identity_replace_exchanges(
    v_before_payload, v_after_exchanges
  );
  v_after_payload_sha256 := util.dataset_flow_identity_sha256(v_desired_payload);
  v_after_exchange_sha256 := util.dataset_flow_identity_sha256(v_after_exchanges);
  if v_desired_payload is null
    or jsonb_array_length(v_before_exchanges)
      <> jsonb_array_length(v_after_exchanges)
    or v_after_payload_sha256 is distinct from
      v_ledger.manifest->>'desired_payload_sha256'
    or v_after_exchange_sha256 is distinct from
      v_ledger.manifest->>'desired_exchange_set_sha256'
    or exists (
      select 1
      from util.dataset_flow_identity_mappings as mapping
      join lateral jsonb_array_elements(v_after_exchanges) as exchange(value)
        on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
          = mapping.source_id::text
        and exchange.value #>> '{referenceToFlowDataSet,@version}'
          = mapping.source_version
      where mapping.scope_id = p_scope_id
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_DESIRED_CLOSURE_MISMATCH', 'status', 409,
      'message', 'Server-reconstructed desired process does not match the seal'
    );
  end if;

  insert into util.dataset_flow_identity_mutation_permits (
    transaction_id, scope_id, ordinal, process_id, process_version,
    mutation_nonce, before_payload_sha256, after_payload_sha256
  ) values (
    txid_current(), p_scope_id, v_ledger.ordinal, v_process.id,
    btrim(v_process.version::text), v_ledger.mutation_nonce,
    v_before_payload_sha256, v_after_payload_sha256
  );

  update public.processes
  set json_ordered = v_desired_payload::json
  where id = v_process.id
    and btrim(version::text) = btrim(v_process.version::text)
    and user_id = v_actor
    and state_code = 0
    and modified_at = v_process.modified_at
  returning * into v_committed;

  if v_committed.id is null then
    raise exception using
      errcode = '40001',
      message = 'Step 3 locked process update precondition was lost';
  end if;
  begin
    v_after_snapshot := util.dataset_derivative_rebuild_snapshot(v_committed);
  exception when others then
    v_after_snapshot := null;
  end;
  if v_after_snapshot is null
    or v_after_snapshot->>'json_sha256'
      is distinct from v_after_snapshot->>'json_ordered_sha256'
    or util.dataset_flow_identity_sha256(v_committed.json_ordered::jsonb)
      is distinct from v_after_payload_sha256 then
    raise exception using
      errcode = 'P0001',
      message = 'Step 3 committed process primary hash mismatch';
  end if;

  -- Generate and bind the protected derivative batch before the primary audit.
  -- The audit, request ledger, and admission therefore share one exact causal
  -- identifier even though the admission itself happens after the audit row.
  v_derivative_batch_id := gen_random_uuid();
  v_derivative_reason_code := 'FLOW_IDENTITY_SCOPE:'
    || p_scope_id::text || ':' || v_ledger.ordinal::text;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, 'processes', v_process.id,
    btrim(v_process.version::text),
    jsonb_build_object(
      'record_type', 'process_rewrite',
      'schema_version', v_schema_version,
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'ordinal', v_ledger.ordinal,
      'process_request_sha256', v_request_sha256,
      'process_template_sha256', v_ledger.process_template_sha256,
      'rewrite_set_sha256', v_ledger.manifest->>'rewrite_set_sha256',
      'collision_ledger_sha256',
        v_ledger.manifest->>'collision_ledger_sha256',
      'before_payload_sha256', v_before_payload_sha256,
      'after_payload_sha256', v_after_payload_sha256,
      'after_exchange_set_sha256', v_after_exchange_sha256,
      'rewrite_count', v_ledger.rewrite_count,
      'derivative_batch_id', v_derivative_batch_id,
      'derivative_reason_code', v_derivative_reason_code,
      'committed_modified_at', v_committed.modified_at,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_audit_id;

  v_derivative_targets := jsonb_build_array(jsonb_build_object(
    'table', 'processes',
    'id', v_committed.id,
    'version', btrim(v_committed.version::text),
    'expected_json_ordered_sha256',
      v_after_snapshot->>'json_ordered_sha256',
    'baseline_snapshot_sha256',
      v_ledger.manifest->>'derivative_baseline_snapshot_sha256'
  ));
  v_derivative_result := util.admit_dataset_derivative_rebuild_batch(
    v_actor,
    v_derivative_batch_id,
    v_scope.plan_sha256,
    v_scope.operation_id,
    v_derivative_reason_code,
    v_derivative_targets
  );
  if coalesce((v_derivative_result->>'ok')::boolean, false) is false
    or v_derivative_result->>'target_count' is distinct from '1'
    or v_derivative_result->>'process_count' is distinct from '1' then
    raise exception using
      errcode = 'P0001',
      message = 'Step 3 derivative admission mismatch';
  end if;

  update util.dataset_flow_identity_process_ledger
  set
    status = 'completed',
    process_request_sha256 = v_request_sha256,
    audit_id = v_audit_id,
    after_payload_sha256 = v_after_payload_sha256,
    after_exchange_set_sha256 = v_after_exchange_sha256,
    derivative_batch_id = v_derivative_batch_id,
    derivative_admission = v_derivative_result,
    completed_at = clock_timestamp()
  where scope_id = p_scope_id and ordinal = v_ledger.ordinal;

  -- Ordinals execute strictly in order, so after committing ordinal N the
  -- remaining primary count is exact without rescanning the whole ledger.
  v_remaining := v_scope.process_count - v_ledger.ordinal;
  update util.dataset_flow_identity_scopes
  set
    status = case when v_remaining = 0
      then 'derivatives_pending' else 'running' end,
    primary_completed_at = case when v_remaining = 0
      then clock_timestamp() else primary_completed_at end,
    updated_at = clock_timestamp()
  where id = p_scope_id;

  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', v_result_schema_version,
    'scope_id', p_scope_id, 'ordinal', v_ledger.ordinal,
    'process_id', v_process.id,
    'process_version', btrim(v_process.version::text),
    'process_request_sha256', v_request_sha256,
    'before_payload_sha256', v_before_payload_sha256,
    'after_payload_sha256', v_after_payload_sha256,
    'rewrite_count', v_ledger.rewrite_count,
    'audit_id', v_audit_id::text,
    'derivative_batch_id', v_derivative_batch_id,
    'status', 'completed', 'replay', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_LOCK_BUSY', 'status', 409,
      'message', 'Process transaction could not acquire its bounded lock'
    );
  when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_TRANSACTION_FAILED', 'status', 409,
      'message', sqlerrm, 'sqlstate', sqlstate,
      'primary_rolled_back', true, 'automatic_retry', false
    );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_process_rewrite_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_process_rewrite_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_process_rewrite_core_v1"("p_scope_id" "uuid", "p_request" "jsonb") TO "api_internal_executor";
