CREATE OR REPLACE FUNCTION "public"."cmd_dataset_flow_identity_capture_attest_guarded"("p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '180s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_capture_attest_guarded';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_request_sha256 text;
  v_context jsonb;
  v_existing util.dataset_flow_identity_capture_receipts%rowtype;
  v_receipt_id uuid := gen_random_uuid();
  v_captured_at timestamp with time zone := clock_timestamp();
  v_expires_at timestamp with time zone;
  v_mapping_intent jsonb;
  v_mapping jsonb;
  v_mapping_body jsonb;
  v_mappings jsonb := '[]'::jsonb;
  v_mapping_index jsonb;
  v_source_guard jsonb;
  v_target_guard jsonb;
  v_source_records jsonb := '[]'::jsonb;
  v_target_records jsonb := '[]'::jsonb;
  v_source_ordinal integer := 0;
  v_partition text;
  v_protected_entry jsonb;
  v_protected jsonb;
  v_supports jsonb;
  v_process_records jsonb := '[]'::jsonb;
  v_processes jsonb := '[]'::jsonb;
  v_source_universe jsonb;
  v_source_guard_set_sha256 text;
  v_target_guard_set_sha256 text;
  v_mapping_guard_set_sha256 text;
  v_process_intent_set_sha256 text;
  v_support_set_sha256 text;
  v_protected_sha256 text;
  v_source_universe_sha256 text;
  v_mapping_set_sha256 text;
  v_process_manifest_sha256 text;
  v_receipt_proof_sha256 text;
  v_whole_scope_proof_sha256 text;
  v_rewrite_count integer;
  v_validation jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if v_request is null
    or pg_column_size(v_request) > 134217728
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'environment', 'project_ref', 'actor',
      'target_visibility', 'operation_id', 'compatibility_policy',
      'artifact_evidence', 'mappings', 'process_intents', 'protected_closure'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-capture-attest.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}'))
      is distinct from v_actor_email
    or not private.dataset_flow_identity_exact_keys(
      v_request->'artifact_evidence', array[
        'review_ledger_sha256', 'live_capture_artifact_sha256',
        'toolchain_evidence_sha256'
      ]
    )
    or exists (
      select 1 from unnest(array[
        'review_ledger_sha256', 'live_capture_artifact_sha256',
        'toolchain_evidence_sha256'
      ]) as field(name)
      where v_request->'artifact_evidence'->>field.name
        !~ '^[a-f0-9]{64}$'
    )
    or jsonb_typeof(v_request->'mappings') <> 'array'
    or jsonb_array_length(v_request->'mappings') not between 1 and 305
    or jsonb_typeof(v_request->'process_intents') <> 'array'
    or jsonb_array_length(v_request->'process_intents') not between 1 and 12000
    or not private.dataset_flow_identity_exact_keys(
      v_request->'compatibility_policy', array[
        'schema_version', 'policy_sha256', 'evidence_resolution_sha256',
        'approved_at_utc', 'approval_text_sha256'
      ]
    )
    or v_request #>> '{compatibility_policy,schema_version}'
      <> 'dataset-flow-identity-compatibility-policy.v1'
    or exists (
      select 1 from unnest(array[
        'policy_sha256', 'evidence_resolution_sha256', 'approval_text_sha256'
      ]) as field(name)
      where v_request->'compatibility_policy'->>field.name
        !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CAPTURE_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 capture request schema or safe-JSON domain mismatch'
    );
  end if;
  begin
    perform (v_request #>>
      '{compatibility_policy,approved_at_utc}')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CAPTURE_POLICY_TIME_INVALID', 'status', 400,
      'message', 'Compatibility policy approval time is invalid'
    );
  end;
  v_context := util.dataset_alias_execution_server_context();
  if v_request->>'environment' is distinct from v_context->>'environment'
    or v_request->>'project_ref' is distinct from v_context->>'project_ref' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CAPTURE_ENVIRONMENT_MISMATCH', 'status', 409,
      'message', 'Capture does not target this database branch'
    );
  end if;
  -- Capture and every owner-row trigger share this actor fence.  No process
  -- or elementary-flow write can straddle the final receipt attestation.
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  v_request_sha256 := util.dataset_flow_identity_restricted_sha256_v2(v_request);
  select receipt.* into v_existing
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.actor_user_id = v_actor
    and receipt.request_id = (v_request->>'request_id')::uuid;
  if v_existing.id is not null then
    if v_existing.capture_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CAPTURE_REQUEST_REUSE_MISMATCH', 'status', 409,
        'message', 'Capture request ID is already bound differently'
      );
    elsif v_existing.expires_at <= clock_timestamp() then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CAPTURE_RECEIPT_EXPIRED', 'status', 409,
        'message', 'Capture receipt expired; create a fresh request ID'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', 'dataset-flow-identity-capture-attest-result.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'receipt_id', v_existing.id,
      'receipt_proof_sha256', v_existing.receipt_proof_sha256,
      'operation_id', v_existing.operation_id,
      'environment', v_existing.environment,
      'project_ref', v_existing.project_ref,
      'captured_at', v_existing.captured_at,
      'expires_at', v_existing.expires_at,
      'source_guard_set_sha256', v_existing.source_guard_set_sha256,
      'support_guard_set_sha256', v_existing.support_snapshot_set_sha256,
      'target_guard_set_sha256', v_existing.target_guard_set_sha256,
      'mapping_guard_set_sha256', v_existing.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_existing.process_intent_set_sha256,
      'protected_closure_sha256', v_existing.protected_closure_sha256,
      'whole_scope_proof_sha256', v_existing.whole_scope_proof_sha256,
      'policy_sha256', v_existing.compatibility_policy->>'policy_sha256',
      'policy_approval_text_sha256',
        v_existing.policy_approval_text_sha256,
      'source_count', v_existing.source_count,
      'target_count', v_existing.target_count,
      'support_count', v_existing.support_count,
      'mapping_count', v_existing.mapping_count,
      'process_count', v_existing.process_count,
      'rewrite_count', v_existing.rewrite_count,
      'capture_request_sha256', v_existing.capture_request_sha256,
      'replay', true
    );
  end if;

  v_protected := private.dataset_flow_identity_build_protected_v2(
    v_actor, v_request->'protected_closure'
  );
  v_protected_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_protected);

  for v_mapping_intent in
    select item.value
    from jsonb_array_elements(v_request->'mappings')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(v_mapping_intent, array[
        'ordinal', 'source', 'target', 'compatibility'
      ])
      or jsonb_typeof(v_mapping_intent->'ordinal') <> 'number'
      or (v_mapping_intent->>'ordinal')::integer
        <> jsonb_array_length(v_mappings) + 1
      or not private.dataset_flow_identity_exact_keys(
        v_mapping_intent->'compatibility', array[
          'policy_sha256', 'mode', 'confidence',
          'flow_property_compatible', 'unit_group_compatible',
          'direction_compatible', 'compartment_compatible',
          'conversion_factor', 'evidence_sha256', 'flow_schema',
          'process_schema_required'
        ]
      )
      or v_mapping_intent #>> '{compatibility,policy_sha256}'
        is distinct from v_request #>> '{compatibility_policy,policy_sha256}'
      or v_mapping_intent #>> '{compatibility,mode}' <> 'identity'
      or v_mapping_intent #>> '{compatibility,confidence}' <> 'approved'
      or v_mapping_intent #>> '{compatibility,conversion_factor}' <> '1'
      or v_mapping_intent #>> '{compatibility,evidence_sha256}'
        !~ '^[a-f0-9]{64}$'
      or v_mapping_intent #>> '{compatibility,process_schema_required}'
        <> 'pass'
      or not private.dataset_flow_identity_exact_keys(
        v_mapping_intent #> '{compatibility,flow_schema}',
        array['status', 'warning_set_sha256']
      )
      or v_mapping_intent #>> '{compatibility,flow_schema,status}'
        not in ('pass', 'legacy_warning')
      or v_mapping_intent #>>
        '{compatibility,flow_schema,warning_set_sha256}'
        !~ '^[a-f0-9]{64}$'
      or exists (
        select 1 from unnest(array[
          'flow_property_compatible', 'unit_group_compatible',
          'direction_compatible', 'compartment_compatible'
        ]) as field(name)
        where v_mapping_intent->'compatibility'->>field.name <> 'true'
      ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_MAPPING_INTENT_MISMATCH';
    end if;
    v_source_guard := private.dataset_flow_identity_build_flow_guard_v2(
      v_actor, v_mapping_intent->'source', false
    );
    v_target_guard := private.dataset_flow_identity_build_flow_guard_v2(
      v_actor, v_mapping_intent->'target', true
    );
    if v_source_guard->>'flow_property_id'
        is distinct from v_target_guard->>'flow_property_id'
      or v_source_guard->>'flow_property_version'
        is distinct from v_target_guard->>'flow_property_version'
      or v_source_guard->>'unit_group_id'
        is distinct from v_target_guard->>'unit_group_id'
      or v_source_guard->>'unit_group_version'
        is distinct from v_target_guard->>'unit_group_version' then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_MAPPING_NOT_IDENTITY_ONLY';
    end if;
    v_mapping_body := jsonb_build_object(
      'source', v_source_guard,
      'target', v_target_guard,
      'compatibility', v_mapping_intent->'compatibility'
    );
    v_mapping := jsonb_build_object(
      'ordinal', v_mapping_intent->'ordinal',
      'mapping_id',
        util.dataset_flow_identity_restricted_sha256_v2(v_mapping_body)
    ) || v_mapping_body;
    v_mappings := v_mappings || jsonb_build_array(v_mapping);
    v_source_ordinal := v_source_ordinal + 1;
    v_source_records := v_source_records || jsonb_build_array(
      jsonb_build_object(
        'ordinal', v_source_ordinal, 'disposition', 'mapped',
        'source_id', v_source_guard->>'id',
        'source_version', v_source_guard->>'version',
        'guard', v_source_guard,
        'evidence_sha256', v_source_guard->>'source_trace_sha256'
      )
    );
  end loop;

  foreach v_partition in array array['pending', 'blockers', 'orphans'] loop
    for v_protected_entry in
      select item.value
      from jsonb_array_elements(v_request->'protected_closure'->v_partition)
        with ordinality as item(value, ordinality)
      order by item.ordinality
    loop
      v_source_guard := private.dataset_flow_identity_build_flow_guard_v2(
        v_actor,
        jsonb_build_object(
          'id', v_protected_entry->>'source_id',
          'version', v_protected_entry->>'source_version',
          'source_trace_sha256', v_protected_entry->>'evidence_sha256'
        ),
        false
      );
      v_source_ordinal := v_source_ordinal + 1;
      v_source_records := v_source_records || jsonb_build_array(
        jsonb_build_object(
          'ordinal', v_source_ordinal,
          'disposition', case v_partition
            when 'blockers' then 'blocker'
            when 'orphans' then 'orphan'
            else 'pending' end,
          'source_id', v_source_guard->>'id',
          'source_version', v_source_guard->>'version',
          'guard', v_source_guard,
          'evidence_sha256', v_protected_entry->>'evidence_sha256'
        )
      );
    end loop;
  end loop;
  if jsonb_array_length(v_source_records) <> 305
    or (select count(distinct (item.value->>'source_id') || '@'
          || (item.value->>'source_version'))
        from jsonb_array_elements(v_source_records) as item(value)) <> 305 then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_SOURCE_CLOSURE_MISMATCH';
  end if;

  select coalesce(jsonb_agg(target order by target_id, target_version), '[]'::jsonb)
  into v_target_records
  from (
    select distinct on (
      item.value #>> '{target,id}', item.value #>> '{target,version}'
    )
      item.value #>> '{target,id}' as target_id,
      item.value #>> '{target,version}' as target_version,
      item.value->'target' as target
    from jsonb_array_elements(v_mappings) as item(value)
    order by item.value #>> '{target,id}', item.value #>> '{target,version}'
  ) as distinct_target;
  v_target_records := coalesce((
    select jsonb_agg(jsonb_build_object(
      'ordinal', item.ordinality,
      'target_id', item.value->>'id',
      'target_version', item.value->>'version',
      'guard', item.value
    ) order by item.ordinality)
    from jsonb_array_elements(v_target_records)
      with ordinality as item(value, ordinality)
  ), '[]'::jsonb);

  v_supports := private.dataset_flow_identity_build_support_set_v2(
    v_actor, v_mappings
  );
  for v_mapping in select item.value
    from jsonb_array_elements(v_mappings) as item(value)
  loop
    v_validation := util.dataset_flow_identity_validate_mapping(
      v_actor, v_mapping, v_request->'compatibility_policy', v_supports,
      (v_mapping->>'ordinal')::integer
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_MAPPING_LIVE_MISMATCH';
    end if;
  end loop;
  v_mapping_index :=
    private.dataset_flow_identity_mapping_index_v2(v_mappings);

  if exists (
    select 1
    from jsonb_array_elements(v_request->'process_intents')
      with ordinality as item(value, ordinality)
    where case when jsonb_typeof(item.value->'ordinal') = 'number'
      then (item.value->>'ordinal')::numeric = item.ordinality::numeric
      else false end is not true
  ) then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_ORDINAL_MISMATCH';
  end if;
  with built as materialized (
    select item.ordinality::integer as ordinal,
      private.dataset_flow_identity_build_process_v2(
        v_actor, item.value, v_mapping_index,
        v_request->'compatibility_policy', v_supports, v_protected_sha256
      ) as manifest
    from jsonb_array_elements(v_request->'process_intents')
      with ordinality as item(value, ordinality)
  )
  select
    coalesce(jsonb_agg(built.manifest order by built.ordinal), '[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', built.manifest->'ordinal',
      'process_id', built.manifest->>'id',
      'process_version', built.manifest->>'version',
      'intent_proof_sha256',
        util.dataset_flow_identity_restricted_sha256_v2(built.manifest),
      'manifest', built.manifest
    ) order by built.ordinal), '[]'::jsonb),
    coalesce(sum((built.manifest->>'rewrite_count')::integer), 0)::integer
  into v_processes, v_process_records, v_rewrite_count
  from built;
  if v_rewrite_count <= 0 then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_CLOSURE_EMPTY';
  end if;

  select jsonb_agg(jsonb_build_object(
    'id', (item.value->>'source_id')::uuid,
    'version', item.value->>'source_version',
    'user_id', v_actor,
    'state_code', 0,
    'flow_type', 'Elementary flow'
  ) order by item.value->>'source_id', item.value->>'source_version')
  into v_source_universe
  from jsonb_array_elements(v_source_records) as item(value);
  v_source_universe_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_source_universe);
  v_validation := util.dataset_flow_identity_source_universe(
    v_actor, v_source_universe, v_source_universe_sha256
  );
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_SOURCE_UNIVERSE_LIVE_MISMATCH';
  end if;

  v_source_guard_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_source_records);
  v_target_guard_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_target_records);
  v_mapping_guard_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_mappings);
  v_process_intent_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_process_records);
  v_support_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_supports);
  v_mapping_set_sha256 := v_mapping_guard_set_sha256;
  v_process_manifest_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_processes);
  v_expires_at := v_captured_at + interval '7 days';
  v_whole_scope_proof_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
      'schema_version', 'dataset-flow-identity-capture-whole-scope.v2',
      'receipt_id', v_receipt_id,
      'actor_user_id', v_actor,
      'environment', v_request->>'environment',
      'project_ref', v_request->>'project_ref',
      'operation_id', v_request->>'operation_id',
      'capture_request_sha256', v_request_sha256,
      'source_guard_set_sha256', v_source_guard_set_sha256,
      'support_guard_set_sha256', v_support_set_sha256,
      'target_guard_set_sha256', v_target_guard_set_sha256,
      'mapping_guard_set_sha256', v_mapping_guard_set_sha256,
      'process_intent_set_sha256', v_process_intent_set_sha256,
      'protected_closure_sha256', v_protected_sha256,
      'captured_at', v_captured_at,
      'expires_at', v_expires_at
    ));
  v_receipt_proof_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'receipt_id', v_receipt_id,
      'whole_scope_proof_sha256', v_whole_scope_proof_sha256,
      'policy_sha256',
        v_request #>> '{compatibility_policy,policy_sha256}',
      'policy_approval_text_sha256',
        v_request #>> '{compatibility_policy,approval_text_sha256}'
    ));

  insert into util.dataset_flow_identity_capture_receipts (
    id, actor_user_id, actor_email, request_id, environment, project_ref,
    target_visibility, operation_id, compatibility_policy,
    policy_approval_text_sha256, artifact_evidence, protected_closure,
    protected_closure_sha256, source_universe, source_universe_sha256,
    support_snapshot_set_sha256, source_guard_set_sha256,
    target_guard_set_sha256, mapping_guard_set_sha256,
    process_intent_set_sha256, mapping_set_sha256,
    process_manifest_sha256, capture_request_sha256, receipt_proof_sha256,
    whole_scope_proof_sha256, source_count, target_count, support_count,
    mapping_count, process_count, rewrite_count, captured_at, expires_at
  ) values (
    v_receipt_id, v_actor, v_actor_email, (v_request->>'request_id')::uuid,
    v_request->>'environment', v_request->>'project_ref',
    'owner_draft', v_request->>'operation_id',
    v_request->'compatibility_policy',
    v_request #>> '{compatibility_policy,approval_text_sha256}',
    v_request->'artifact_evidence', v_protected, v_protected_sha256,
    v_source_universe, v_source_universe_sha256, v_support_set_sha256,
    v_source_guard_set_sha256, v_target_guard_set_sha256,
    v_mapping_guard_set_sha256, v_process_intent_set_sha256,
    v_mapping_set_sha256, v_process_manifest_sha256, v_request_sha256,
    v_receipt_proof_sha256, v_whole_scope_proof_sha256, 305,
    jsonb_array_length(v_target_records), jsonb_array_length(v_supports),
    jsonb_array_length(v_mappings), jsonb_array_length(v_processes),
    v_rewrite_count, v_captured_at, v_expires_at
  );
  insert into util.dataset_flow_identity_capture_source_guards (
    receipt_id, ordinal, disposition, source_id, source_version,
    guard, evidence_sha256
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      item.value->>'disposition', (item.value->>'source_id')::uuid,
      item.value->>'source_version', item.value->'guard',
      item.value->>'evidence_sha256'
    from jsonb_array_elements(v_source_records) as item(value);
  insert into util.dataset_flow_identity_capture_target_guards (
    receipt_id, ordinal, target_id, target_version, guard
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      (item.value->>'target_id')::uuid, item.value->>'target_version',
      item.value->'guard'
    from jsonb_array_elements(v_target_records) as item(value);
  insert into util.dataset_flow_identity_capture_support_guards (
    receipt_id, ordinal, support_table, support_id, support_version, guard
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      item.value->>'table', (item.value->>'id')::uuid,
      item.value->>'version', item.value
    from jsonb_array_elements(v_supports) as item(value);
  insert into util.dataset_flow_identity_capture_mapping_guards (
    receipt_id, ordinal, mapping_id, source_id, source_version,
    target_id, target_version, mapping
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      item.value->>'mapping_id', (item.value #>> '{source,id}')::uuid,
      item.value #>> '{source,version}',
      (item.value #>> '{target,id}')::uuid,
      item.value #>> '{target,version}', item.value
    from jsonb_array_elements(v_mappings) as item(value);
  insert into util.dataset_flow_identity_capture_process_intents (
    receipt_id, ordinal, process_id, process_version,
    intent_proof_sha256, manifest
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      (item.value->>'process_id')::uuid, item.value->>'process_version',
      item.value->>'intent_proof_sha256', item.value->'manifest'
    from jsonb_array_elements(v_process_records) as item(value);

  -- The helper is declared later in this migration, so use runtime resolution
  -- here.  It locks and revalidates all 305 sources, every support/target,
  -- every protected occurrence, and every intended process after the receipt
  -- relation has been fully inserted but before this transaction can return.
  execute
    'select private.dataset_flow_identity_whole_scope_proof_v2($1,$2,null,true)'
    into v_validation using v_actor, v_receipt_id;
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '40001',
      message = 'FLOW_IDENTITY_CAPTURE_FINAL_WHOLE_SCOPE_DRIFT';
  end if;

  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-capture-attest-result.v2',
    'proof_domain', 'dataset-flow-identity-db-proof.v2',
    'receipt_id', v_receipt_id,
    'receipt_proof_sha256', v_receipt_proof_sha256,
    'operation_id', v_request->>'operation_id',
    'environment', v_request->>'environment',
    'project_ref', v_request->>'project_ref',
    'captured_at', v_captured_at, 'expires_at', v_expires_at,
    'source_guard_set_sha256', v_source_guard_set_sha256,
    'support_guard_set_sha256', v_support_set_sha256,
    'target_guard_set_sha256', v_target_guard_set_sha256,
    'mapping_guard_set_sha256', v_mapping_guard_set_sha256,
    'process_intent_set_sha256', v_process_intent_set_sha256,
    'protected_closure_sha256', v_protected_sha256,
    'whole_scope_proof_sha256', v_whole_scope_proof_sha256,
    'policy_sha256',
      v_request #>> '{compatibility_policy,policy_sha256}',
    'policy_approval_text_sha256',
      v_request #>> '{compatibility_policy,approval_text_sha256}',
    'source_count', 305,
    'target_count', jsonb_array_length(v_target_records),
    'support_count', jsonb_array_length(v_supports),
    'mapping_count', jsonb_array_length(v_mappings),
    'process_count', jsonb_array_length(v_processes),
    'rewrite_count', v_rewrite_count,
    'capture_request_sha256', v_request_sha256,
    'replay', false
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', case when sqlstate = '55P03'
      then 'FLOW_IDENTITY_CAPTURE_LOCK_BUSY'
      else 'FLOW_IDENTITY_CAPTURE_FAILED' end,
    'status', case when sqlstate = '55P03' then 409 else 400 end,
    'message', sqlerrm, 'sqlstate', sqlstate
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_flow_identity_capture_attest_guarded"("p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_flow_identity_capture_attest_guarded"("p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_flow_identity_capture_attest_guarded"("p_request" "jsonb") TO "authenticated";
