CREATE OR REPLACE FUNCTION "public"."cmd_dataset_alias_execution_preflight_guarded"("p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_actor_email text := auth.email();
  v_schema_version constant text := 'dataset-alias-execution-preflight.v1';
  v_request_id uuid;
  v_environment text;
  v_project_ref text;
  v_server_context jsonb;
  v_request_actor jsonb;
  v_plan jsonb;
  v_freeze jsonb;
  v_approval jsonb;
  v_expected_freeze jsonb;
  v_expected_approval jsonb;
  v_bindings jsonb;
  v_expected jsonb;
  v_input_targets jsonb;
  v_targets jsonb;
  v_sorted_targets jsonb;
  v_gate_expectations jsonb;
  v_primary_gate_material jsonb;
  v_unused_gate_material jsonb;
  v_quiescence_gate_material jsonb;
  v_plan_sha256 text;
  v_operation_id text;
  v_plan_request_sha256 text;
  v_alias_plan_request_sha256 text;
  v_derivative_target_set_sha256 text;
  v_derivative_baseline_set_sha256 text;
  v_bindings_sha256 text;
  v_expected_sha256 text;
  v_targets_sha256 text;
  v_gate_expectations_sha256 text;
  v_failure_baseline_material jsonb;
  v_failure_baseline_sha256 text;
  v_request_sha256 text;
  v_token text;
  v_token_sha256 text;
  v_proof_material jsonb;
  v_proof_sha256 text;
  v_completed_at timestamp with time zone;
  v_expires_at timestamp with time zone;
  v_target jsonb;
  v_snapshot jsonb;
  v_alias_result jsonb;
  v_batch_result jsonb;
  v_simulation_passed boolean := false;
  v_simulation_error jsonb;
  v_existing_id uuid;
  v_execution_count integer := 0;
  v_alias_audit_count integer := 0;
  v_derivative_child_count integer := 0;
  v_snapshot_drift_count integer := 0;
  v_active_rebuild_count integer := 0;
  v_http_count integer := 0;
  v_extraction_count integer := 0;
  v_embedding_count integer := 0;
  v_pending_count integer := 0;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if nullif(v_actor_email, '') is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_EMAIL_REQUIRED',
      'status', 401,
      'message', 'Authenticated email claim is required'
    );
  end if;

  if p_request is not null and pg_column_size(p_request) > 67108864 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_TOO_LARGE',
      'status', 413,
      'message', 'Protected preflight request exceeds 64 MiB'
    );
  end if;

  if jsonb_typeof(p_request) is distinct from 'object'
    or not (p_request ?& array[
      'schema_version',
      'request_id',
      'environment',
      'project_ref',
      'actor',
      'target_visibility',
      'plan',
      'freeze',
      'approval',
      'bindings',
      'expected',
      'derivative_targets'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_request) as request_key(key)
      where request_key.key <> all (array[
        'schema_version',
        'request_id',
        'environment',
        'project_ref',
        'actor',
        'target_visibility',
        'plan',
        'freeze',
        'approval',
        'bindings',
        'expected',
        'derivative_targets'
      ])
    )
    or p_request->>'schema_version' is distinct from v_schema_version
    or jsonb_typeof(p_request->'request_id') is distinct from 'string'
    or (p_request->>'request_id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'environment' not in ('production', 'preview', 'local')
    or jsonb_typeof(p_request->'project_ref') is distinct from 'string'
    or nullif(btrim(p_request->>'project_ref'), '') is null
    or octet_length(p_request->>'project_ref') > 128
    or p_request->>'target_visibility' is distinct from 'owner_draft'
    or jsonb_typeof(p_request->'actor') is distinct from 'object'
    or jsonb_typeof(p_request->'plan') is distinct from 'object'
    or jsonb_typeof(p_request->'freeze') is distinct from 'object'
    or jsonb_typeof(p_request->'approval') is distinct from 'object'
    or jsonb_typeof(p_request->'bindings') is distinct from 'object'
    or jsonb_typeof(p_request->'expected') is distinct from 'object'
    or jsonb_typeof(p_request->'derivative_targets') is distinct from 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_INVALID_REQUEST',
      'status', 400,
      'message', 'Preflight request must match dataset-alias-execution-preflight.v1 exactly'
    );
  end if;

  v_request_id := (p_request->>'request_id')::uuid;
  v_environment := p_request->>'environment';
  v_project_ref := btrim(p_request->>'project_ref');
  v_request_actor := p_request->'actor';
  v_plan := p_request->'plan';
  v_freeze := p_request->'freeze';
  v_approval := p_request->'approval';
  v_bindings := p_request->'bindings';
  v_expected := p_request->'expected';
  v_input_targets := p_request->'derivative_targets';

  begin
    v_server_context := util.dataset_alias_execution_server_context();
  exception
    when others then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_SERVER_CONTEXT_UNAVAILABLE',
        'status', 409,
        'message', 'Branch-local project identity could not be derived from trusted server configuration'
      );
  end;

  if v_environment is distinct from v_server_context->>'environment'
    or v_project_ref is distinct from v_server_context->>'project_ref' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_SERVER_CONTEXT_MISMATCH',
      'status', 409,
      'message', 'Requested environment and project_ref do not match the connected database'
    );
  end if;

  if not (v_request_actor ?& array['user_id', 'email'])
    or exists (
      select 1
      from jsonb_object_keys(v_request_actor) as actor_key(key)
      where actor_key.key <> all (array['user_id', 'email'])
    )
    or jsonb_typeof(v_request_actor->'user_id') is distinct from 'string'
    or v_request_actor->>'user_id' is distinct from v_actor::text
    or jsonb_typeof(v_request_actor->'email') is distinct from 'string'
    or lower(btrim(v_request_actor->>'email'))
      is distinct from lower(btrim(v_actor_email))
    or octet_length(v_request_actor->>'email') > 320 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_ACTOR_MISMATCH',
      'status', 403,
      'message', 'Preflight actor must match the authenticated user and email'
    );
  end if;

  if not (v_bindings ?& array[
      'plan_file_sha256',
      'freeze_file_sha256',
      'freeze_sha256',
      'approval_file_sha256',
      'approval_identity_sha256',
      'approval_text_sha256',
      'alias_plan_request_sha256',
      'before_hash_set_sha256',
      'desired_hash_set_sha256',
      'exchange_rewrite_set_sha256',
      'support_snapshot_set_sha256',
      'derivative_baseline_set_sha256',
      'derivative_target_set_sha256',
      'toolchain_evidence_sha256'
    ])
    or exists (
      select 1
      from jsonb_object_keys(v_bindings) as binding_key(key)
      where binding_key.key <> all (array[
        'plan_file_sha256',
        'freeze_file_sha256',
        'freeze_sha256',
        'approval_file_sha256',
        'approval_identity_sha256',
        'approval_text_sha256',
        'alias_plan_request_sha256',
        'before_hash_set_sha256',
        'desired_hash_set_sha256',
        'exchange_rewrite_set_sha256',
        'support_snapshot_set_sha256',
        'derivative_baseline_set_sha256',
        'derivative_target_set_sha256',
        'toolchain_evidence_sha256'
      ])
    )
    or exists (
      select 1
      from jsonb_each(v_bindings) as binding_item(key, value)
      where jsonb_typeof(binding_item.value) is distinct from 'string'
        or (binding_item.value #>> '{}') !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_INVALID_BINDINGS',
      'status', 400,
      'message', 'All protected artifact bindings must be exact SHA-256 values'
    );
  end if;

  if not (v_expected ?& array[
      'action_count',
      'batch_count',
      'exchange_count',
      'amount_field_count',
      'unrelated_exchange_count',
      'audit_count',
      'flowproperty_count',
      'flow_count',
      'process_count',
      'derivative_target_count'
    ])
    or exists (
      select 1
      from jsonb_object_keys(v_expected) as expected_key(key)
      where expected_key.key <> all (array[
        'action_count',
        'batch_count',
        'exchange_count',
        'amount_field_count',
        'unrelated_exchange_count',
        'audit_count',
        'flowproperty_count',
        'flow_count',
        'process_count',
        'derivative_target_count'
      ])
    )
    or v_expected is distinct from jsonb_build_object(
      'action_count', 52,
      'batch_count', 2,
      'exchange_count', 59,
      'amount_field_count', 118,
      'unrelated_exchange_count', 309,
      'audit_count', 55,
      'flowproperty_count', 2,
      'flow_count', 23,
      'process_count', 27,
      'derivative_target_count', 50
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_INVALID_COUNTS',
      'status', 400,
      'message', 'Protected profile requires exact 52/59/118/309/55 and 2/23/27/50 counts'
    );
  end if;

  if jsonb_array_length(v_input_targets) <> 50
    or exists (
      select 1
      from jsonb_array_elements(v_input_targets) as target_item(value)
      where jsonb_typeof(target_item.value) is distinct from 'object'
        or not (target_item.value ?& array[
          'table',
          'id',
          'version',
          'user_id',
          'state_code',
          'baseline_snapshot_sha256'
        ])
        or exists (
          select 1
          from jsonb_object_keys(target_item.value) as target_key(key)
          where target_key.key <> all (array[
            'table',
            'id',
            'version',
            'user_id',
            'state_code',
            'baseline_snapshot_sha256'
          ])
        )
        or target_item.value->>'table' not in ('flows', 'processes')
        or jsonb_typeof(target_item.value->'table') is distinct from 'string'
        or jsonb_typeof(target_item.value->'id') is distinct from 'string'
        or (target_item.value->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or jsonb_typeof(target_item.value->'version') is distinct from 'string'
        or (target_item.value->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or jsonb_typeof(target_item.value->'user_id') is distinct from 'string'
        or target_item.value->>'user_id' is distinct from v_actor::text
        or jsonb_typeof(target_item.value->'state_code') is distinct from 'number'
        or target_item.value->>'state_code' is distinct from '0'
        or jsonb_typeof(target_item.value->'baseline_snapshot_sha256')
          is distinct from 'string'
        or (target_item.value->>'baseline_snapshot_sha256') !~ '^[a-f0-9]{64}$'
    )
    or (
      select count(*)
      from jsonb_array_elements(v_input_targets) as target_item(value)
      where target_item.value->>'table' = 'flows'
    ) <> 23
    or (
      select count(*)
      from jsonb_array_elements(v_input_targets) as target_item(value)
      where target_item.value->>'table' = 'processes'
    ) <> 27
    or (
      select count(distinct (
        target_item.value->>'table',
        target_item.value->>'id',
        target_item.value->>'version'
      ))
      from jsonb_array_elements(v_input_targets) as target_item(value)
    ) <> 50 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_INVALID_TARGETS',
      'status', 400,
      'message', 'Derivative targets must be 23 unique flows and 27 unique processes owned by the actor at state_code 0'
    );
  end if;

  select jsonb_agg(target_item.value order by
    target_item.value->>'table',
    target_item.value->>'id',
    target_item.value->>'version'
  )
  into v_sorted_targets
  from jsonb_array_elements(v_input_targets) as target_item(value);

  if v_input_targets is distinct from v_sorted_targets then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_TARGET_ORDER',
      'status', 400,
      'message', 'Derivative targets must use stable table/id/version order'
    );
  end if;

  if v_plan->>'schema_version' is distinct from 'dataset-alias-plan.v1'
    or v_plan->>'target_visibility' is distinct from 'owner_draft'
    or (v_plan->>'plan_sha256') !~ '^[a-f0-9]{64}$'
    or nullif(btrim(v_plan->>'operation_id'), '') is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_INVALID_PLAN',
      'status', 400,
      'message', 'Protected preflight requires one owner-draft dataset-alias-plan.v1 request'
    );
  end if;

  v_plan_sha256 := v_plan->>'plan_sha256';
  v_operation_id := btrim(v_plan->>'operation_id');

  select jsonb_agg(
    jsonb_build_object(
      'table', target_item.value->>'table',
      'id', target_item.value->>'id',
      'version', target_item.value->>'version',
      'expected_json_ordered_sha256',
        util.dataset_alias_execution_sha256(
          (action_item.value->'desired_json_ordered')::text
        ),
      'baseline_snapshot_sha256',
        target_item.value->>'baseline_snapshot_sha256'
    ) order by
      target_item.value->>'table',
      target_item.value->>'id',
      target_item.value->>'version'
  )
  into v_targets
  from jsonb_array_elements(v_plan->'batches') as batch_item(value)
  cross join lateral jsonb_array_elements(
    batch_item.value->'actions'
  ) as action_item(value)
  join jsonb_array_elements(v_input_targets) as target_item(value)
    on target_item.value->>'table' = action_item.value->>'table'
   and target_item.value->>'id' = action_item.value->>'id'
   and target_item.value->>'version' = action_item.value->>'version'
  where action_item.value->>'table' in ('flows', 'processes');

  if jsonb_typeof(v_targets) is distinct from 'array'
    or jsonb_array_length(v_targets) <> 50 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_TARGET_PLAN_MISMATCH',
      'status', 409,
      'message', 'Derivative target identities must exactly match the 50 flow/process plan actions'
    );
  end if;

  v_alias_plan_request_sha256 :=
    util.dataset_alias_execution_artifact_sha256(v_plan);

  select util.dataset_alias_execution_artifact_sha256(
    jsonb_agg(
      jsonb_build_object(
        'table', target_item.value->>'table',
        'id', target_item.value->>'id',
        'version', target_item.value->>'version',
        'user_id', target_item.value->>'user_id',
        'state_code', 0
      ) order by
        target_item.value->>'table',
        target_item.value->>'id',
        target_item.value->>'version'
    )
  )
  into v_derivative_target_set_sha256
  from jsonb_array_elements(v_input_targets) as target_item(value);

  select util.dataset_alias_execution_artifact_sha256(
    jsonb_agg(
      jsonb_build_object(
        'table', target_item.value->>'table',
        'id', target_item.value->>'id',
        'version', target_item.value->>'version',
        'baseline_snapshot_sha256',
          target_item.value->>'baseline_snapshot_sha256'
      ) order by
        target_item.value->>'table',
        target_item.value->>'id',
        target_item.value->>'version'
    )
  )
  into v_derivative_baseline_set_sha256
  from jsonb_array_elements(v_input_targets) as target_item(value);

  if v_bindings->>'alias_plan_request_sha256'
      is distinct from v_alias_plan_request_sha256
    or v_bindings->>'derivative_target_set_sha256'
      is distinct from v_derivative_target_set_sha256
    or v_bindings->>'derivative_baseline_set_sha256'
      is distinct from v_derivative_baseline_set_sha256 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_ARTIFACT_SET_MISMATCH',
      'status', 409,
      'message', 'The approved alias request or derivative target sets do not match server recomputation'
    );
  end if;

  v_expected_freeze := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-freeze.v1',
    'environment', v_environment,
    'project_ref', v_project_ref,
    'account', v_request_actor,
    'target_visibility', 'owner_draft',
    'plan', jsonb_build_object(
      'plan_file_sha256', v_bindings->>'plan_file_sha256',
      'plan_sha256', v_plan_sha256,
      'operation_id', v_operation_id
    ),
    'sets', jsonb_build_object(
      'alias_plan_request_sha256',
        v_bindings->>'alias_plan_request_sha256',
      'before_hash_set_sha256',
        v_bindings->>'before_hash_set_sha256',
      'desired_hash_set_sha256',
        v_bindings->>'desired_hash_set_sha256',
      'exchange_rewrite_set_sha256',
        v_bindings->>'exchange_rewrite_set_sha256',
      'support_snapshot_set_sha256',
        v_bindings->>'support_snapshot_set_sha256',
      'derivative_baseline_set_sha256',
        v_bindings->>'derivative_baseline_set_sha256',
      'derivative_target_set_sha256',
        v_bindings->>'derivative_target_set_sha256',
      'toolchain_evidence_sha256',
        v_bindings->>'toolchain_evidence_sha256'
    ),
    'expected', v_expected,
    'derivative_targets', v_input_targets,
    'policy', jsonb_build_object(
      'state_code_changes', 0,
      'save_draft', 0,
      'deletes', 0,
      'rebuild_derivatives', 0,
      'unitgroup_actions', 0,
      'person_distance_actions', 0,
      'max_admit_posts', 1,
      'automatic_retry', false
    ),
    'freeze_sha256', v_bindings->>'freeze_sha256'
  );

  if v_freeze is distinct from v_expected_freeze
    or util.dataset_alias_execution_artifact_sha256(
      jsonb_set(v_freeze, '{freeze_sha256}', '""'::jsonb, false)
    ) is distinct from v_bindings->>'freeze_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_FREEZE_MISMATCH',
      'status', 409,
      'message', 'The production freeze envelope or canonical freeze hash is invalid'
    );
  end if;

  begin
    perform (v_approval->>'approved_at_utc')::timestamp with time zone;
  exception
    when others then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_PREFLIGHT_APPROVAL_MISMATCH',
        'status', 409,
        'message', 'The exact approval timestamp is invalid'
      );
  end;

  v_expected_approval := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-approval.v1',
    'approved_at_utc', v_approval->>'approved_at_utc',
    'environment', v_environment,
    'project_ref', v_project_ref,
    'account', v_request_actor,
    'target_visibility', 'owner_draft',
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'plan_file_sha256', v_bindings->>'plan_file_sha256',
    'freeze_file_sha256', v_bindings->>'freeze_file_sha256',
    'freeze_sha256', v_bindings->>'freeze_sha256',
    'approval_text_sha256', v_bindings->>'approval_text_sha256',
    'max_admit_posts', 1,
    'automatic_retry', false,
    'approval_identity_sha256', v_bindings->>'approval_identity_sha256'
  );

  if v_approval is distinct from v_expected_approval
    or util.dataset_alias_execution_artifact_sha256(
      jsonb_set(
        v_approval,
        '{approval_identity_sha256}',
        '""'::jsonb,
        false
      )
    ) is distinct from v_bindings->>'approval_identity_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_APPROVAL_MISMATCH',
      'status', 409,
      'message', 'The approval identity does not bind this exact production freeze and alias request'
    );
  end if;

  select preflight.id
  into v_existing_id
  from util.dataset_alias_execution_preflights as preflight
  where preflight.id = v_request_id
     or (
       preflight.actor_user_id = v_actor
       and preflight.approval_identity_sha256 =
         v_bindings->>'approval_identity_sha256'
     )
  limit 1;

  if v_existing_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_APPROVAL_ALREADY_USED',
      'status', 409,
      'message', 'This request ID or exact approval identity already created a protected preflight; freeze and approve again'
    );
  end if;

  for v_target in
    select target_item.value
    from jsonb_array_elements(v_targets) as target_item(value)
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
      or v_snapshot->>'json_sha256' is distinct from v_snapshot->>'json_ordered_sha256'
      or v_snapshot->>'snapshot_sha256'
        is distinct from v_target->>'baseline_snapshot_sha256' then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_PREFLIGHT_BASELINE_DRIFT',
        'status', 409,
        'message', 'A derivative target no longer matches its owner-draft baseline snapshot'
      );
    end if;
  end loop;

  v_plan_request_sha256 := util.dataset_alias_execution_sha256(v_plan::text);
  v_bindings_sha256 := util.dataset_alias_execution_sha256(v_bindings::text);
  v_expected_sha256 := util.dataset_alias_execution_sha256(v_expected::text);
  v_targets_sha256 := util.dataset_alias_execution_sha256(v_targets::text);
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
  into v_failure_baseline_material
  from util.embedding_job_failures as failure
  where exists (
    select 1
    from jsonb_array_elements(v_targets) as target_item(value)
    where failure.message->>'table' = target_item.value->>'table'
      and failure.message->>'id' = target_item.value->>'id'
      and btrim(failure.message->>'version') = target_item.value->>'version'
  );
  v_failure_baseline_sha256 :=
    util.dataset_alias_execution_sha256(v_failure_baseline_material::text);
  v_request_sha256 := util.dataset_alias_execution_sha256(p_request::text);

  -- The simulation creates all normal alias writes, audits, webhook work, and
  -- derivative fences inside this exception block.  The controlled P0002
  -- exception always rolls those effects back before a durable token exists.
  begin
    v_alias_result := public.cmd_dataset_alias_plan_guarded(v_plan);
    if coalesce((v_alias_result->>'ok')::boolean, false) is not true
      or coalesce((v_alias_result->>'idempotent_replay')::boolean, true)
      or v_alias_result->>'row_count' is distinct from '52'
      or v_alias_result->>'exchange_count' is distinct from '59' then
      v_simulation_error := jsonb_build_object(
        'phase', 'alias',
        'result', coalesce(v_alias_result, '{}'::jsonb)
      );
      raise exception using
        errcode = 'P0001',
        message = 'Protected alias simulation rejected';
    end if;

    v_batch_result := util.admit_dataset_derivative_rebuild_batch(
      v_actor,
      v_request_id,
      v_plan_sha256,
      v_operation_id,
      'PROTECTED_ALIAS_DERIVATIVE_CLOSURE',
      v_targets
    );

    if coalesce((v_batch_result->>'ok')::boolean, false) is not true
      or v_batch_result->>'target_count' is distinct from '50'
      or v_batch_result->>'flow_count' is distinct from '23'
      or v_batch_result->>'process_count' is distinct from '27' then
      v_simulation_error := jsonb_build_object(
        'phase', 'derivative_batch',
        'result', coalesce(v_batch_result, '{}'::jsonb)
      );
      raise exception using
        errcode = 'P0001',
        message = 'Protected derivative batch simulation rejected';
    end if;

    raise exception using
      errcode = 'P0002',
      message = 'Protected execution preflight simulation rollback';
  exception
    when sqlstate 'P0002' then
      v_simulation_passed := true;
    when others then
      v_simulation_passed := false;
      if v_simulation_error is null then
        v_simulation_error := jsonb_build_object(
          'phase', 'unexpected',
          'sqlstate', sqlstate,
          'message', sqlerrm
        );
      end if;
  end;

  if not v_simulation_passed then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_SIMULATION_FAILED',
      'status', 409,
      'message', 'The exact protected plan failed rollback-only server simulation',
      'evidence', v_simulation_error
    );
  end if;

  select count(*)::integer
  into v_execution_count
  from util.dataset_alias_execution_requests as request
  where request.actor_user_id = v_actor
    and request.approval_identity_sha256 =
      v_bindings->>'approval_identity_sha256';

  select count(*)::integer
  into v_alias_audit_count
  from public.command_audit_log as audit
  where audit.actor_user_id = v_actor
    and (
      (
        audit.command = 'cmd_dataset_alias_batch_guarded'
        and audit.payload->>'plan_sha256' = v_plan_sha256
        and audit.payload->>'operation_id' = v_operation_id
      )
      or (
        audit.command = 'cmd_dataset_alias_plan_guarded'
        and audit.payload->>'plan_request_sha256' = v_plan_request_sha256
      )
    );

  select count(*)::integer
  into v_derivative_child_count
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = v_actor
    and request.batch_id = v_request_id;

  for v_target in
    select target_item.value
    from jsonb_array_elements(v_targets) as target_item(value)
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
      from jsonb_array_elements(v_targets) as target_item(value)
      where request.target_table = target_item.value->>'table'
        and request.target_id = (target_item.value->>'id')::uuid
        and request.target_version = target_item.value->>'version'
    );

  select count(*)::integer
  into v_http_count
  from net.http_request_queue as request
  where exists (
    select 1
    from jsonb_array_elements(v_targets) as target_item(value)
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
    from jsonb_array_elements(v_targets) as target_item(value)
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
    from jsonb_array_elements(v_targets) as target_item(value)
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
      from jsonb_array_elements(v_targets) as target_item(value)
      where pending.table_name = target_item.value->>'table'
        and pending.record_id = target_item.value->>'id'
        and btrim(pending.record_version) = target_item.value->>'version'
    );

  if v_execution_count <> 0
    or v_alias_audit_count <> 0
    or v_derivative_child_count <> 0
    or v_snapshot_drift_count <> 0
    or v_active_rebuild_count <> 0
    or v_http_count <> 0
    or v_extraction_count <> 0
    or v_embedding_count <> 0
    or v_pending_count <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_NOT_QUIESCENT',
      'status', 409,
      'message', 'The exact approval identity, targets, or derivative queues are not unused and quiescent'
    );
  end if;

  v_primary_gate_material := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-gate-material.v1',
    'gate', 'primary_support_plan',
    'request_id', v_request_id,
    'actor_user_id', v_actor,
    'plan_request_sha256', v_plan_request_sha256,
    'derivative_targets_sha256', v_targets_sha256,
    'plan_rows', 52,
    'plan_exchanges', 59,
    'alias_audits', 55,
    'derivative_targets', 50,
    'rollback_simulation_passed', true
  );
  v_unused_gate_material := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-gate-material.v1',
    'gate', 'execution_unused',
    'request_id', v_request_id,
    'actor_user_id', v_actor,
    'plan_request_sha256', v_plan_request_sha256,
    'sealed_execution_rows', v_execution_count,
    'alias_audit_rows', v_alias_audit_count,
    'derivative_child_rows', v_derivative_child_count
  );
  v_quiescence_gate_material := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-gate-material.v1',
    'gate', 'derivative_quiescence',
    'request_id', v_request_id,
    'actor_user_id', v_actor,
    'derivative_targets_sha256', v_targets_sha256,
    'snapshot_drift_count', v_snapshot_drift_count,
    'active_rebuild_count', v_active_rebuild_count,
    'http_request_count', v_http_count,
    'extraction_job_count', v_extraction_count,
    'embedding_job_count', v_embedding_count,
    'pending_embedding_count', v_pending_count,
    'failure_baseline_sha256', v_failure_baseline_sha256
  );
  v_gate_expectations := jsonb_build_object(
    'primary_support_plan_sha256',
      util.dataset_alias_execution_sha256(v_primary_gate_material::text),
    'execution_unused_sha256',
      util.dataset_alias_execution_sha256(v_unused_gate_material::text),
    'derivative_quiescence_sha256',
      util.dataset_alias_execution_sha256(v_quiescence_gate_material::text)
  );
  v_gate_expectations_sha256 :=
    util.dataset_alias_execution_sha256(v_gate_expectations::text);

  select preflight.id
  into v_existing_id
  from util.dataset_alias_execution_preflights as preflight
  where preflight.id = v_request_id
     or (
       preflight.actor_user_id = v_actor
       and preflight.preflight_request_sha256 = v_request_sha256
     )
  limit 1;

  if v_existing_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_ALREADY_EXISTS',
      'status', 409,
      'message', 'A preflight request ID or exact request was already used; tokens are never replayed'
    );
  end if;

  v_completed_at := pg_catalog.clock_timestamp();
  v_expires_at := v_completed_at + interval '180 seconds';
  v_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
  v_token_sha256 := util.dataset_alias_execution_sha256(v_token);
  v_proof_material := jsonb_build_object(
    'schema_version', 'dataset-alias-execution-preflight-proof.v1',
    'request_id', v_request_id,
    'actor_user_id', v_actor,
    'environment', v_environment,
    'project_ref', v_project_ref,
    'server_context_sha256',
      util.dataset_alias_execution_sha256(v_server_context::text),
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'alias_plan_request_sha256', v_alias_plan_request_sha256,
    'freeze_sha256', v_bindings->>'freeze_sha256',
    'approval_identity_sha256',
      v_bindings->>'approval_identity_sha256',
    'plan_request_sha256', v_plan_request_sha256,
    'bindings_sha256', v_bindings_sha256,
    'expected_sha256', v_expected_sha256,
    'derivative_targets_sha256', v_targets_sha256,
    'gate_expectations', v_gate_expectations,
    'gate_expectations_sha256', v_gate_expectations_sha256,
    'failure_baseline_sha256', v_failure_baseline_sha256,
    'preflight_request_sha256', v_request_sha256,
    'completed_at', v_completed_at,
    'expires_at', v_expires_at
  );
  v_proof_sha256 := util.dataset_alias_execution_sha256(v_proof_material::text);

  insert into util.dataset_alias_execution_preflights (
    id,
    actor_user_id,
    actor_email,
    environment,
    project_ref,
    target_visibility,
    plan,
    freeze_envelope,
    approval_envelope,
    plan_sha256,
    operation_id,
    plan_request_sha256,
    bindings,
    bindings_sha256,
    expected,
    expected_sha256,
    derivative_targets,
    derivative_targets_sha256,
    gate_expectations,
    gate_expectations_sha256,
    failure_baseline_sha256,
    preflight_request_sha256,
    preflight_proof_sha256,
    freeze_sha256,
    approval_identity_sha256,
    token_sha256,
    completed_at,
    expires_at
  ) values (
    v_request_id,
    v_actor,
    lower(btrim(v_actor_email)),
    v_environment,
    v_project_ref,
    'owner_draft',
    v_plan,
    v_freeze,
    v_approval,
    v_plan_sha256,
    v_operation_id,
    v_plan_request_sha256,
    v_bindings,
    v_bindings_sha256,
    v_expected,
    v_expected_sha256,
    v_targets,
    v_targets_sha256,
    v_gate_expectations,
    v_gate_expectations_sha256,
    v_failure_baseline_sha256,
    v_request_sha256,
    v_proof_sha256,
    v_bindings->>'freeze_sha256',
    v_bindings->>'approval_identity_sha256',
    v_token_sha256,
    v_completed_at,
    v_expires_at
  );

  return v_proof_material || jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_alias_execution_preflight_guarded',
    'preflight_token', v_token,
    'preflight_proof_sha256', v_proof_sha256,
    'simulation', jsonb_build_object(
      'plan_rows', 52,
      'plan_exchanges', 59,
      'alias_audits', 55,
      'derivative_targets', 50,
      'rolled_back', true
    )
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_LOCK_BUSY',
      'status', 409,
      'message', 'Protected preflight could not acquire its bounded locks'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_CONCURRENT_CONFLICT',
      'status', 409,
      'message', 'A concurrent preflight consumed the same request identity'
    );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_alias_execution_preflight_guarded"("p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_alias_execution_preflight_guarded"("p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_alias_execution_preflight_guarded"("p_request" "jsonb") TO "authenticated";
