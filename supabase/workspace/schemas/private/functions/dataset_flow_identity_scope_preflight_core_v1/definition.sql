CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_scope_preflight_core_v1"("p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '120s'
    AS $_$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_preflight_guarded';
  v_schema_version constant text :=
    'dataset-flow-identity-scope-preflight.v1';
  v_result_schema_version constant text :=
    'dataset-flow-identity-scope-preflight-result.v1';
  v_top_keys constant text[] := array[
    'schema_version', 'request_id', 'environment', 'project_ref', 'actor',
    'target_visibility', 'operation_id', 'plan_sha256', 'freeze_sha256',
    'approval_identity_sha256', 'approval_text_sha256',
    'toolchain_evidence_sha256', 'compatibility_policy',
    'support_snapshot_set_sha256', 'support_snapshots',
    'source_universe_sha256', 'source_universe_count',
    'mapping_set_sha256', 'process_manifest_sha256',
    'protected_closure_sha256', 'mappings', 'processes',
    'protected_closure'
  ];
  v_policy_keys constant text[] := array[
    'schema_version', 'policy_sha256', 'evidence_resolution_sha256',
    'approved_at_utc', 'approval_text_sha256'
  ];
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_context jsonb;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_scope_id uuid;
  v_scope_request_sha256 text;
  v_scope_proof_sha256 text;
  v_mapping jsonb;
  v_mapping_index jsonb;
  v_manifest jsonb;
  v_validation jsonb;
  v_protected_proof jsonb;
  v_universe_proof jsonb;
  v_source_universe jsonb;
  v_support_validation jsonb;
  v_mapping_count integer;
  v_support_count integer;
  v_process_count integer;
  v_rewrite_count integer;
  v_approved_reference_count integer;
  v_process_reference_mismatch_count integer;
  v_unlisted_reference_count integer;
  v_audit_id bigint;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;

  if p_request is null
    or pg_column_size(p_request) > 134217728
    or not private.dataset_flow_identity_exact_keys(p_request, v_top_keys)
    or p_request->>'schema_version' <> v_schema_version
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'target_visibility' <> 'owner_draft'
    or nullif(btrim(p_request->>'operation_id'), '') is null
    or octet_length(p_request->>'operation_id') > 512
    or not private.dataset_flow_identity_exact_keys(
      p_request->'actor', array['user_id', 'email']
    )
    or p_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(p_request #>> '{actor,email}'))
      is distinct from v_actor_email
    or not private.dataset_flow_identity_exact_keys(
      p_request->'compatibility_policy', v_policy_keys
    )
    or p_request #>> '{compatibility_policy,schema_version}'
      <> 'dataset-flow-identity-compatibility-policy.v1'
    or jsonb_typeof(p_request->'mappings') <> 'array'
    or jsonb_typeof(p_request->'processes') <> 'array'
    or jsonb_typeof(p_request->'support_snapshots') <> 'array'
    or jsonb_array_length(p_request->'mappings') not between 1 and 305
    or jsonb_array_length(p_request->'processes') not between 1 and 12000
    or jsonb_array_length(p_request->'support_snapshots') not between 2 and 100
    or p_request->>'source_universe_count' <> '305'
    or exists (
      select 1
      from unnest(array[
        'plan_sha256', 'freeze_sha256', 'approval_identity_sha256',
        'approval_text_sha256', 'toolchain_evidence_sha256',
        'support_snapshot_set_sha256', 'source_universe_sha256',
        'mapping_set_sha256', 'process_manifest_sha256',
        'protected_closure_sha256'
      ]) as hash_field(name)
      where p_request->>hash_field.name !~ '^[a-f0-9]{64}$'
    )
    or exists (
      select 1
      from unnest(array[
        'policy_sha256', 'evidence_resolution_sha256',
        'approval_text_sha256'
      ]) as hash_field(name)
      where p_request->'compatibility_policy'->>hash_field.name
        !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_INVALID_REQUEST',
      'status', 400, 'message', 'Step 3 preflight request schema mismatch'
    );
  end if;

  begin
    perform (p_request #>>
      '{compatibility_policy,approved_at_utc}')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_POLICY_TIME_INVALID',
      'status', 400, 'message', 'Compatibility policy approval time is invalid'
    );
  end;

  v_context := util.dataset_alias_execution_server_context();
  if p_request->>'environment' is distinct from v_context->>'environment'
    or p_request->>'project_ref' is distinct from v_context->>'project_ref' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_ENVIRONMENT_MISMATCH',
      'status', 409, 'message', 'Request does not target this database branch'
    );
  end if;

  if util.dataset_flow_identity_restricted_sha256_v2(p_request->'mappings')
      is distinct from p_request->>'mapping_set_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'support_snapshots'
    )
      is distinct from p_request->>'support_snapshot_set_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(p_request->'processes')
      is distinct from p_request->>'process_manifest_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'protected_closure'
    )
      is distinct from p_request->>'protected_closure_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_ARTIFACT_HASH_MISMATCH',
      'status', 409, 'message', 'A sealed artifact hash does not match'
    );
  end if;

  v_scope_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(p_request);
  select scope.*
  into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.actor_user_id = v_actor
    and scope.operation_id = p_request->>'operation_id';

  if v_scope.id is not null then
    if v_scope.scope_request_sha256 is distinct from v_scope_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_OPERATION_REUSE_MISMATCH',
        'status', 409, 'message', 'Operation ID is already sealed differently'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'command', v_command,
      'schema_version', v_result_schema_version,
      'scope_id', v_scope.id,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', v_scope.status,
      'process_count', v_scope.process_count,
      'mapping_count', v_scope.mapping_count,
      'rewrite_count', v_scope.rewrite_count,
      'next_ordinal', coalesce((
        select min(ledger.ordinal)
        from util.dataset_flow_identity_process_ledger as ledger
        where ledger.scope_id = v_scope.id and ledger.status = 'pending'
      ), v_scope.process_count + 1),
      'replay', true
    );
  end if;

  -- Validate and deterministically lock only actor-owned mutable rows.  Public
  -- targets/support remain optimistic hash guards and are rechecked by every
  -- process plus finalize.  No relation-level lock may block unrelated actors.
  v_support_count := jsonb_array_length(p_request->'support_snapshots');
  for v_mapping in
    select item.value
    from jsonb_array_elements(p_request->'support_snapshots')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_support_validation :=
      util.dataset_flow_identity_validate_support_snapshot(v_actor, v_mapping);
    if coalesce((v_support_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_SUPPORT_REJECTED',
        'status', 409, 'message', 'A sealed FP/UG support row is invalid',
        'details', v_support_validation
      );
    end if;
  end loop;

  if (
    select count(distinct (
      (item.value->>'table') || ':' || (item.value->>'id') || '@'
        || (item.value->>'version')
    )) = v_support_count
      and min((item.value->>'ordinal')::integer) = 1
      and max((item.value->>'ordinal')::integer) = v_support_count
      and bool_and(
        (item.value->>'ordinal')::integer = item.ordinality::integer
      )
    from jsonb_array_elements(p_request->'support_snapshots')
      with ordinality as item(value, ordinality)
  ) is not true or (
    with claimed as (
      select distinct 'flowproperties'::text as support_table,
        guard.value->>'flow_property_id' as id,
        guard.value->>'flow_property_version' as version
      from jsonb_array_elements(p_request->'mappings') as mapping(value)
      cross join lateral jsonb_array_elements(
        jsonb_build_array(mapping.value->'source', mapping.value->'target')
      ) as guard(value)
      union
      select distinct 'unitgroups',
        guard.value->>'unit_group_id', guard.value->>'unit_group_version'
      from jsonb_array_elements(p_request->'mappings') as mapping(value)
      cross join lateral jsonb_array_elements(
        jsonb_build_array(mapping.value->'source', mapping.value->'target')
      ) as guard(value)
    ), sealed as (
      select item.value->>'table' as support_table,
        item.value->>'id' as id, item.value->>'version' as version
      from jsonb_array_elements(p_request->'support_snapshots') as item(value)
    )
    select count(*) from (
      (select * from claimed except select * from sealed)
      union all
      (select * from sealed except select * from claimed)
    ) as difference
  ) <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_SUPPORT_SET_MISMATCH',
      'status', 409,
      'message', 'Support snapshots must exactly cover every claimed FP/UG'
    );
  end if;

  perform 1
  from public.flows as flow
  join (
    select (mapping.value #>> '{source,id}')::uuid as id,
      mapping.value #>> '{source,version}' as version
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
    union
    select (item.value->>'source_id')::uuid,
      item.value->>'source_version'
    from jsonb_array_elements(
      coalesce(p_request #> '{protected_closure,pending}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,blockers}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,orphans}', '[]'::jsonb)
    ) as item(value)
  ) as wanted
    on wanted.id = flow.id and wanted.version = btrim(flow.version::text)
  where flow.user_id = v_actor and flow.state_code = 0
  order by flow.id, btrim(flow.version::text)
  for share of flow;

  perform 1
  from public.flowproperties as support
  join jsonb_array_elements(p_request->'support_snapshots') as item(value)
    on item.value->>'table' = 'flowproperties'
    and (item.value->>'id')::uuid = support.id
    and item.value->>'version' = btrim(support.version::text)
  where support.user_id = v_actor and support.state_code = 0
  order by support.id, btrim(support.version::text)
  for share of support;

  perform 1
  from public.unitgroups as support
  join jsonb_array_elements(p_request->'support_snapshots') as item(value)
    on item.value->>'table' = 'unitgroups'
    and (item.value->>'id')::uuid = support.id
    and item.value->>'version' = btrim(support.version::text)
  where support.user_id = v_actor and support.state_code = 0
  order by support.id, btrim(support.version::text)
  for share of support;

  perform 1
  from public.processes as process
  join jsonb_array_elements(p_request->'processes') as item(value)
    on (item.value->>'id')::uuid = process.id
    and item.value->>'version' = btrim(process.version::text)
  where process.user_id = v_actor and process.state_code = 0
  order by process.id, btrim(process.version::text)
  for share of process;

  v_mapping_count := jsonb_array_length(p_request->'mappings');
  for v_mapping in
    select item.value
    from jsonb_array_elements(p_request->'mappings')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_validation := util.dataset_flow_identity_validate_mapping(
      v_actor,
      v_mapping,
      p_request->'compatibility_policy',
      p_request->'support_snapshots',
      (v_mapping->>'ordinal')::integer
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_MAPPING_REJECTED',
        'status', 409, 'message', 'A public-flow identity mapping is invalid',
        'details', v_validation
      );
    end if;
  end loop;

  if (
    select count(*)
    from (
      select distinct item.value->>'mapping_id'
      from jsonb_array_elements(p_request->'mappings') as item(value)
    ) as unique_mapping
  ) <> v_mapping_count
    or (
      select count(distinct (
        (item.value #>> '{source,id}') || '@'
          || (item.value #>> '{source,version}')
      ))
      from jsonb_array_elements(p_request->'mappings') as item(value)
    ) <> v_mapping_count
    or (
      select min((item.value->>'ordinal')::integer) = 1
        and max((item.value->>'ordinal')::integer) = v_mapping_count
        and count(distinct (item.value->>'ordinal')::integer) = v_mapping_count
        and bool_and(
          (item.value->>'ordinal')::integer = item.ordinality::integer
        )
      from jsonb_array_elements(p_request->'mappings')
        with ordinality as item(value, ordinality)
    ) is not true then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_MAPPING_ORDINAL_MISMATCH',
      'status', 409, 'message', 'Mapping ordinals must be unique and contiguous'
    );
  end if;
  v_mapping_index := private.dataset_flow_identity_mapping_index_v2(
    p_request->'mappings'
  );

  v_process_count := jsonb_array_length(p_request->'processes');
  v_rewrite_count := 0;
  for v_manifest in
    select item.value
    from jsonb_array_elements(p_request->'processes')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_validation := util.dataset_flow_identity_dry_validate_process(
      v_actor, v_manifest, v_mapping_index,
      p_request->'compatibility_policy', p_request->'support_snapshots'
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false
      or v_manifest->>'pending_blocker_closure_sha256'
        is distinct from p_request->>'protected_closure_sha256' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_PROCESS_REJECTED',
        'status', 409, 'message', 'An owner-draft process baseline is invalid',
        'details', v_validation
      );
    end if;
    v_rewrite_count := v_rewrite_count
      + (v_manifest->>'rewrite_count')::integer;
  end loop;

  if (
    select min((item.value->>'ordinal')::integer) = 1
      and max((item.value->>'ordinal')::integer) = v_process_count
      and count(distinct (item.value->>'ordinal')::integer) = v_process_count
      and count(distinct (
        (item.value->>'id') || '@' || (item.value->>'version')
      )) = v_process_count
      and bool_and(
        (item.value->>'ordinal')::integer = item.ordinality::integer
      )
    from jsonb_array_elements(p_request->'processes')
      with ordinality as item(value, ordinality)
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_PROCESS_ORDINAL_MISMATCH',
      'status', 409, 'message', 'Process ordinals and identities must be unique'
    );
  end if;

  -- Prove that the compact process manifest is a complete closure of every
  -- currently approved source reference.  Exact locators remain sealed by
  -- each rewrite_set_sha256 and are independently checked by the process RPC,
  -- while these set-based counts reject omitted or phantom processes before
  -- the first business mutation can occur.
  with source_keys as (
    select
      mapping.value #>> '{source,id}' as source_id,
      mapping.value #>> '{source,version}' as source_version
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
  ), manifest as (
    select
      (item.value->>'id')::uuid as process_id,
      item.value->>'version' as process_version,
      (item.value->>'rewrite_count')::integer as expected_count
    from jsonb_array_elements(p_request->'processes') as item(value)
  ), approved_occurrences as (
    select
      process.id as process_id,
      btrim(process.version::text) as process_version
    from public.processes as process
    cross join lateral jsonb_array_elements(
      private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
    ) as exchange(value)
    join source_keys
      on source_keys.source_id = exchange.value #>>
          '{referenceToFlowDataSet,@refObjectId}'
      and source_keys.source_version = exchange.value #>>
          '{referenceToFlowDataSet,@version}'
    where process.user_id = v_actor and process.state_code = 0
  ), live_counts as (
    select
      manifest.process_id,
      manifest.process_version,
      manifest.expected_count,
      count(approved_occurrences.process_id)::integer as observed_count
    from manifest
    left join approved_occurrences using (process_id, process_version)
    group by
      manifest.process_id, manifest.process_version, manifest.expected_count
  )
  select
    (select count(*)::integer from approved_occurrences),
    (select count(*)::integer from live_counts
      where expected_count <> observed_count),
    (select count(*)::integer
      from approved_occurrences
      left join manifest using (process_id, process_version)
      where manifest.process_id is null)
  into
    v_approved_reference_count,
    v_process_reference_mismatch_count,
    v_unlisted_reference_count;

  if v_approved_reference_count <> v_rewrite_count
    or v_process_reference_mismatch_count <> 0
    or v_unlisted_reference_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_APPROVED_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Approved source references are omitted or mispartitioned',
      'approved_reference_count', v_approved_reference_count,
      'sealed_rewrite_count', v_rewrite_count,
      'process_mismatch_count', v_process_reference_mismatch_count,
      'unlisted_reference_count', v_unlisted_reference_count
    );
  end if;

  v_protected_proof := util.dataset_flow_identity_protected_closure(
    v_actor, p_request->'protected_closure'
  );
  if coalesce((v_protected_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_PROTECTED_CLOSURE_MISMATCH',
      'status', 409, 'message', 'Pending/blocker occurrence closure drifted',
      'details', v_protected_proof
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
    join jsonb_array_elements(
      coalesce(p_request #> '{protected_closure,pending}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,blockers}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,orphans}', '[]'::jsonb)
    ) as protected(value)
      on protected.value->>'source_id' = mapping.value #>> '{source,id}'
      and protected.value->>'source_version'
        = mapping.value #>> '{source,version}'
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_PARTITION_OVERLAP',
      'status', 409, 'message', 'Approved and protected source partitions overlap'
    );
  end if;

  with universe_entry as (
    select mapping.value #>> '{source,id}' as id,
      mapping.value #>> '{source,version}' as version
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
    union all
    select item.value->>'source_id', item.value->>'source_version'
    from jsonb_array_elements(
      coalesce(p_request #> '{protected_closure,pending}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,blockers}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,orphans}', '[]'::jsonb)
    ) as item(value)
  ), universe_count as (
    select count(*)::integer as raw_count,
      count(distinct id || '@' || version)::integer as distinct_count
    from universe_entry
  )
  select case
    when universe_count.raw_count = 305
      and universe_count.distinct_count = 305 then (
        select jsonb_agg(jsonb_build_object(
          'id', entry.id::uuid,
          'version', entry.version,
          'user_id', v_actor,
          'state_code', 0,
          'flow_type', 'Elementary flow'
        ) order by entry.id::uuid, entry.version)
        from universe_entry as entry
      )
    else null
  end
  into v_source_universe
  from universe_count;

  if v_source_universe is null
    or util.dataset_flow_identity_sha256(v_source_universe)
      is distinct from p_request->>'source_universe_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_SOURCE_UNIVERSE_MISMATCH',
      'status', 409,
      'message', 'Mappings and protected partitions must form exactly 305 distinct sources'
    );
  end if;
  v_universe_proof := util.dataset_flow_identity_source_universe(
    v_actor, v_source_universe, p_request->>'source_universe_sha256'
  );
  if coalesce((v_universe_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_SOURCE_UNIVERSE_LIVE_MISMATCH',
      'status', 409,
      'message', 'The live actor-owned elementary-flow universe is not exact',
      'details', v_universe_proof
    );
  end if;

  v_scope_id := gen_random_uuid();
  v_scope_proof_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    jsonb_build_object(
      'schema_version', 'dataset-flow-identity-scope-proof.v1',
      'scope_id', v_scope_id,
      'actor_user_id', v_actor,
      'environment', p_request->>'environment',
      'project_ref', p_request->>'project_ref',
      'operation_id', p_request->>'operation_id',
      'plan_sha256', p_request->>'plan_sha256',
      'freeze_sha256', p_request->>'freeze_sha256',
      'approval_identity_sha256', p_request->>'approval_identity_sha256',
      'support_snapshot_set_sha256',
        p_request->>'support_snapshot_set_sha256',
      'source_universe_sha256', p_request->>'source_universe_sha256',
      'mapping_set_sha256', p_request->>'mapping_set_sha256',
      'process_manifest_sha256', p_request->>'process_manifest_sha256',
      'protected_closure_sha256', p_request->>'protected_closure_sha256',
      'scope_request_sha256', v_scope_request_sha256
    )
  );

  insert into util.dataset_flow_identity_scopes (
    id, actor_user_id, actor_email, request_id, environment, project_ref,
    target_visibility, operation_id, plan_sha256, freeze_sha256,
    approval_identity_sha256, approval_text_sha256,
    toolchain_evidence_sha256, compatibility_policy,
    support_snapshot_set_sha256, support_snapshots,
    source_universe_sha256, source_universe, source_universe_count,
    mapping_set_sha256,
    process_manifest_sha256, protected_closure_sha256, protected_closure,
    scope_request_sha256, scope_proof_sha256, mapping_count, process_count,
    rewrite_count
  ) values (
    v_scope_id, v_actor, v_actor_email, (p_request->>'request_id')::uuid,
    p_request->>'environment', p_request->>'project_ref',
    p_request->>'target_visibility', p_request->>'operation_id',
    p_request->>'plan_sha256', p_request->>'freeze_sha256',
    p_request->>'approval_identity_sha256', p_request->>'approval_text_sha256',
    p_request->>'toolchain_evidence_sha256',
    p_request->'compatibility_policy',
    p_request->>'support_snapshot_set_sha256',
    p_request->'support_snapshots',
    p_request->>'source_universe_sha256', v_source_universe, 305,
    p_request->>'mapping_set_sha256',
    p_request->>'process_manifest_sha256',
    p_request->>'protected_closure_sha256', p_request->'protected_closure',
    v_scope_request_sha256, v_scope_proof_sha256, v_mapping_count,
    v_process_count, v_rewrite_count
  );

  insert into util.dataset_flow_identity_mappings (
    scope_id, ordinal, mapping_id, source_id, source_version,
    target_id, target_version, mapping
  )
  select
    v_scope_id,
    (item.value->>'ordinal')::integer,
    item.value->>'mapping_id',
    (item.value #>> '{source,id}')::uuid,
    item.value #>> '{source,version}',
    (item.value #>> '{target,id}')::uuid,
    item.value #>> '{target,version}',
    item.value
  from jsonb_array_elements(p_request->'mappings') as item(value);

  insert into util.dataset_flow_identity_process_ledger (
    scope_id, ordinal, process_id, process_version, manifest,
    process_template_sha256, process_intent_proof_sha256,
    rewrite_count, before_payload_sha256
  )
  select
    v_scope_id,
    (item.value->>'ordinal')::integer,
    (item.value->>'id')::uuid,
    item.value->>'version',
    item.value,
    item.value->>'process_template_sha256',
    util.dataset_flow_identity_restricted_sha256_v2(item.value),
    (item.value->>'rewrite_count')::integer,
    item.value->>'before_payload_sha256'
  from jsonb_array_elements(p_request->'processes') as item(value);

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null,
    jsonb_build_object(
      'record_type', 'scope_seal',
      'schema_version', v_schema_version,
      'scope_id', v_scope_id,
      'operation_id', p_request->>'operation_id',
      'plan_sha256', p_request->>'plan_sha256',
      'freeze_sha256', p_request->>'freeze_sha256',
      'approval_identity_sha256', p_request->>'approval_identity_sha256',
      'support_snapshot_set_sha256',
        p_request->>'support_snapshot_set_sha256',
      'source_universe_sha256', p_request->>'source_universe_sha256',
      'source_universe_count', 305,
      'scope_request_sha256', v_scope_request_sha256,
      'scope_proof_sha256', v_scope_proof_sha256,
      'mapping_count', v_mapping_count,
      'process_count', v_process_count,
      'rewrite_count', v_rewrite_count,
      'protected_closure_proof', v_protected_proof,
      'source_universe_proof', v_universe_proof,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_audit_id;

  return jsonb_build_object(
    'ok', true,
    'command', v_command,
    'schema_version', v_result_schema_version,
    'scope_id', v_scope_id,
    'operation_id', p_request->>'operation_id',
    'plan_sha256', p_request->>'plan_sha256',
    'scope_proof_sha256', v_scope_proof_sha256,
    'status', 'sealed',
    'process_count', v_process_count,
    'mapping_count', v_mapping_count,
    'support_snapshot_count', v_support_count,
    'source_universe_count', 305,
    'rewrite_count', v_rewrite_count,
    'next_ordinal', 1,
    'audit_id', v_audit_id::text,
    'replay', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_LOCK_BUSY', 'status', 409,
      'message', 'Scope seal could not acquire its bounded validation locks'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_CONCURRENT_CONFLICT', 'status', 409,
      'message', 'Another active scope already owns an exact process or plan'
    );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_scope_preflight_core_v1"("p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_scope_preflight_core_v1"("p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_scope_preflight_core_v1"("p_request" "jsonb") TO "api_internal_executor";
