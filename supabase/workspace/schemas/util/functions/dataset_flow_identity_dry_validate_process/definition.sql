CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_dry_validate_process"("p_actor" "uuid", "p_manifest" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_support_snapshots" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_rewrite_keys constant text[] := array[
    'ordinal', 'exchange_index', 'internal_id', 'direction', 'mapping_id',
    'source_reference', 'target_reference', 'before_reference_sha256',
    'after_reference_sha256'
  ];
  v_reference_keys constant text[] := array[
    '@refObjectId', '@type', '@uri', '@version',
    'common:shortDescription'
  ];
  v_process_validation jsonb;
  v_mapping_validation jsonb;
  v_process public.processes%rowtype;
  v_mapping jsonb;
  v_rewrite jsonb;
  v_before_payload jsonb;
  v_before_exchanges jsonb;
  v_after_exchanges jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_before_reference jsonb;
  v_after_reference jsonb;
  v_collision jsonb;
  v_desired_payload jsonb;
  v_after_payload_sha256 text;
  v_after_exchange_sha256 text;
  v_mapping_array jsonb;
  v_mapping_by_id jsonb;
  v_mapping_by_source jsonb;
  v_indexed boolean := false;
begin
  if jsonb_typeof(p_mappings) = 'array' then
    v_mapping_array := p_mappings;
  elsif jsonb_typeof(p_mappings) = 'object'
    and private.dataset_flow_identity_exact_keys(p_mappings, array[
      'schema_version', 'mapping_count', 'mapping_guard_set_sha256',
      'mappings', 'by_ordinal', 'by_id', 'by_source'
    ])
    and p_mappings->>'schema_version'
      = 'dataset-flow-identity-mapping-index.v2'
    and jsonb_typeof(p_mappings->'mapping_count') = 'number'
    and (p_mappings->>'mapping_count')::integer
      = jsonb_array_length(p_mappings->'mappings')
    and p_mappings->>'mapping_guard_set_sha256' ~ '^[a-f0-9]{64}$'
    and jsonb_typeof(p_mappings->'mappings') = 'array'
    and jsonb_typeof(p_mappings->'by_ordinal') = 'object'
    and jsonb_typeof(p_mappings->'by_id') = 'object'
    and jsonb_typeof(p_mappings->'by_source') = 'object' then
    v_mapping_array := p_mappings->'mappings';
    v_mapping_by_id := p_mappings->'by_id';
    v_mapping_by_source := p_mappings->'by_source';
    v_indexed := true;
  end if;
  if p_actor is null
    or v_mapping_array is null
    or jsonb_typeof(p_support_snapshots) <> 'array' then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_PROCESS_INVALID_REQUEST'
    );
  end if;
  v_process_validation := util.dataset_flow_identity_validate_process_guard(
    p_actor, p_manifest
  );
  if coalesce((v_process_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_PROCESS_BASELINE_REJECTED',
      'details', v_process_validation
    );
  end if;
  if (
    select count(*) = (p_manifest->>'rewrite_count')::integer
      and min((rewrite.value->>'ordinal')::integer) = 1
      and max((rewrite.value->>'ordinal')::integer)
        = (p_manifest->>'rewrite_count')::integer
      and count(distinct (rewrite.value->>'ordinal')::integer)
        = (p_manifest->>'rewrite_count')::integer
      and count(distinct (rewrite.value->>'exchange_index')::integer)
        = (p_manifest->>'rewrite_count')::integer
      and bool_and(
        (rewrite.value->>'ordinal')::integer = rewrite.ordinality::integer
      )
    from jsonb_array_elements(p_manifest->'rewrites')
      with ordinality as rewrite(value, ordinality)
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_REWRITE_ORDINAL_MISMATCH'
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = (p_manifest->>'id')::uuid
    and btrim(process.version::text) = p_manifest->>'version'
    and process.user_id = p_actor and process.state_code = 0;
  v_before_payload := v_process.json_ordered::jsonb;
  v_before_exchanges := private.dataset_flow_identity_exchanges(v_before_payload);
  v_after_exchanges := v_before_exchanges;

  for v_rewrite in
    select rewrite.value
    from jsonb_array_elements(p_manifest->'rewrites')
      with ordinality as rewrite(value, ordinality)
    order by rewrite.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(v_rewrite, v_rewrite_keys)
      or v_rewrite->>'ordinal' !~ '^[1-9][0-9]*$'
      or v_rewrite->>'exchange_index' !~ '^(0|[1-9][0-9]*)$'
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
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_REWRITE_SCHEMA_MISMATCH'
      );
    end if;

    if v_indexed then
      v_mapping := v_mapping_by_id->(v_rewrite->>'mapping_id');
    else
      select mapping.value
      into v_mapping
      from jsonb_array_elements(v_mapping_array) as mapping(value)
      where mapping.value->>'mapping_id' = v_rewrite->>'mapping_id';
    end if;
    if v_mapping is null then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_MAPPING_NOT_SEALED'
      );
    end if;
    if not v_indexed then
      v_mapping_validation := util.dataset_flow_identity_validate_mapping(
        p_actor, v_mapping, p_policy, p_support_snapshots,
        (v_mapping->>'ordinal')::integer
      );
      if coalesce((v_mapping_validation->>'ok')::boolean, false) is false then
        return jsonb_build_object(
          'ok', false, 'code', 'FLOW_IDENTITY_DRY_MAPPING_REJECTED',
          'details', v_mapping_validation
        );
      end if;
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
        is distinct from v_mapping #>> '{source,id}'
      or v_before_reference->>'@version'
        is distinct from v_mapping #>> '{source,version}'
      or v_rewrite->'target_reference'
        is distinct from v_mapping #> '{target,reference}'
      or util.dataset_flow_identity_sha256(v_rewrite->'target_reference')
        is distinct from v_rewrite->>'after_reference_sha256' then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_EXCHANGE_LOCATOR_DRIFT'
      );
    end if;

    v_after_exchange := jsonb_set(
      v_before_exchange,
      '{referenceToFlowDataSet}',
      (v_before_exchange->'referenceToFlowDataSet')
        || v_rewrite->'target_reference',
      false
    );
    v_after_reference := private.dataset_flow_identity_reference(v_after_exchange);
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
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_FIVE_FIELD_BOUNDARY_FAILED'
      );
    end if;
    v_after_exchanges := jsonb_set(
      v_after_exchanges,
      array[(v_rewrite->>'exchange_index')::integer::text],
      v_after_exchange,
      false
    );
  end loop;

  v_collision := private.dataset_flow_identity_collision_ledger(
    v_after_exchanges, p_manifest->'rewrites'
  );
  v_desired_payload := private.dataset_flow_identity_replace_exchanges(
    v_before_payload, v_after_exchanges
  );
  v_after_payload_sha256 := util.dataset_flow_identity_sha256(v_desired_payload);
  v_after_exchange_sha256 := util.dataset_flow_identity_sha256(v_after_exchanges);
  if v_collision is distinct from p_manifest->'collision_ledger'
    or v_after_payload_sha256
      is distinct from p_manifest->>'desired_payload_sha256'
    or v_after_exchange_sha256
      is distinct from p_manifest->>'desired_exchange_set_sha256'
    or jsonb_array_length(v_after_exchanges)
      <> jsonb_array_length(v_before_exchanges) then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_DESIRED_CLOSURE_MISMATCH'
    );
  end if;
  if v_indexed then
    if exists (
      select 1
      from jsonb_array_elements(v_after_exchanges) as exchange(value)
      where v_mapping_by_source ? (
        (exchange.value #>> '{referenceToFlowDataSet,@refObjectId}') || '@'
          || (exchange.value #>> '{referenceToFlowDataSet,@version}')
      )
    ) then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_DESIRED_CLOSURE_MISMATCH'
      );
    end if;
  elsif exists (
    select 1
    from jsonb_array_elements(v_mapping_array) as mapping(value)
    join lateral jsonb_array_elements(v_after_exchanges) as exchange(value)
      on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
        = mapping.value #>> '{source,id}'
      and exchange.value #>> '{referenceToFlowDataSet,@version}'
        = mapping.value #>> '{source,version}'
  ) then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_DESIRED_CLOSURE_MISMATCH'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'before_payload_sha256', p_manifest->>'before_payload_sha256',
    'after_payload_sha256', v_after_payload_sha256,
    'after_exchange_set_sha256', v_after_exchange_sha256,
    'desired_payload', v_desired_payload,
    'desired_exchanges', v_after_exchanges,
    'collision_ledger', v_collision
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_DRY_PROCESS_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_dry_validate_process"("p_actor" "uuid", "p_manifest" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_support_snapshots" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_dry_validate_process"("p_actor" "uuid", "p_manifest" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_support_snapshots" "jsonb") FROM PUBLIC;
