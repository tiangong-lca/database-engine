CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_build_process_v2"("p_actor" "uuid", "p_intent" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_supports" "jsonb", "p_protected_closure_sha256" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_process public.processes%rowtype;
  v_before_payload jsonb;
  v_before_exchanges jsonb;
  v_after_exchanges jsonb;
  v_desired_payload jsonb;
  v_thin jsonb;
  v_mapping jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_source_reference jsonb;
  v_target_reference jsonb;
  v_rewrite jsonb;
  v_rewrites jsonb := '[]'::jsonb;
  v_collision jsonb;
  v_snapshot jsonb;
  v_before_payload_sha256 text;
  v_before_exchange_sha256 text;
  v_desired_payload_sha256 text;
  v_desired_exchange_sha256 text;
  v_manifest jsonb;
  v_validation jsonb;
begin
  if jsonb_typeof(p_mappings) <> 'object'
    or not private.dataset_flow_identity_exact_keys(p_mappings, array[
      'schema_version', 'mapping_count', 'mapping_guard_set_sha256',
      'mappings', 'by_ordinal', 'by_id', 'by_source'
    ])
    or p_mappings->>'schema_version'
      <> 'dataset-flow-identity-mapping-index.v2'
    or jsonb_typeof(p_mappings->'by_ordinal') <> 'object'
    or jsonb_typeof(p_mappings->'by_id') <> 'object'
    or jsonb_typeof(p_mappings->'by_source') <> 'object'
    or not private.dataset_flow_identity_exact_keys(p_intent, array[
      'ordinal', 'id', 'version', 'rewrites', 'process_schema'
    ])
    or jsonb_typeof(p_intent->'ordinal') <> 'number'
    or (p_intent->>'ordinal')::integer <= 0
    or p_intent->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_intent->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(p_intent->'rewrites') <> 'array'
    or jsonb_array_length(p_intent->'rewrites') <= 0
    or not private.dataset_flow_identity_exact_keys(
      p_intent->'process_schema', array['status', 'evidence_sha256']
    )
    or p_intent #>> '{process_schema,status}' <> 'pass'
    or p_intent #>> '{process_schema,evidence_sha256}'
      !~ '^[a-f0-9]{64}$' then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_INTENT_SCHEMA_MISMATCH';
  end if;

  select process.* into v_process
  from public.processes as process
  where process.id = (p_intent->>'id')::uuid
    and btrim(process.version::text) = p_intent->>'version'
    and process.user_id = p_actor and process.state_code = 0;
  if v_process.id is null
    or v_process.json is null or v_process.json_ordered is null
    or v_process.json::jsonb is distinct from v_process.json_ordered::jsonb then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_LIVE_MISMATCH';
  end if;
  v_before_payload := v_process.json_ordered::jsonb;
  v_before_exchanges := private.dataset_flow_identity_exchanges(v_before_payload);
  v_after_exchanges := v_before_exchanges;
  if v_before_exchanges is null then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_EXCHANGES_INVALID';
  end if;

  for v_thin in
    select item.value
    from jsonb_array_elements(p_intent->'rewrites')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(v_thin, array[
        'ordinal', 'exchange_index', 'internal_id', 'direction',
        'mapping_ordinal'
      ])
      or jsonb_typeof(v_thin->'ordinal') <> 'number'
      or (v_thin->>'ordinal')::integer <= 0
      or jsonb_typeof(v_thin->'exchange_index') <> 'number'
      or (v_thin->>'exchange_index')::integer < 0
      or (v_thin->>'exchange_index')::integer
        >= jsonb_array_length(v_before_exchanges)
      or jsonb_typeof(v_thin->'mapping_ordinal') <> 'number'
      or (v_thin->>'mapping_ordinal')::integer <= 0
      or nullif(v_thin->>'internal_id', '') is null
      or v_thin->>'direction' not in ('Input', 'Output') then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_REWRITE_INTENT_MISMATCH';
    end if;
    v_mapping := p_mappings #> array[
      'by_ordinal', (v_thin->>'mapping_ordinal')::integer::text
    ];
    if v_mapping is null then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_REWRITE_MAPPING_MISSING';
    end if;
    v_before_exchange := v_after_exchanges->(v_thin->>'exchange_index')::integer;
    v_source_reference := private.dataset_flow_identity_reference(v_before_exchange);
    v_target_reference := v_mapping #> '{target,reference}';
    if v_before_exchange->>'@dataSetInternalID'
        is distinct from v_thin->>'internal_id'
      or v_before_exchange->>'exchangeDirection'
        is distinct from v_thin->>'direction'
      or v_source_reference->>'@refObjectId'
        is distinct from v_mapping #>> '{source,id}'
      or v_source_reference->>'@version'
        is distinct from v_mapping #>> '{source,version}'
      or not private.dataset_flow_identity_short_description_v2(
        v_source_reference->'common:shortDescription'
      )
      or not private.dataset_flow_identity_short_description_v2(
        v_target_reference->'common:shortDescription'
      ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_REWRITE_LOCATOR_DRIFT';
    end if;
    v_rewrite := jsonb_build_object(
      'ordinal', v_thin->'ordinal',
      'exchange_index', v_thin->'exchange_index',
      'internal_id', v_thin->>'internal_id',
      'direction', v_thin->>'direction',
      'mapping_id', v_mapping->>'mapping_id',
      'source_reference', v_source_reference,
      'target_reference', v_target_reference,
      'before_reference_sha256',
        util.dataset_flow_identity_sha256(v_source_reference),
      'after_reference_sha256',
        util.dataset_flow_identity_sha256(v_target_reference)
    );
    v_rewrites := v_rewrites || jsonb_build_array(v_rewrite);
    v_after_exchange := jsonb_set(
      v_before_exchange,
      '{referenceToFlowDataSet}',
      (v_before_exchange->'referenceToFlowDataSet') || v_target_reference,
      false
    );
    if v_after_exchange - 'referenceToFlowDataSet'
        is distinct from v_before_exchange - 'referenceToFlowDataSet'
      or (v_after_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription'
        is distinct from
        (v_before_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription' then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_FIVE_FIELD_BOUNDARY_FAILED';
    end if;
    v_after_exchanges := jsonb_set(
      v_after_exchanges,
      array[(v_thin->>'exchange_index')::integer::text],
      v_after_exchange,
      false
    );
  end loop;
  if jsonb_array_length(v_rewrites)
      <> jsonb_array_length(p_intent->'rewrites') then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_REWRITE_COUNT_MISMATCH';
  end if;

  v_collision := private.dataset_flow_identity_collision_ledger(
    v_after_exchanges, v_rewrites
  );
  v_desired_payload := private.dataset_flow_identity_replace_exchanges(
    v_before_payload, v_after_exchanges
  );
  v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  if v_desired_payload is null or v_snapshot is null then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_DESIRED_OR_DERIVATIVE_INVALID';
  end if;
  v_before_payload_sha256 := util.dataset_flow_identity_sha256(v_before_payload);
  v_before_exchange_sha256 := util.dataset_flow_identity_sha256(v_before_exchanges);
  v_desired_payload_sha256 := util.dataset_flow_identity_sha256(v_desired_payload);
  v_desired_exchange_sha256 := util.dataset_flow_identity_sha256(v_after_exchanges);
  v_manifest := jsonb_build_object(
    'ordinal', p_intent->'ordinal',
    'id', v_process.id,
    'version', btrim(v_process.version::text),
    'user_id', v_process.user_id,
    'state_code', v_process.state_code,
    'modified_at', v_process.modified_at,
    'model_id', v_process.model_id,
    'rule_verification', v_process.rule_verification,
    'before_row_sha256', util.dataset_flow_identity_sha256(jsonb_build_object(
      'id', v_process.id,
      'version', btrim(v_process.version::text),
      'user_id', v_process.user_id,
      'state_code', v_process.state_code,
      'modified_at', v_process.modified_at,
      'model_id', v_process.model_id,
      'rule_verification', v_process.rule_verification,
      'payload_sha256', v_before_payload_sha256
    )),
    'before_payload_sha256', v_before_payload_sha256,
    'before_exchange_set_sha256', v_before_exchange_sha256,
    'before_exchange_count', jsonb_array_length(v_before_exchanges),
    'desired_payload_sha256', v_desired_payload_sha256,
    'desired_exchange_set_sha256', v_desired_exchange_sha256,
    'rewrite_count', jsonb_array_length(v_rewrites),
    'rewrite_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_rewrites),
    'rewrites', v_rewrites,
    'collision_ledger', v_collision,
    'collision_ledger_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_collision),
    'derivative_baseline_snapshot_sha256', v_snapshot->>'snapshot_sha256',
    'process_schema', p_intent->'process_schema',
    'pending_blocker_closure_sha256', p_protected_closure_sha256
  );
  v_manifest := v_manifest || jsonb_build_object(
    'process_template_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_manifest)
  );
  v_validation := util.dataset_flow_identity_dry_validate_process(
    p_actor, v_manifest, p_mappings, p_policy, p_supports
  );
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_DRY_VALIDATION_FAILED';
  end if;
  return v_manifest;
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_build_process_v2"("p_actor" "uuid", "p_intent" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_supports" "jsonb", "p_protected_closure_sha256" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_build_process_v2"("p_actor" "uuid", "p_intent" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_supports" "jsonb", "p_protected_closure_sha256" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_build_process_v2"("p_actor" "uuid", "p_intent" "jsonb", "p_mappings" "jsonb", "p_policy" "jsonb", "p_supports" "jsonb", "p_protected_closure_sha256" "text") TO "api_internal_executor";
