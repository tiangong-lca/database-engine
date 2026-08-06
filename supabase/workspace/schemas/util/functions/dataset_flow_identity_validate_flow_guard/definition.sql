CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_validate_flow_guard"("p_actor" "uuid", "p_guard" "jsonb", "p_target" boolean, "p_support_snapshots" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_source_keys constant text[] := array[
    'id', 'version', 'user_id', 'state_code', 'modified_at',
    'payload_sha256', 'row_sha256', 'flow_type', 'flow_property_id',
    'flow_property_version', 'unit_group_id', 'unit_group_version',
    'category_path_sha256', 'source_trace_sha256'
  ];
  v_target_keys constant text[] := array[
    'id', 'version', 'user_id', 'state_code', 'modified_at',
    'payload_sha256', 'row_sha256', 'flow_type', 'flow_property_id',
    'flow_property_version', 'unit_group_id', 'unit_group_version',
    'category_path_sha256', 'reference'
  ];
  v_reference_keys constant text[] := array[
    '@refObjectId', '@type', '@uri', '@version',
    'common:shortDescription'
  ];
  v_flow public.flows%rowtype;
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_payload_sha256 text;
  v_row_sha256 text;
  v_category_sha256 text;
  v_flow_properties jsonb;
  v_reference_flow_property_internal_id text;
  v_reference_unit_internal_id text;
  v_claimed_fp_id uuid;
  v_claimed_fp_version text;
  v_claimed_ug_id uuid;
  v_claimed_ug_version text;
  v_fp_snapshot jsonb;
  v_ug_snapshot jsonb;
  v_support_validation jsonb;
begin
  if p_actor is null
    or jsonb_typeof(p_support_snapshots) <> 'array'
    or not private.dataset_flow_identity_exact_keys(
      p_guard,
      case when p_target then v_target_keys else v_source_keys end
    )
    or p_guard->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'user_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_guard->>'flow_property_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'unit_group_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'flow_property_version'
      !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_guard->>'unit_group_version'
      !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_guard->>'payload_sha256' !~ '^[a-f0-9]{64}$'
    or p_guard->>'row_sha256' !~ '^[a-f0-9]{64}$'
    or p_guard->>'category_path_sha256' !~ '^[a-f0-9]{64}$'
    or (not p_target
      and p_guard->>'source_trace_sha256' !~ '^[a-f0-9]{64}$')
    or p_guard->>'flow_type' <> 'Elementary flow'
    or jsonb_typeof(p_guard->'state_code') <> 'number'
    or (p_guard->>'state_code')::integer
      <> (case when p_target then 100 else 0 end)
    or (p_target and not private.dataset_flow_identity_exact_keys(
      p_guard->'reference', v_reference_keys
    )) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_SCHEMA_MISMATCH'
    );
  end if;

  begin
    perform (p_guard->>'modified_at')::timestamp with time zone;
    v_claimed_fp_id := (p_guard->>'flow_property_id')::uuid;
    v_claimed_fp_version := p_guard->>'flow_property_version';
    v_claimed_ug_id := (p_guard->>'unit_group_id')::uuid;
    v_claimed_ug_version := p_guard->>'unit_group_version';
  exception when others then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_VALUE_MISMATCH'
    );
  end;

  select flow.*
  into v_flow
  from public.flows as flow
  where flow.id = (p_guard->>'id')::uuid
    and btrim(flow.version::text) = p_guard->>'version';

  if v_flow.id is null
    or v_flow.json_ordered is null
    or v_flow.json is null
    or v_flow.json::jsonb is distinct from v_flow.json_ordered::jsonb
    or v_flow.user_id::text is distinct from p_guard->>'user_id'
    or v_flow.state_code is distinct from (p_guard->>'state_code')::integer
    or v_flow.modified_at is distinct from
      (p_guard->>'modified_at')::timestamp with time zone
    or (not p_target and v_flow.user_id is distinct from p_actor)
    or (p_target and (v_flow.user_id is null or v_flow.user_id = p_actor))
    or v_flow.json #>>
      '{flowDataSet,flowInformation,dataSetInformation,common:UUID}'
      is distinct from p_guard->>'id'
    or v_flow.json #>>
      '{flowDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from p_guard->>'version'
    or v_flow.json #>>
      '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      is distinct from 'Elementary flow' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_LIVE_MISMATCH'
    );
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(
    v_flow.json_ordered::jsonb
  );
  v_row_sha256 := private.dataset_flow_identity_row_sha256(
    v_flow.id,
    btrim(v_flow.version::text),
    v_flow.user_id,
    v_flow.state_code,
    v_flow.modified_at,
    v_payload_sha256
  );
  v_category_sha256 := util.dataset_flow_identity_sha256(coalesce(
    v_flow.json #>
      '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}',
    'null'::jsonb
  ));

  if v_payload_sha256 is distinct from p_guard->>'payload_sha256'
    or v_row_sha256 is distinct from p_guard->>'row_sha256'
    or v_category_sha256 is distinct from p_guard->>'category_path_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_HASH_DRIFT'
    );
  end if;

  v_flow_properties := v_flow.json #>
    '{flowDataSet,flowProperties,flowProperty}';
  if jsonb_typeof(v_flow_properties) = 'object' then
    v_flow_properties := jsonb_build_array(v_flow_properties);
  end if;
  v_reference_flow_property_internal_id := v_flow.json #>>
    '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  if nullif(v_reference_flow_property_internal_id, '') is null
    or jsonb_typeof(v_flow_properties) <> 'array'
    or not exists (
      select 1
      from jsonb_array_elements(v_flow_properties) as fp(value)
      where fp.value->>'@dataSetInternalID'
          = v_reference_flow_property_internal_id
        and fp.value #>>
        '{referenceToFlowPropertyDataSet,@refObjectId}'
          = v_claimed_fp_id::text
        and fp.value #>>
          '{referenceToFlowPropertyDataSet,@version}'
          = v_claimed_fp_version
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_REFERENCE_FLOW_PROPERTY_MISMATCH'
    );
  end if;

  select flowproperty.*
  into v_flowproperty
  from public.flowproperties as flowproperty
  where flowproperty.id = v_claimed_fp_id
    and btrim(flowproperty.version::text) = v_claimed_fp_version
    and (
      flowproperty.state_code = 100
      or (flowproperty.user_id = p_actor and flowproperty.state_code = 0)
    );

  if v_flowproperty.id is null
    or v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@refObjectId}'
      is distinct from v_claimed_ug_id::text
    or v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@version}'
      is distinct from v_claimed_ug_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_REFERENCE_UNIT_GROUP_MISMATCH'
    );
  end if;

  select snapshot.value
  into v_fp_snapshot
  from jsonb_array_elements(p_support_snapshots) as snapshot(value)
  where snapshot.value->>'table' = 'flowproperties'
    and snapshot.value->>'id' = v_claimed_fp_id::text
    and snapshot.value->>'version' = v_claimed_fp_version;
  v_support_validation := util.dataset_flow_identity_validate_support_snapshot(
    p_actor, v_fp_snapshot
  );
  if v_fp_snapshot is null
    or coalesce((v_support_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_PROPERTY_SNAPSHOT_MISMATCH',
      'details', v_support_validation
    );
  end if;

  select unitgroup.*
  into v_unitgroup
  from public.unitgroups as unitgroup
  where unitgroup.id = v_claimed_ug_id
    and btrim(unitgroup.version::text) = v_claimed_ug_version
    and (
      unitgroup.state_code = 100
      or (unitgroup.user_id = p_actor and unitgroup.state_code = 0)
    );
  v_reference_unit_internal_id := v_unitgroup.json #>>
    '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}';

  if v_unitgroup.id is null
    or v_unitgroup.json #>>
      '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:UUID}'
      is distinct from v_claimed_ug_id::text
    or v_unitgroup.json #>>
      '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from v_claimed_ug_version
    or nullif(v_reference_unit_internal_id, '') is null
    or not exists (
      select 1
      from jsonb_array_elements(
        case jsonb_typeof(v_unitgroup.json #>
          '{unitGroupDataSet,units,unit}')
          when 'array' then v_unitgroup.json #>
            '{unitGroupDataSet,units,unit}'
          when 'object' then jsonb_build_array(v_unitgroup.json #>
            '{unitGroupDataSet,units,unit}')
          else '[]'::jsonb
        end
      ) as unit_item(value)
      where unit_item.value->>'@dataSetInternalID'
          = v_reference_unit_internal_id
        and (unit_item.value->>'meanValue')::numeric = 1
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_REFERENCE_UNIT_MISMATCH'
    );
  end if;

  select snapshot.value
  into v_ug_snapshot
  from jsonb_array_elements(p_support_snapshots) as snapshot(value)
  where snapshot.value->>'table' = 'unitgroups'
    and snapshot.value->>'id' = v_claimed_ug_id::text
    and snapshot.value->>'version' = v_claimed_ug_version;
  v_support_validation := util.dataset_flow_identity_validate_support_snapshot(
    p_actor, v_ug_snapshot
  );
  if v_ug_snapshot is null
    or coalesce((v_support_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_UNIT_GROUP_SNAPSHOT_MISMATCH',
      'details', v_support_validation
    );
  end if;

  if p_target and (
    p_guard #>> '{reference,@refObjectId}' is distinct from v_flow.id::text
    or p_guard #>> '{reference,@version}'
      is distinct from btrim(v_flow.version::text)
    or p_guard #>> '{reference,@type}' is distinct from 'flow data set'
    or p_guard #>> '{reference,@uri}' is distinct from
      '../flows/' || v_flow.id::text || '_'
        || btrim(v_flow.version::text) || '.xml'
    or jsonb_typeof(p_guard #> '{reference,common:shortDescription}')
      not in ('string', 'object', 'array')
    or not (
      private.dataset_flow_identity_text_values(
        p_guard #> '{reference,common:shortDescription}'
      ) && private.dataset_flow_identity_text_values(
        v_flow.json #>
          '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
      )
    )
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_TARGET_REFERENCE_MISMATCH'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'payload_sha256', v_payload_sha256,
    'row_sha256', v_row_sha256,
    'category_path_sha256', v_category_sha256
  );
exception when others then
  return jsonb_build_object(
    'ok', false,
    'code', 'FLOW_IDENTITY_FLOW_GUARD_INVALID',
    'sqlstate', sqlstate,
    'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_validate_flow_guard"("p_actor" "uuid", "p_guard" "jsonb", "p_target" boolean, "p_support_snapshots" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_validate_flow_guard"("p_actor" "uuid", "p_guard" "jsonb", "p_target" boolean, "p_support_snapshots" "jsonb") FROM PUBLIC;
