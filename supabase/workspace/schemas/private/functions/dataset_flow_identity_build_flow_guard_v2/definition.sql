CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_build_flow_guard_v2"("p_actor" "uuid", "p_endpoint" "jsonb", "p_target" boolean) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_flow public.flows%rowtype;
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_flow_properties jsonb;
  v_reference_internal_id text;
  v_fp_id uuid;
  v_fp_version text;
  v_ug_id uuid;
  v_ug_version text;
  v_reference_unit_internal_id text;
  v_payload_sha256 text;
  v_category_sha256 text;
  v_row_sha256 text;
  v_guard jsonb;
  v_reference jsonb;
  v_expected_uri text;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(
      p_endpoint,
      case when p_target
        then array['id', 'version', 'reference']
        else array['id', 'version', 'source_trace_sha256']
      end
    )
    or p_endpoint->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_endpoint->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or (not p_target
      and p_endpoint->>'source_trace_sha256' !~ '^[a-f0-9]{64}$') then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_ENDPOINT_SCHEMA_MISMATCH';
  end if;

  if p_target then
    v_reference := p_endpoint->'reference';
    v_expected_uri := '../flows/' || (p_endpoint->>'id') || '_'
      || (p_endpoint->>'version') || '.xml';
    if not private.dataset_flow_identity_exact_keys(
        v_reference,
        array[
          '@refObjectId', '@type', '@uri', '@version',
          'common:shortDescription'
        ]
      )
      or exists (
        select 1
        from unnest(array[
          '@refObjectId', '@type', '@uri', '@version'
        ]) as field(name)
        where jsonb_typeof(v_reference->field.name) <> 'string'
      )
      or v_reference->>'@refObjectId' <> p_endpoint->>'id'
      or v_reference->>'@version' <> p_endpoint->>'version'
      or v_reference->>'@type' <> 'flow data set'
      or v_reference->>'@uri' <> v_expected_uri
      or not private.dataset_flow_identity_short_description_v2(
        v_reference->'common:shortDescription'
      ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_TARGET_REFERENCE_MISMATCH';
    end if;
  end if;

  select flow.* into v_flow
  from public.flows as flow
  where flow.id = (p_endpoint->>'id')::uuid
    and btrim(flow.version::text) = p_endpoint->>'version';

  if v_flow.id is null
    or v_flow.json is null or v_flow.json_ordered is null
    or v_flow.json::jsonb is distinct from v_flow.json_ordered::jsonb
    or (p_target and v_flow.state_code <> 100)
    or (not p_target and v_flow.state_code <> 0)
    or (p_target and (v_flow.user_id is null or v_flow.user_id = p_actor))
    or (not p_target and v_flow.user_id is distinct from p_actor)
    or v_flow.json #>>
      '{flowDataSet,flowInformation,dataSetInformation,common:UUID}'
      is distinct from p_endpoint->>'id'
    or v_flow.json #>>
      '{flowDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from p_endpoint->>'version'
    or v_flow.json #>>
      '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      is distinct from 'Elementary flow' then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_FLOW_LIVE_MISMATCH';
  end if;

  v_flow_properties := v_flow.json #> '{flowDataSet,flowProperties,flowProperty}';
  if jsonb_typeof(v_flow_properties) = 'object' then
    v_flow_properties := jsonb_build_array(v_flow_properties);
  end if;
  v_reference_internal_id := v_flow.json #>>
    '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  if jsonb_typeof(v_flow_properties) <> 'array'
    or nullif(v_reference_internal_id, '') is null then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_FLOW_PROPERTY_MISSING';
  end if;

  select
    (item.value #>> '{referenceToFlowPropertyDataSet,@refObjectId}')::uuid,
    item.value #>> '{referenceToFlowPropertyDataSet,@version}'
  into v_fp_id, v_fp_version
  from jsonb_array_elements(v_flow_properties) as item(value)
  where item.value->>'@dataSetInternalID' = v_reference_internal_id;

  select support.* into v_flowproperty
  from public.flowproperties as support
  where support.id = v_fp_id
    and btrim(support.version::text) = v_fp_version
    and (
      (
        p_target
        and support.state_code = 100
        and support.user_id is not null
        and support.user_id <> p_actor
      )
      or (
        not p_target
        and (
          support.state_code = 100
          or (support.user_id = p_actor and support.state_code = 0)
        )
      )
    );
  if v_flowproperty.id is null
    or v_flowproperty.json is null or v_flowproperty.json_ordered is null
    or v_flowproperty.json::jsonb
      is distinct from v_flowproperty.json_ordered::jsonb then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_FLOW_PROPERTY_INVALID';
  end if;

  begin
    v_ug_id := (v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@refObjectId}')::uuid;
    v_ug_version := v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@version}';
  exception when others then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_UNIT_GROUP_REFERENCE_INVALID';
  end;

  select support.* into v_unitgroup
  from public.unitgroups as support
  where support.id = v_ug_id
    and btrim(support.version::text) = v_ug_version
    and (
      (
        p_target
        and support.state_code = 100
        and support.user_id is not null
        and support.user_id <> p_actor
      )
      or (
        not p_target
        and (
          support.state_code = 100
          or (support.user_id = p_actor and support.state_code = 0)
        )
      )
    );
  v_reference_unit_internal_id := v_unitgroup.json #>>
    '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}';
  if v_unitgroup.id is null
    or v_unitgroup.json is null or v_unitgroup.json_ordered is null
    or v_unitgroup.json::jsonb is distinct from v_unitgroup.json_ordered::jsonb
    or nullif(v_reference_unit_internal_id, '') is null
    or not exists (
      select 1
      from jsonb_array_elements(
        case jsonb_typeof(v_unitgroup.json #> '{unitGroupDataSet,units,unit}')
          when 'array' then v_unitgroup.json #> '{unitGroupDataSet,units,unit}'
          when 'object' then jsonb_build_array(
            v_unitgroup.json #> '{unitGroupDataSet,units,unit}'
          )
          else '[]'::jsonb
        end
      ) as unit_item(value)
      where unit_item.value->>'@dataSetInternalID'
          = v_reference_unit_internal_id
        and (unit_item.value->>'meanValue')::numeric = 1
    ) then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_REFERENCE_UNIT_INVALID';
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(
    v_flow.json_ordered::jsonb
  );
  v_category_sha256 := util.dataset_flow_identity_sha256(coalesce(
    v_flow.json #>
      '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}',
    'null'::jsonb
  ));
  v_row_sha256 := private.dataset_flow_identity_row_sha256(
    v_flow.id, btrim(v_flow.version::text), v_flow.user_id,
    v_flow.state_code, v_flow.modified_at, v_payload_sha256
  );
  v_guard := jsonb_build_object(
    'id', v_flow.id,
    'version', btrim(v_flow.version::text),
    'user_id', v_flow.user_id,
    'state_code', v_flow.state_code,
    'modified_at', v_flow.modified_at,
    'payload_sha256', v_payload_sha256,
    'row_sha256', v_row_sha256,
    'flow_type', 'Elementary flow',
    'flow_property_id', v_fp_id,
    'flow_property_version', v_fp_version,
    'unit_group_id', v_ug_id,
    'unit_group_version', v_ug_version,
    'category_path_sha256', v_category_sha256
  );
  if p_target then
    if not (
      private.dataset_flow_identity_text_values(
        v_reference->'common:shortDescription'
      ) && private.dataset_flow_identity_text_values(
        v_flow.json #>
          '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
      )
    ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_TARGET_NAME_MISMATCH';
    end if;
    return v_guard || jsonb_build_object('reference', v_reference);
  end if;
  return v_guard || jsonb_build_object(
    'source_trace_sha256', p_endpoint->>'source_trace_sha256'
  );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_build_flow_guard_v2"("p_actor" "uuid", "p_endpoint" "jsonb", "p_target" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_build_flow_guard_v2"("p_actor" "uuid", "p_endpoint" "jsonb", "p_target" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_build_flow_guard_v2"("p_actor" "uuid", "p_endpoint" "jsonb", "p_target" boolean) TO "api_internal_executor";
