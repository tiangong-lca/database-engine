CREATE OR REPLACE FUNCTION "private"."portal_exchange_support_v1"("p_process_state" integer, "p_process_json" "jsonb", "p_exchange" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_process_capabilities jsonb;
  v_internal_id text := btrim(coalesce(p_exchange ->> '@dataSetInternalID', ''));
  v_amount text;
  v_direction text;
  v_flow_reference jsonb := p_exchange -> 'referenceToFlowDataSet';
  v_flow_id_text text;
  v_flow_version text;
  v_flow_id uuid;
  v_flow_json jsonb;
  v_flow_state integer;
  v_flow_type text;
  v_exchange_kind text;
  v_flow_property_internal text;
  v_flow_property_item jsonb;
  v_flow_property_reference jsonb;
  v_flow_property_id_text text;
  v_flow_property_version text;
  v_flow_property_id uuid;
  v_flow_property_json jsonb;
  v_flow_property_state integer;
  v_unit_group_reference jsonb;
  v_unit_group_id_text text;
  v_unit_group_version text;
  v_unit_group_id uuid;
  v_unit_group_json jsonb;
  v_unit_group_state integer;
  v_unit_internal text;
  v_unit_item jsonb;
  v_unit text;
  v_unit_factor text;
  v_flow_factor text;
  v_classifications jsonb;
  v_uncertainty_type text;
  v_minimum text;
  v_maximum text;
  v_row jsonb;
  v_match_count integer;
begin
  v_process_capabilities := private.portal_capabilities_v1('process', p_process_state, p_process_json);
  if coalesce((v_process_capabilities ->> 'exchangesVisible')::boolean, false) is not true
     or v_internal_id !~ '^(0|[1-9][0-9]{0,5})$' then
    return null;
  end if;

  v_amount := private.portal_canonical_decimal_v1(
    coalesce(p_exchange ->> 'resultingAmount', p_exchange ->> 'meanAmount')
  );
  v_direction := case lower(btrim(coalesce(p_exchange ->> 'exchangeDirection', '')))
    when 'input' then 'input'
    when 'output' then 'output'
    else null
  end;
  v_flow_id_text := lower(btrim(coalesce(v_flow_reference ->> '@refObjectId', '')));
  v_flow_version := btrim(coalesce(v_flow_reference ->> '@version', ''));
  if v_amount is null
     or v_direction is null
     or v_flow_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_flow_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_flow_id := v_flow_id_text::uuid;

  select row.json, row.state_code
  into v_flow_json, v_flow_state
  from public.flows as row
  where row.id = v_flow_id
    and row.version::text = v_flow_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
  limit 1;
  if v_flow_json is null
     or coalesce((private.portal_support_capabilities_v1('flow', v_flow_state) ->> 'exchangesVisible')::boolean, false) is not true then
    return null;
  end if;

  v_flow_type := private.portal_flow_kind_v1(
    private.portal_scalar_text_v1(
      v_flow_json #> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
    )
  );
  v_exchange_kind := case v_flow_type
    when 'product' then 'technosphere'
    when 'elementary' then 'elementary'
    when 'waste' then 'waste'
    else null
  end;
  if v_exchange_kind is null then
    return null;
  end if;

  v_flow_property_internal := v_flow_json #>> '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  if v_flow_property_internal !~ '^(0|[1-9][0-9]{0,4})$' then
    return null;
  end if;
  select count(*), (jsonb_agg(item) -> 0)
  into v_match_count, v_flow_property_item
  from private.portal_json_items_v1(v_flow_json #> '{flowDataSet,flowProperties,flowProperty}') as item
  where item ->> '@dataSetInternalID' = v_flow_property_internal
    and item ->> '@dataSetInternalID' ~ '^(0|[1-9][0-9]{0,4})$';
  if v_match_count <> 1 then
    return null;
  end if;
  v_flow_factor := private.portal_canonical_decimal_v1(v_flow_property_item ->> 'meanValue');
  v_flow_property_reference := v_flow_property_item -> 'referenceToFlowPropertyDataSet';
  v_flow_property_id_text := lower(btrim(coalesce(v_flow_property_reference ->> '@refObjectId', '')));
  v_flow_property_version := btrim(coalesce(v_flow_property_reference ->> '@version', ''));
  if v_flow_factor is distinct from '1'
     or v_flow_property_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_flow_property_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_flow_property_id := v_flow_property_id_text::uuid;

  select row.json, row.state_code
  into v_flow_property_json, v_flow_property_state
  from public.flowproperties as row
  where row.id = v_flow_property_id
    and row.version::text = v_flow_property_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'flowPropertyDataSet') = 'object'
  limit 1;
  if v_flow_property_json is null
     or coalesce((private.portal_support_capabilities_v1('flowproperty', v_flow_property_state) ->> 'exchangesVisible')::boolean, false) is not true then
    return null;
  end if;

  v_unit_group_reference := v_flow_property_json #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}';
  v_unit_group_id_text := lower(btrim(coalesce(v_unit_group_reference ->> '@refObjectId', '')));
  v_unit_group_version := btrim(coalesce(v_unit_group_reference ->> '@version', ''));
  if v_unit_group_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_unit_group_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_unit_group_id := v_unit_group_id_text::uuid;

  select row.json, row.state_code
  into v_unit_group_json, v_unit_group_state
  from public.unitgroups as row
  where row.id = v_unit_group_id
    and row.version::text = v_unit_group_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'unitGroupDataSet') = 'object'
  limit 1;
  if v_unit_group_json is null
     or coalesce((private.portal_support_capabilities_v1('unitgroup', v_unit_group_state) ->> 'exchangesVisible')::boolean, false) is not true then
    return null;
  end if;

  v_unit_internal := v_unit_group_json #>> '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}';
  if v_unit_internal !~ '^(0|[1-9][0-9]{0,4})$' then
    return null;
  end if;
  select count(*), (jsonb_agg(item) -> 0)
  into v_match_count, v_unit_item
  from private.portal_json_items_v1(v_unit_group_json #> '{unitGroupDataSet,units,unit}') as item
  where item ->> '@dataSetInternalID' = v_unit_internal
    and item ->> '@dataSetInternalID' ~ '^(0|[1-9][0-9]{0,4})$';
  if v_match_count <> 1 then
    return null;
  end if;
  v_unit := nullif(private.portal_scalar_text_v1(v_unit_item -> 'name'), '');
  v_unit_factor := private.portal_canonical_decimal_v1(v_unit_item ->> 'meanValue');
  if v_unit is null or v_unit_factor is distinct from '1' then
    return null;
  end if;

  v_classifications := private.portal_classifications_v1(
    v_flow_json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}'
  );
  v_uncertainty_type := nullif(
    private.portal_scalar_text_v1(p_exchange -> 'uncertaintyDistributionType'),
    ''
  );
  v_minimum := private.portal_canonical_decimal_v1(p_exchange ->> 'minimumAmount');
  if v_minimum is null then
    v_minimum := private.portal_canonical_decimal_v1(p_exchange ->> 'minimumValue');
  end if;
  v_maximum := private.portal_canonical_decimal_v1(p_exchange ->> 'maximumAmount');
  if v_maximum is null then
    v_maximum := private.portal_canonical_decimal_v1(p_exchange ->> 'maximumValue');
  end if;

  v_row := jsonb_build_object(
    'internalId', v_internal_id,
    'kind', v_exchange_kind,
    'direction', v_direction,
    'flow', jsonb_build_object(
      'id', v_flow_id_text,
      'version', v_flow_version,
      'name', private.portal_localized_text_v1(
        v_flow_json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
      )
    ),
    'classification', case when jsonb_array_length(v_classifications) > 0 then v_classifications -> 0 else null end,
    'amount', v_amount,
    'unit', v_unit,
    'isQuantitativeReference', v_internal_id = (
      p_process_json #>> '{processDataSet,processInformation,quantitativeReference,referenceToReferenceFlow}'
    ),
    'uncertainty', case when v_uncertainty_type is null then null else jsonb_build_object(
      'type', v_uncertainty_type,
      'minimum', v_minimum,
      'maximum', v_maximum
    ) end,
    'origin', '[]'::jsonb
  );
  return jsonb_build_object(
    'row', v_row,
    'functionalUnit', jsonb_build_object(
      'amount', v_amount,
      'unit', v_unit,
      'description', private.portal_localized_text_v1(
        p_process_json #> '{processDataSet,processInformation,quantitativeReference,functionalUnitOrOther}'
      )
    )
  );
end
$_$;

ALTER FUNCTION "private"."portal_exchange_support_v1"("p_process_state" integer, "p_process_json" "jsonb", "p_exchange" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_exchange_support_v1"("p_process_state" integer, "p_process_json" "jsonb", "p_exchange" "jsonb") FROM PUBLIC;
