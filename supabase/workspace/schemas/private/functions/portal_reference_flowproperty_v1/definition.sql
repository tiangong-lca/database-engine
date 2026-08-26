CREATE OR REPLACE FUNCTION "private"."portal_reference_flowproperty_v1"("p_flow_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_internal_id text := p_flow_json #>> '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  v_flow_property jsonb;
  v_reference jsonb;
  v_id_text text;
  v_version text;
  v_id uuid;
  v_row_json jsonb;
  v_match_count bigint;
begin
  if v_internal_id !~ '^(0|[1-9][0-9]{0,4})$' then
    return null;
  end if;
  select count(*), (jsonb_agg(item) -> 0)
  into v_match_count, v_flow_property
  from private.portal_json_items_v1(p_flow_json #> '{flowDataSet,flowProperties,flowProperty}') as item
  where item ->> '@dataSetInternalID' = v_internal_id
    and item ->> '@dataSetInternalID' ~ '^(0|[1-9][0-9]{0,4})$';
  if v_match_count <> 1 then
    return null;
  end if;
  v_reference := v_flow_property -> 'referenceToFlowPropertyDataSet';
  v_id_text := lower(btrim(coalesce(v_reference ->> '@refObjectId', '')));
  v_version := btrim(coalesce(v_reference ->> '@version', ''));
  if v_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_id := v_id_text::uuid;
  select row.json
  into v_row_json
  from public.flowproperties as row
  where row.id = v_id
    and row.version::text = v_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'flowPropertyDataSet') = 'object'
  limit 1;
  if v_row_json is null then
    return null;
  end if;
  return jsonb_build_object(
    'id', v_id_text,
    'version', v_version,
    'name', private.portal_localized_text_v1(
      v_row_json #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
    )
  );
end
$_$;

ALTER FUNCTION "private"."portal_reference_flowproperty_v1"("p_flow_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_reference_flowproperty_v1"("p_flow_json" "jsonb") FROM PUBLIC;
