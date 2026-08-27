CREATE OR REPLACE FUNCTION "private"."portal_process_functional_unit_v1"("p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $$
declare
  v_reference_internal text := p_json #>> '{processDataSet,processInformation,quantitativeReference,referenceToReferenceFlow}';
  v_exchange jsonb;
  v_support jsonb;
  v_match_count integer;
begin
  select count(*), jsonb_agg(item) -> 0
  into v_match_count, v_exchange
  from private.portal_json_items_v1(p_json #> '{processDataSet,exchanges,exchange}') as item
  where item ->> '@dataSetInternalID' = v_reference_internal;
  if v_match_count <> 1 then
    return jsonb_build_object(
      'amount', null,
      'unit', null,
      'description', private.portal_localized_text_v1(
        p_json #> '{processDataSet,processInformation,quantitativeReference,functionalUnitOrOther}'
      )
    );
  end if;
  v_support := private.portal_exchange_support_v1(p_state_code, p_json, v_exchange);
  return coalesce(
    v_support -> 'functionalUnit',
    jsonb_build_object(
      'amount', null,
      'unit', null,
      'description', private.portal_localized_text_v1(
        p_json #> '{processDataSet,processInformation,quantitativeReference,functionalUnitOrOther}'
      )
    )
  );
end
$$;

ALTER FUNCTION "private"."portal_process_functional_unit_v1"("p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_process_functional_unit_v1"("p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;
