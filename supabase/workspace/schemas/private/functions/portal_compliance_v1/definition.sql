CREATE OR REPLACE FUNCTION "private"."portal_compliance_v1"("p_kind" "text", "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_value jsonb;
  v_item jsonb;
  v_result jsonb := '[]'::jsonb;
begin
  v_value := case p_kind
    when 'process' then p_json #> '{processDataSet,modellingAndValidation,complianceDeclarations,compliance}'
    when 'flow' then p_json #> '{flowDataSet,modellingAndValidation,complianceDeclarations,compliance}'
    else null
  end;
  for v_item in select private.portal_json_items_v1(v_value)
  loop
    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'system', private.portal_named_reference_v1(v_item -> 'common:referenceToComplianceSystem'),
      'overall', nullif(private.portal_scalar_text_v1(v_item -> 'common:approvalOfOverallCompliance'), ''),
      'nomenclature', nullif(private.portal_scalar_text_v1(v_item -> 'common:nomenclatureCompliance'), ''),
      'methodological', nullif(private.portal_scalar_text_v1(v_item -> 'common:methodologicalCompliance'), ''),
      'review', nullif(private.portal_scalar_text_v1(v_item -> 'common:reviewCompliance'), ''),
      'documentation', nullif(private.portal_scalar_text_v1(v_item -> 'common:documentationCompliance'), ''),
      'quality', nullif(private.portal_scalar_text_v1(v_item -> 'common:qualityCompliance'), '')
    ));
  end loop;
  return v_result;
end
$$;

ALTER FUNCTION "private"."portal_compliance_v1"("p_kind" "text", "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_compliance_v1"("p_kind" "text", "p_json" "jsonb") FROM PUBLIC;
