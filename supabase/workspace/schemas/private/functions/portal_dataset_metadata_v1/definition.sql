CREATE OR REPLACE FUNCTION "private"."portal_dataset_metadata_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_location_code text;
  v_cas text;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information #> '{geography,locationOfOperationSupplyOrProduction}';
    v_location_code := nullif(
      private.portal_scalar_text_v1(v_location -> '@location'),
      ''
    );
    return jsonb_build_object(
      'kind', 'process',
      'names', private.portal_localized_text_v1(v_information #> '{dataSetInformation,name,baseName}'),
      'generalComment', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:generalComment}'),
      'referenceProduct', private.portal_process_reference_product_v1(p_json),
      'functionalUnit', private.portal_process_functional_unit_v1(p_state_code, p_json),
      'classifications', private.portal_classifications_v1(v_information #> '{dataSetInformation,classificationInformation}'),
      'geography', jsonb_build_object(
        'code', v_location_code,
        'label', private.portal_localized_text_v1(v_location -> 'descriptionOfRestrictions'),
        'precision', private.portal_geography_precision_v1(v_location_code)
      ),
      'referenceYear', private.portal_safe_year_v1(v_information #>> '{time,common:referenceYear}'),
      'validUntilYear', private.portal_safe_year_v1(v_information #>> '{time,common:dataSetValidUntil}'),
      'technology',
        private.portal_localized_text_v1(
          v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
        ) || private.portal_localized_text_v1(
          v_information #> '{technology,technologicalApplicability}'
        ),
      'dataSetType', nullif(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
      ), ''),
      'allocationAndModeling',
        private.portal_localized_text_v1(
          v_modelling #> '{LCIMethodAndAllocation,deviationsFromLCIMethodPrinciple}'
        ) || private.portal_localized_text_v1(
          v_modelling #> '{LCIMethodAndAllocation,deviationsFromModellingConstants}'
        ),
      'cutoffRules', private.portal_localized_text_v1(
        v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,deviationsFromCutOffAndCompletenessPrinciples}'
      ),
      'quality', jsonb_build_object(
        'reviewStatus', (
          select nullif(private.portal_scalar_text_v1(review_item -> '@type'), '')
          from private.portal_json_items_v1(v_modelling #> '{validation,review}') as review_item
          limit 1
        ),
        'timeRepresentativeness', private.portal_first_text_v1(
          v_information #> '{time,common:timeRepresentativenessDescription}'
        ),
        'geographyRepresentativeness', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,geographicalRepresentativenessDescription}'
        ),
        'technologyRepresentativeness', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,technologicalRepresentativenessDescription}'
        ),
        'completeness', private.portal_first_text_v1(
          v_modelling #> '{completeness,completenessOtherProblemField}'
        ),
        'uncertainty', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,uncertaintyAdjustments}'
        )
      ),
      'source', private.portal_source_v1('process', p_json),
      'compliance', private.portal_compliance_v1('process', p_json),
      'administration', private.portal_administration_v1('process', p_json)
    );
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_modelling := p_json #> '{flowDataSet,modellingAndValidation}';
    v_location := v_information -> 'geography';
    v_location_code := case jsonb_typeof(v_location -> 'locationOfSupply')
      when 'string' then nullif(
        private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
        ''
      )
      when 'object' then nullif(
        private.portal_scalar_text_v1(v_location #> '{locationOfSupply,@location}'),
        ''
      )
      else null
    end;
    v_cas := nullif(btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    return jsonb_build_object(
      'kind', 'flow',
      'names', private.portal_localized_text_v1(v_information #> '{dataSetInformation,name,baseName}'),
      'synonyms', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:synonyms}'),
      'generalComment', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:generalComment}'),
      'casNumber', v_cas,
      'flowType', private.portal_flow_kind_v1(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethod,typeOfDataSet}'
      )),
      'classifications', private.portal_classifications_v1(v_information #> '{dataSetInformation,classificationInformation}'),
      'locationOfSupply', jsonb_build_object(
        'code', v_location_code,
        'label', private.portal_localized_text_v1(v_location #> '{locationOfSupply,descriptionOfRestrictions}')
      ),
      'referenceFlowProperty', private.portal_reference_flowproperty_v1(p_json),
      'source', private.portal_source_v1('flow', p_json),
      'compliance', private.portal_compliance_v1('flow', p_json),
      'administration', private.portal_administration_v1('flow', p_json)
    );
  end if;
  return null;
end
$_$;

ALTER FUNCTION "private"."portal_dataset_metadata_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_dataset_metadata_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;
