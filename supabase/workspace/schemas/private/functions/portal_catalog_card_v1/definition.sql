CREATE OR REPLACE FUNCTION "private"."portal_catalog_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_capabilities jsonb := private.portal_capabilities_v1(p_kind, p_state_code, p_json);
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_names jsonb := '[]'::jsonb;
  v_synonyms jsonb := '[]'::jsonb;
  v_summary jsonb := '[]'::jsonb;
  v_technology jsonb := '[]'::jsonb;
  v_geography jsonb;
  v_classifications jsonb := '[]'::jsonb;
  v_reference_year integer;
  v_process_subtype text;
  v_cas text;
  v_source_metadata jsonb;
  v_source text;
  v_document text;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information #> '{geography,locationOfOperationSupplyOrProduction}';
    v_names := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,name,baseName}'
    );
    v_summary := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:generalComment}'
    );
    v_technology := private.portal_localized_text_v1(
      v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
    ) || private.portal_localized_text_v1(
      v_information #> '{technology,technologicalApplicability}'
    );
    v_classifications := private.portal_classifications_v1(
      v_information #> '{dataSetInformation,classificationInformation}'
    );
    v_reference_year := private.portal_safe_year_v1(
      v_information #>> '{time,common:referenceYear}'
    );
    v_process_subtype := nullif(private.portal_scalar_text_v1(
      v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
    ), '');
    v_geography := jsonb_build_object(
      'code', nullif(private.portal_scalar_text_v1(v_location -> '@location'), ''),
      'label', private.portal_localized_text_v1(v_location -> 'descriptionOfRestrictions'),
      'precision', 'unknown'
    );
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_location := v_information -> 'geography';
    v_names := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,name,baseName}'
    );
    v_synonyms := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:synonyms}'
    );
    v_summary := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:generalComment}'
    );
    v_classifications := private.portal_classifications_v1(
      v_information #> '{dataSetInformation,classificationInformation}'
    );
    v_cas := nullif(btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    v_geography := jsonb_build_object(
      'code', case jsonb_typeof(v_location -> 'locationOfSupply')
        when 'string' then nullif(
          private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
          ''
        )
        when 'object' then nullif(
          private.portal_scalar_text_v1(v_location #> '{locationOfSupply,@location}'),
          ''
        )
        else null
      end,
      'label', private.portal_localized_text_v1(
        v_location #> '{locationOfSupply,descriptionOfRestrictions}'
      ),
      'precision', 'unknown'
    );
  else
    return null;
  end if;

  v_source_metadata := private.portal_source_v1(p_kind, p_json);
  select string_agg(item ->> 'value', ' ' order by item ->> 'language')
  into v_source
  from jsonb_array_elements(v_source_metadata -> 'providerName') as localized(item);
  select lower(concat_ws(' ',
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_names) as localized(item)),
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_synonyms) as localized(item)),
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_summary) as localized(item)),
    (select string_agg(item ->> 'code', ' ') from jsonb_array_elements(v_classifications) as classification(item)),
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_technology) as localized(item)),
    v_geography ->> 'code',
    v_reference_year::text,
    v_process_subtype,
    v_cas,
    v_source
  )) into v_document;
  return jsonb_build_object(
    'accessLevel', case when (v_capabilities ->> 'exchangesVisible')::boolean then 'open' else 'metadata_only' end,
    'capabilities', v_capabilities,
    'names', v_names,
    'summary', v_summary,
    'geography', v_geography,
    'referenceYear', to_jsonb(v_reference_year),
    'processSubtype', to_jsonb(v_process_subtype),
    'source', to_jsonb(v_source),
    'classifications', v_classifications,
    'casNumber', to_jsonb(v_cas),
    'document', to_jsonb(coalesce(v_document, ''))
  );
end
$_$;

ALTER FUNCTION "private"."portal_catalog_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;
