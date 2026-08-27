CREATE OR REPLACE FUNCTION "private"."portal_card_context_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_reference_name jsonb := '[]'::jsonb;
  v_functional_unit jsonb := 'null'::jsonb;
  v_technology jsonb := '[]'::jsonb;
  v_source jsonb;
  v_review_status jsonb := 'null'::jsonb;
  v_flow_property jsonb;
begin
  if p_state_code not in (100, 200)
     or pg_catalog.jsonb_typeof(p_json) <> 'object' then
    return null;
  end if;

  if p_kind = 'process'
     and pg_catalog.jsonb_typeof(p_json -> 'processDataSet') = 'object' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_reference_name := private.portal_process_reference_product_v1(p_json);

    -- Functional-unit amount/unit are public metadata, not permission to read
    -- Exchanges. Reuse the exact open support-chain validator for both public
    -- Process states, then emit null unless the evidence is complete.
    v_functional_unit := private.portal_process_functional_unit_v1(100, p_json);
    if pg_catalog.jsonb_typeof(v_functional_unit) <> 'object'
       or pg_catalog.jsonb_typeof(
         v_functional_unit -> 'amount'
       ) <> 'string'
       or pg_catalog.jsonb_typeof(
         v_functional_unit -> 'unit'
       ) <> 'string' then
      v_functional_unit := 'null'::jsonb;
    end if;

    v_technology := private.portal_localized_text_v1(
      v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
    ) || private.portal_localized_text_v1(
      v_information #> '{technology,technologicalApplicability}'
    );
    select coalesce(
      pg_catalog.to_jsonb(nullif(
        private.portal_scalar_text_v1(review_item -> '@type'),
        ''
      )),
      'null'::jsonb
    )
    into v_review_status
    from private.portal_json_items_v1(
      v_modelling #> '{validation,review}'
    ) as review_item
    limit 1;
    v_review_status := coalesce(v_review_status, 'null'::jsonb);
  elsif p_kind = 'flow'
     and pg_catalog.jsonb_typeof(p_json -> 'flowDataSet') = 'object' then
    v_flow_property := private.portal_reference_flowproperty_v1(p_json);
    v_reference_name := coalesce(
      v_flow_property -> 'name',
      '[]'::jsonb
    );
  else
    return null;
  end if;

  v_source := private.portal_source_v1(p_kind, p_json);
  if pg_catalog.jsonb_typeof(v_reference_name) <> 'array'
     or pg_catalog.jsonb_typeof(v_technology) <> 'array'
     or pg_catalog.jsonb_typeof(v_source) <> 'object' then
    return null;
  end if;

  return pg_catalog.jsonb_build_object(
    'reference', pg_catalog.jsonb_build_object(
      'kind', case p_kind
        when 'process' then 'reference_product'
        else 'reference_flow_property'
      end,
      'name', v_reference_name
    ),
    'functionalUnit', v_functional_unit,
    'technology', v_technology,
    'source', v_source,
    'quality', pg_catalog.jsonb_build_object(
      'reviewStatus', v_review_status
    )
  );
end
$$;

ALTER FUNCTION "private"."portal_card_context_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_card_context_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;
