CREATE OR REPLACE FUNCTION "private"."portal_classifications_v1"("p_information" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_result jsonb := '[]'::jsonb;
  v_classification jsonb;
  v_class jsonb;
  v_category jsonb;
  v_system text;
  v_code text;
begin
  for v_classification in
    select private.portal_json_items_v1(p_information -> 'common:classification')
  loop
    v_system := coalesce(
      nullif(private.portal_scalar_text_v1(v_classification -> '@name'), ''),
      'ILCD'
    );
    for v_class in
      select private.portal_json_items_v1(v_classification -> 'common:class')
    loop
      v_code := coalesce(
        nullif(private.portal_scalar_text_v1(v_class -> '@classId'), ''),
        nullif(private.portal_scalar_text_v1(v_class -> '#text'), '')
      );
      if v_code is not null then
        v_result := v_result || jsonb_build_array(jsonb_build_object(
          'system', v_system,
          'code', v_code,
          'label', private.portal_localized_text_v1(v_class)
        ));
      end if;
    end loop;
  end loop;

  for v_category in
    select private.portal_json_items_v1(
      p_information #> '{common:elementaryFlowCategorization,common:category}'
    )
  loop
    v_code := coalesce(
      nullif(private.portal_scalar_text_v1(v_category -> '@catId'), ''),
      nullif(private.portal_scalar_text_v1(v_category -> '@classId'), ''),
      nullif(private.portal_scalar_text_v1(v_category -> '#text'), '')
    );
    if v_code is not null then
      v_result := v_result || jsonb_build_array(jsonb_build_object(
        'system', 'elementary-flow',
        'code', v_code,
        'label', private.portal_localized_text_v1(v_category)
      ));
    end if;
  end loop;
  return v_result;
end
$$;

ALTER FUNCTION "private"."portal_classifications_v1"("p_information" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_classifications_v1"("p_information" "jsonb") FROM PUBLIC;
