CREATE OR REPLACE FUNCTION "private"."portal_process_names_v1"("p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  with parts as (
    select part.ordinality as part_order, item.ordinality as item_order,
      item.value ->> 'language' as language,
      pg_catalog.lower(item.value ->> 'language') as language_key,
      item.value ->> 'value' as value
    from pg_catalog.unnest(array[
      'baseName', 'treatmentStandardsRoutes', 'mixAndLocationTypes',
      'functionalUnitFlowProperties'
    ]) with ordinality as part(field, ordinality)
    cross join lateral pg_catalog.jsonb_array_elements(
      private.portal_localized_text_v1(
        p_json #> array['processDataSet','processInformation','dataSetInformation','name',part.field]
      )
    ) with ordinality as item(value, ordinality)
  ), first_values as (
    select distinct on (part_order, language_key) * from parts
    order by part_order, language_key, item_order
  ), names as (
    select base.language, base.item_order,
      pg_catalog.string_agg(part.value, '; ' order by part.part_order) as value
    from first_values as base
    join first_values as part on part.language_key = base.language_key
    where base.part_order = 1
    group by base.language, base.item_order
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('language', language, 'value', value)
    order by item_order
  ), '[]'::jsonb) from names
$$;

ALTER FUNCTION "private"."portal_process_names_v1"("p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_process_names_v1"("p_json" "jsonb") FROM PUBLIC;
