CREATE OR REPLACE FUNCTION "private"."portal_catalog_facet_facts_v1"("p_kind" "text", "p_card" "jsonb") RETURNS TABLE("facet_access_level" "text", "facet_geography" "text", "facet_reference_year" "text", "facet_process_subtype" "text", "facet_source" "text")
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select
    p_card ->> 'accessLevel',
    pg_catalog.lower(pg_catalog.btrim(
      p_card #>> '{geography,code}'
    )),
    pg_catalog.btrim(p_card ->> 'referenceYear'),
    case when p_kind = 'process' then
      pg_catalog.lower(pg_catalog.btrim(
        p_card ->> 'processSubtype'
      ))
    else null::text end,
    pg_catalog.lower(pg_catalog.btrim(p_card ->> 'source'))
$$;

ALTER FUNCTION "private"."portal_catalog_facet_facts_v1"("p_kind" "text", "p_card" "jsonb") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_facet_facts_v1"("p_kind" "text", "p_card" "jsonb") FROM PUBLIC;
