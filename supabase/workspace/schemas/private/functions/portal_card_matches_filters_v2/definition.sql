CREATE OR REPLACE FUNCTION "private"."portal_card_matches_filters_v2"("p_card" "jsonb", "p_filters" "jsonb") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select
    (not (p_filters ? 'accessLevel') or p_card ->> 'accessLevel' = p_filters ->> 'accessLevel')
    and (not (p_filters ? 'geography') or pg_catalog.lower(pg_catalog.btrim(coalesce(
      p_card #>> '{geography,code}', ''))) = p_filters ->> 'geography')
    and (not (p_filters ? 'classification') or exists (
      select 1 from pg_catalog.jsonb_array_elements(coalesce(
        p_card -> 'classifications', '[]'::jsonb)) as classification(item)
      where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
        = p_filters ->> 'classification'))
    and (not (p_filters ? 'referenceYearFrom') or (p_card ->> 'referenceYear')::integer
      >= (p_filters ->> 'referenceYearFrom')::integer)
    and (not (p_filters ? 'referenceYearTo') or (p_card ->> 'referenceYear')::integer
      <= (p_filters ->> 'referenceYearTo')::integer)
    and (not (p_filters ? 'processSubtype') or pg_catalog.lower(pg_catalog.btrim(coalesce(
      p_card ->> 'processSubtype', ''))) = p_filters ->> 'processSubtype')
    and (not (p_filters ? 'source') or pg_catalog.lower(pg_catalog.btrim(coalesce(
      p_card ->> 'source', ''))) = p_filters ->> 'source');
$$;

ALTER FUNCTION "private"."portal_card_matches_filters_v2"("p_card" "jsonb", "p_filters" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_card_matches_filters_v2"("p_card" "jsonb", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_card_matches_filters_v2"("p_card" "jsonb", "p_filters" "jsonb") TO "api_internal_executor";
