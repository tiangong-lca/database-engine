CREATE OR REPLACE FUNCTION "private"."portal_geography_precision_v1"("p_code" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select 'unknown'::text
$$;

ALTER FUNCTION "private"."portal_geography_precision_v1"("p_code" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_geography_precision_v1"("p_code" "text") FROM PUBLIC;
