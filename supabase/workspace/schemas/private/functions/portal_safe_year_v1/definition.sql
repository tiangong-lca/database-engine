CREATE OR REPLACE FUNCTION "private"."portal_safe_year_v1"("p_value" "text") RETURNS integer
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  select case
    when btrim(coalesce(p_value, '')) ~ '^[0-9]{4}$'
      then btrim(p_value)::integer
    else null
  end
$_$;

ALTER FUNCTION "private"."portal_safe_year_v1"("p_value" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_safe_year_v1"("p_value" "text") FROM PUBLIC;
