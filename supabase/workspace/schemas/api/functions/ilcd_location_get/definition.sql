CREATE OR REPLACE FUNCTION "api"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) RETURNS SETOF "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
BEGIN
  RETURN QUERY
  SELECT lc
  FROM (
    SELECT
      ilcd.file_name,
      jsonb_array_elements(ilcd.json -> 'ILCDLocations' -> 'location') AS lc
    FROM
      ilcd
    WHERE ilcd.file_name = this_file_name
  ) AS lcs
  WHERE lcs.lc->>'@value' = ANY(get_values);
END;
$$;

ALTER FUNCTION "api"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "api_internal_executor";
