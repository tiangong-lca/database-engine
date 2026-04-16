CREATE OR REPLACE FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) RETURNS SETOF "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
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

ALTER FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "authenticated";

GRANT ALL ON FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "service_role";
