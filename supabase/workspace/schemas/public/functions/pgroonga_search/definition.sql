CREATE OR REPLACE FUNCTION "public"."pgroonga_search"("query_text" "text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
BEGIN
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.json
		FROM flows f
		WHERE f.extracted_text &@~ query_text
		ORDER BY pgroonga_score(tableoid, ctid) DESC;
END;$$;

ALTER FUNCTION "public"."pgroonga_search"("query_text" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search"("query_text" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search"("query_text" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search"("query_text" "text") TO "service_role";
