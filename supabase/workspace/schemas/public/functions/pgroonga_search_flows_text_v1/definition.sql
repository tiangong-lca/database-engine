CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "extracted_text" "text", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
BEGIN
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.extracted_text,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM public.flows AS f
		WHERE f.extracted_text &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'co' AND state_code = 200) or (data_source = 'my' AND user_id = auth.uid())
																			  or (data_source = 'te' and
		EXISTS ( 
						SELECT 1
						FROM roles r
						WHERE r.user_id = auth.uid() and r.team_id =  f.team_id
						AND r.role::text IN ('admin', 'member', 'owner') 
				)
			)
		)
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";
