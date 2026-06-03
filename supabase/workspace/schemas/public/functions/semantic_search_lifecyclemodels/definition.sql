CREATE OR REPLACE FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "sql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
  select * from public.semantic_search_lifecyclemodels_v1($1, $2, $3, $4, $5);
$_$;

ALTER FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";
