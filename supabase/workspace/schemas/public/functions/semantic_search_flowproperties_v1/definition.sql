CREATE OR REPLACE FUNCTION "public"."semantic_search_flowproperties_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text", "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "sql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
  select * from private.semantic_simple_dataset_search(
    'public.flowproperties'::regclass,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    data_source,
    state_code_filter,
    team_id_filter
  );
$$;

ALTER FUNCTION "public"."semantic_search_flowproperties_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."semantic_search_flowproperties_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."semantic_search_flowproperties_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."semantic_search_flowproperties_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "service_role";
