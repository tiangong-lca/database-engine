CREATE OR REPLACE FUNCTION "api"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
  select *
  from private.search_processes_latest_v2_impl(
    query_text, filter_condition, page_size, page_current, data_source,
    this_user_id, team_id_filter, state_code_filter, type_of_data_set_filter,
    query_terms, false
  )
$$;

ALTER FUNCTION "api"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "api"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) TO "authenticated";
