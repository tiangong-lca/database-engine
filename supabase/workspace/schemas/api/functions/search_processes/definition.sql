CREATE OR REPLACE FUNCTION "api"."search_processes"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "query_terms" "text"[] DEFAULT NULL::"text"[], "owner_draft_only" boolean DEFAULT false) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
  select *
  from private.search_processes_latest_v2_impl(
    query_text,
    filter_condition,
    page_size::bigint,
    page_current::bigint,
    data_source,
    this_user_id,
    team_id_filter,
    state_code_filter,
    type_of_data_set_filter,
    query_terms,
    owner_draft_only
  );
$$;

ALTER FUNCTION "api"."search_processes"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."search_processes"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."search_processes"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) TO "anon";

GRANT ALL ON FUNCTION "api"."search_processes"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) TO "authenticated";
