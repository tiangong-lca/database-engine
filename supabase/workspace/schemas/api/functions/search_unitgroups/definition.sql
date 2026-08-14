CREATE OR REPLACE FUNCTION "api"."search_unitgroups"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from api._search_simple_dataset_latest(
      'public.unitgroups'::regclass,
      query_text,
      filter_condition,
      page_size::bigint,
      page_current::bigint,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;

ALTER FUNCTION "api"."search_unitgroups"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."search_unitgroups"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."search_unitgroups"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."search_unitgroups"("query_text" "text", "filter_condition" "jsonb", "page_size" integer, "page_current" integer, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
