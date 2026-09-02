begin;

drop function api.search_processes_latest_v2(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean);

grant api_internal_executor to postgres;
grant create on schema api to api_internal_executor;
set role api_internal_executor;

CREATE OR REPLACE FUNCTION "api"."search_processes_latest_v2"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "query_terms" "text"[] DEFAULT NULL::"text"[], "owner_draft_only" boolean DEFAULT false) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "model_version" character, "total_count" bigint)
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
  select
    result.rank,
    result.id,
    result.json,
    result.version,
    result.modified_at,
    result.team_id,
    result.model_id,
    process.model_version,
    result.total_count
  from private.search_processes_latest_v2_impl(
    query_text,
    filter_condition,
    page_size,
    page_current,
    data_source,
    this_user_id,
    team_id_filter,
    state_code_filter,
    type_of_data_set_filter,
    query_terms,
    owner_draft_only
  ) as result
  left join public.processes as process
    on process.id = result.id
   and process.version = result.version;
$$;

ALTER FUNCTION "api"."search_processes_latest_v2"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."search_processes_latest_v2"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."search_processes_latest_v2"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) TO "anon";

GRANT ALL ON FUNCTION "api"."search_processes_latest_v2"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[], "owner_draft_only" boolean) TO "authenticated";

comment on function api.search_processes_latest_v2(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) is
  'Compatibility Process lexical search RPC with optional strict owner-draft scope. Returns model_version from the selected Process revision.';

reset role;
revoke create on schema api from api_internal_executor;
revoke api_internal_executor from postgres;

commit;
