CREATE OR REPLACE FUNCTION "public"."hybrid_search_flowproperties_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[], "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "sql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
  select *
  from private.hybrid_search_simple_dataset_v2('public.flowproperties'::regclass,
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms,
    state_code_filter,
    team_id_filter
  );
$$;

ALTER FUNCTION "public"."hybrid_search_flowproperties_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."hybrid_search_flowproperties_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."hybrid_search_flowproperties_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."hybrid_search_flowproperties_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."hybrid_search_flowproperties_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "service_role";
