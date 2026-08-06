CREATE OR REPLACE FUNCTION "private"."lca_release_error"("p_code" "text", "p_status" integer, "p_message" "text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select jsonb_build_object(
    'ok', false,
    'code', p_code,
    'status', p_status,
    'message', p_message
  )
$$;

ALTER FUNCTION "private"."lca_release_error"("p_code" "text", "p_status" integer, "p_message" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lca_release_error"("p_code" "text", "p_status" integer, "p_message" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lca_release_error"("p_code" "text", "p_status" integer, "p_message" "text") TO "api_internal_executor";
