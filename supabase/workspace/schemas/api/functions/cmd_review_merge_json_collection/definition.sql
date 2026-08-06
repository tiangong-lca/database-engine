CREATE OR REPLACE FUNCTION "api"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select api.cmd_review_json_array(p_existing) || api.cmd_review_json_array(p_additions)
$$;

ALTER FUNCTION "api"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "api_internal_executor";
