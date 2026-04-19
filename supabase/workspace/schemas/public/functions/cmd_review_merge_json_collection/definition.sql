CREATE OR REPLACE FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select public.cmd_review_json_array(p_existing) || public.cmd_review_json_array(p_additions)
$$;

ALTER FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "service_role";
