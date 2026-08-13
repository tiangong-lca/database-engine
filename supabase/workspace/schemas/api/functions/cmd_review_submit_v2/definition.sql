CREATE OR REPLACE FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb" DEFAULT NULL::"jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select api.cmd_review_submit(
    p_target_table,
    p_target_id,
    p_target_version,
    coalesce(p_audit, '{}'::jsonb)
      || jsonb_build_object('compatibility_entrypoint', 'cmd_review_submit_v2')
  )
$$;

ALTER FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_review_submit_v2"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_gate_context" "jsonb", "p_audit" "jsonb") TO "authenticated";
