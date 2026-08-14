CREATE OR REPLACE FUNCTION "api"."svc_lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text" DEFAULT NULL::"text", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.lca_read_result_projection(
    p_requested_by, p_result_id, p_required_artifact_format, p_include_internal
  )
$$;

ALTER FUNCTION "api"."svc_lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) TO "service_role";
