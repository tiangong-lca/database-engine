CREATE OR REPLACE FUNCTION "api"."svc_worker_list_jobs"("p_requested_by" "uuid" DEFAULT NULL::"uuid", "p_subject_type" "text" DEFAULT NULL::"text", "p_subject_id" "uuid" DEFAULT NULL::"uuid", "p_statuses" "text"[] DEFAULT NULL::"text"[], "p_visibility" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 50, "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.worker_list_jobs(
    p_requested_by, p_subject_type, p_subject_id, p_statuses,
    p_visibility, p_limit, p_include_internal
  )
$$;

ALTER FUNCTION "api"."svc_worker_list_jobs"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_statuses" "text"[], "p_visibility" "text", "p_limit" integer, "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_worker_list_jobs"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_statuses" "text"[], "p_visibility" "text", "p_limit" integer, "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_worker_list_jobs"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_statuses" "text"[], "p_visibility" "text", "p_limit" integer, "p_include_internal" boolean) TO "service_role";
