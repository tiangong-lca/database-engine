CREATE OR REPLACE FUNCTION "api"."svc_lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid" DEFAULT NULL::"uuid", "p_legacy_job_id" "uuid" DEFAULT NULL::"uuid", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.lca_read_job_projection(
    p_requested_by => p_requested_by,
    p_worker_job_id => p_worker_job_id,
    p_legacy_job_id => p_legacy_job_id,
    p_include_internal => p_include_internal
  )
$$;

ALTER FUNCTION "api"."svc_lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) TO "service_role";
