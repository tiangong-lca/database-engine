CREATE OR REPLACE FUNCTION "api"."svc_worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.worker_read_job(p_job_id, p_include_internal)
$$;

ALTER FUNCTION "api"."svc_worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) TO "service_role";
