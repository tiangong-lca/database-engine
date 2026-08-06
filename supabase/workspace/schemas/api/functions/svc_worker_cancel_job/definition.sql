CREATE OR REPLACE FUNCTION "api"."svc_worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid" DEFAULT NULL::"uuid", "p_reason" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.worker_cancel_job(p_job_id, p_cancelled_by, p_reason)
$$;

ALTER FUNCTION "api"."svc_worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid", "p_reason" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid", "p_reason" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid", "p_reason" "text") TO "service_role";
