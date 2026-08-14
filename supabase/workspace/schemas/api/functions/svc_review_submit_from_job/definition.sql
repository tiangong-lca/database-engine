CREATE OR REPLACE FUNCTION "api"."svc_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.cmd_review_submit_from_job(p_job_id, p_audit)
$$;

ALTER FUNCTION "api"."svc_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") TO "service_role";
