CREATE OR REPLACE FUNCTION "api"."svc_dataset_review_submit_job_claim"("p_qty" integer DEFAULT 10, "p_stale_submitting_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.cmd_dataset_review_submit_job_claim(p_qty, p_stale_submitting_seconds)
$$;

ALTER FUNCTION "api"."svc_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) TO "service_role";
