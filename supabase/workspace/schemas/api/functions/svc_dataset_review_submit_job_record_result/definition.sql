CREATE OR REPLACE FUNCTION "api"."svc_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_result" "jsonb" DEFAULT NULL::"jsonb", "p_error_code" "text" DEFAULT NULL::"text", "p_error_message" "text" DEFAULT NULL::"text", "p_error_details" "jsonb" DEFAULT NULL::"jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.cmd_dataset_review_submit_job_record_result(
    p_job_id, p_status, p_gate_run_id, p_result, p_error_code,
    p_error_message, p_error_details, p_audit
  )
$$;

ALTER FUNCTION "api"."svc_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") TO "service_role";
