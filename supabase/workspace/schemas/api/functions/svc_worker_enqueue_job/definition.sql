CREATE OR REPLACE FUNCTION "api"."svc_worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb" DEFAULT '{}'::"jsonb", "p_payload_schema_version" "text" DEFAULT NULL::"text", "p_subject_type" "text" DEFAULT NULL::"text", "p_subject_id" "uuid" DEFAULT NULL::"uuid", "p_subject_version" "text" DEFAULT NULL::"text", "p_requested_by" "uuid" DEFAULT NULL::"uuid", "p_requester_type" "text" DEFAULT 'user'::"text", "p_team_id" "uuid" DEFAULT NULL::"uuid", "p_idempotency_key" "text" DEFAULT NULL::"text", "p_request_hash" "text" DEFAULT NULL::"text", "p_concurrency_key" "text" DEFAULT NULL::"text", "p_priority" integer DEFAULT NULL::integer, "p_queue_key" "text" DEFAULT NULL::"text", "p_run_after" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_visibility" "text" DEFAULT NULL::"text", "p_max_attempts" integer DEFAULT NULL::integer, "p_timeout_at" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_payload_ref" "jsonb" DEFAULT NULL::"jsonb", "p_parent_job_id" "uuid" DEFAULT NULL::"uuid", "p_root_job_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.worker_enqueue_job(
    p_job_kind => p_job_kind,
    p_payload_json => p_payload_json,
    p_payload_schema_version => p_payload_schema_version,
    p_subject_type => p_subject_type,
    p_subject_id => p_subject_id,
    p_subject_version => p_subject_version,
    p_requested_by => p_requested_by,
    p_requester_type => p_requester_type,
    p_team_id => p_team_id,
    p_idempotency_key => p_idempotency_key,
    p_request_hash => p_request_hash,
    p_concurrency_key => p_concurrency_key,
    p_priority => p_priority,
    p_queue_key => p_queue_key,
    p_run_after => p_run_after,
    p_visibility => p_visibility,
    p_max_attempts => p_max_attempts,
    p_timeout_at => p_timeout_at,
    p_payload_ref => p_payload_ref,
    p_parent_job_id => p_parent_job_id,
    p_root_job_id => p_root_job_id
  )
$$;

ALTER FUNCTION "api"."svc_worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb", "p_payload_schema_version" "text", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_requested_by" "uuid", "p_requester_type" "text", "p_team_id" "uuid", "p_idempotency_key" "text", "p_request_hash" "text", "p_concurrency_key" "text", "p_priority" integer, "p_queue_key" "text", "p_run_after" timestamp with time zone, "p_visibility" "text", "p_max_attempts" integer, "p_timeout_at" timestamp with time zone, "p_payload_ref" "jsonb", "p_parent_job_id" "uuid", "p_root_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb", "p_payload_schema_version" "text", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_requested_by" "uuid", "p_requester_type" "text", "p_team_id" "uuid", "p_idempotency_key" "text", "p_request_hash" "text", "p_concurrency_key" "text", "p_priority" integer, "p_queue_key" "text", "p_run_after" timestamp with time zone, "p_visibility" "text", "p_max_attempts" integer, "p_timeout_at" timestamp with time zone, "p_payload_ref" "jsonb", "p_parent_job_id" "uuid", "p_root_job_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb", "p_payload_schema_version" "text", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_requested_by" "uuid", "p_requester_type" "text", "p_team_id" "uuid", "p_idempotency_key" "text", "p_request_hash" "text", "p_concurrency_key" "text", "p_priority" integer, "p_queue_key" "text", "p_run_after" timestamp with time zone, "p_visibility" "text", "p_max_attempts" integer, "p_timeout_at" timestamp with time zone, "p_payload_ref" "jsonb", "p_parent_job_id" "uuid", "p_root_job_id" "uuid") TO "service_role";
