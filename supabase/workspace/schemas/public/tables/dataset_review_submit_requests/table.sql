CREATE TABLE IF NOT EXISTS "public"."dataset_review_submit_requests" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_table" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "revision_checksum" "text" NOT NULL,
    "policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text" NOT NULL,
    "report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "gate_run_id" "uuid",
    "gate_worker_job_id" "uuid",
    "submit_worker_job_id" "uuid",
    "attempt_count" integer DEFAULT 0 NOT NULL,
    "last_error_code" "text",
    "last_error_message" "text",
    "last_error_details" "jsonb",
    "result" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    CONSTRAINT "dataset_review_submit_requests_attempt_count_check" CHECK (("attempt_count" >= 0)),
    CONSTRAINT "dataset_review_submit_requests_checksum_check" CHECK (("revision_checksum" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_review_submit_requests_last_error_details_check" CHECK ((("last_error_details" IS NULL) OR ("jsonb_typeof"("last_error_details") = 'object'::"text"))),
    CONSTRAINT "dataset_review_submit_requests_result_check" CHECK ((("result" IS NULL) OR ("jsonb_typeof"("result") = 'object'::"text"))),
    CONSTRAINT "dataset_review_submit_requests_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'waiting_gate'::"text", 'submitting'::"text", 'submitted'::"text", 'blocked'::"text", 'stale'::"text", 'error'::"text", 'cancelled'::"text"]))),
    CONSTRAINT "dataset_review_submit_requests_table_check" CHECK (("dataset_table" = 'processes'::"text"))
);

ALTER TABLE "public"."dataset_review_submit_requests" OWNER TO "postgres";

COMMENT ON TABLE "public"."dataset_review_submit_requests" IS 'Durable review-submit request/coordinator state. This replaces dataset_review_submit_jobs as the active coordinator table while worker_jobs remains the canonical lifecycle fact.';

ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_gate_run_id_fkey" FOREIGN KEY ("gate_run_id") REFERENCES "public"."dataset_review_submit_gate_runs"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_gate_worker_job_id_fkey" FOREIGN KEY ("gate_worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_submit_worker_job_id_fkey" FOREIGN KEY ("submit_worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE "public"."dataset_review_submit_requests" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."dataset_review_submit_requests" TO "service_role";
