CREATE TABLE IF NOT EXISTS "public"."dataset_review_submit_gate_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_table" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "revision_checksum" "text" NOT NULL,
    "policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text" NOT NULL,
    "report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "supersedes_gate_run_id" "uuid",
    "calculator_report" "jsonb",
    "blocking_reasons" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    "worker_job_id" "uuid",
    CONSTRAINT "dataset_review_submit_gate_runs_blocking_reasons_check" CHECK (("jsonb_typeof"("blocking_reasons") = 'array'::"text")),
    CONSTRAINT "dataset_review_submit_gate_runs_calculator_report_check" CHECK ((("calculator_report" IS NULL) OR ("jsonb_typeof"("calculator_report") = 'object'::"text"))),
    CONSTRAINT "dataset_review_submit_gate_runs_checksum_check" CHECK (("revision_checksum" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_review_submit_gate_runs_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'passed'::"text", 'blocked'::"text", 'error'::"text", 'stale'::"text"]))),
    CONSTRAINT "dataset_review_submit_gate_runs_table_check" CHECK (("dataset_table" = ANY (ARRAY['processes'::"text", 'lifecyclemodels'::"text"])))
);

ALTER TABLE "public"."dataset_review_submit_gate_runs" OWNER TO "postgres";

COMMENT ON TABLE "public"."dataset_review_submit_gate_runs" IS 'Review-submit gate report/history table retained for compatibility. New gate execution lifecycle is public.worker_jobs.';

ALTER TABLE ONLY "public"."dataset_review_submit_gate_runs"
    ADD CONSTRAINT "dataset_review_submit_gate_runs_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."dataset_review_submit_gate_runs"
    ADD CONSTRAINT "dataset_review_submit_gate_runs_supersedes_gate_run_id_fkey" FOREIGN KEY ("supersedes_gate_run_id") REFERENCES "public"."dataset_review_submit_gate_runs"("id");

ALTER TABLE ONLY "public"."dataset_review_submit_gate_runs"
    ADD CONSTRAINT "dataset_review_submit_gate_runs_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE "public"."dataset_review_submit_gate_runs" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."dataset_review_submit_gate_runs" TO "service_role";
