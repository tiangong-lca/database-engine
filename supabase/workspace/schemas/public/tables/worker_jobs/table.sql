CREATE TABLE IF NOT EXISTS "public"."worker_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_kind" "text" NOT NULL,
    "worker_runtime" "text" DEFAULT 'calculator'::"text" NOT NULL,
    "worker_queue" "text" NOT NULL,
    "priority" integer DEFAULT 0 NOT NULL,
    "queue_key" "text",
    "root_job_id" "uuid",
    "parent_job_id" "uuid",
    "subject_type" "text",
    "subject_id" "uuid",
    "subject_version" "text",
    "requester_type" "text" DEFAULT 'user'::"text" NOT NULL,
    "requested_by" "uuid",
    "team_id" "uuid",
    "idempotency_key" "text",
    "request_hash" "text",
    "concurrency_key" "text",
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "phase" "text",
    "progress" numeric,
    "visibility" "text" DEFAULT 'user'::"text" NOT NULL,
    "run_after" timestamp with time zone DEFAULT "now"() NOT NULL,
    "attempt_count" integer DEFAULT 0 NOT NULL,
    "max_attempts" integer DEFAULT 3 NOT NULL,
    "leased_by" "text",
    "lease_token" "uuid",
    "lease_expires_at" timestamp with time zone,
    "heartbeat_at" timestamp with time zone,
    "timeout_at" timestamp with time zone,
    "payload_schema_version" "text" NOT NULL,
    "payload_json" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "payload_ref" "jsonb",
    "result_schema_version" "text",
    "result_json" "jsonb",
    "result_ref" "jsonb",
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "error_code" "text",
    "error_message" "text",
    "error_details" "jsonb",
    "blocker_codes" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "resolution_scope" "text",
    "retryable" boolean,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "expires_at" timestamp with time zone,
    "cancelled_at" timestamp with time zone,
    "cancelled_by" "uuid",
    CONSTRAINT "worker_jobs_attempt_check" CHECK ((("attempt_count" >= 0) AND ("max_attempts" >= 0) AND ("attempt_count" <= "max_attempts"))),
    CONSTRAINT "worker_jobs_blocked_explanation_check" CHECK ((("status" <> 'blocked'::"text") OR (("cardinality"("blocker_codes") > 0) AND ("resolution_scope" IS NOT NULL)))),
    CONSTRAINT "worker_jobs_diagnostics_object_check" CHECK (("jsonb_typeof"("diagnostics") = 'object'::"text")),
    CONSTRAINT "worker_jobs_error_details_object_check" CHECK ((("error_details" IS NULL) OR ("jsonb_typeof"("error_details") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_payload_object_check" CHECK (("jsonb_typeof"("payload_json") = 'object'::"text")),
    CONSTRAINT "worker_jobs_payload_ref_object_check" CHECK ((("payload_ref" IS NULL) OR ("jsonb_typeof"("payload_ref") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_progress_check" CHECK ((("progress" IS NULL) OR (("progress" >= (0)::numeric) AND ("progress" <= (1)::numeric)))),
    CONSTRAINT "worker_jobs_queue_check" CHECK (("worker_queue" = ANY (ARRAY['solver'::"text", 'review_submit'::"text", 'review_submit_gate'::"text", 'package'::"text", 'maintenance'::"text"]))),
    CONSTRAINT "worker_jobs_requester_check" CHECK (((("requester_type" = 'user'::"text") AND ("requested_by" IS NOT NULL)) OR ("requester_type" = ANY (ARRAY['system'::"text", 'service'::"text", 'operator'::"text"])))),
    CONSTRAINT "worker_jobs_resolution_scope_check" CHECK ((("resolution_scope" IS NULL) OR ("resolution_scope" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))),
    CONSTRAINT "worker_jobs_result_object_check" CHECK ((("result_json" IS NULL) OR ("jsonb_typeof"("result_json") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_result_ref_object_check" CHECK ((("result_ref" IS NULL) OR ("jsonb_typeof"("result_ref") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_runtime_check" CHECK (("worker_runtime" = 'calculator'::"text")),
    CONSTRAINT "worker_jobs_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'completed'::"text", 'blocked'::"text", 'stale'::"text", 'failed'::"text", 'cancelled'::"text"]))),
    CONSTRAINT "worker_jobs_visibility_check" CHECK (("visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);

ALTER TABLE "public"."worker_jobs" OWNER TO "postgres";

COMMENT ON TABLE "public"."worker_jobs" IS 'Canonical task fact table for work executed or coordinated by tiangong-lca-worker. Legacy job tables may remain only as domain artifact/cache/history compatibility surfaces.';

ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_job_kind_fkey" FOREIGN KEY ("job_kind") REFERENCES "public"."worker_job_kinds"("job_kind");

ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_parent_job_id_fkey" FOREIGN KEY ("parent_job_id") REFERENCES "public"."worker_jobs"("id");

ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_root_job_id_fkey" FOREIGN KEY ("root_job_id") REFERENCES "public"."worker_jobs"("id");

ALTER TABLE "public"."worker_jobs" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."worker_jobs" TO "service_role";
