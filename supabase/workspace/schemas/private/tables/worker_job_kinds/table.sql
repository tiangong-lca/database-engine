CREATE TABLE IF NOT EXISTS "private"."worker_job_kinds" (
    "job_kind" "text" NOT NULL,
    "worker_runtime" "text" DEFAULT 'calculator'::"text" NOT NULL,
    "worker_queue" "text" NOT NULL,
    "default_visibility" "text" DEFAULT 'user'::"text" NOT NULL,
    "default_priority" integer DEFAULT 0 NOT NULL,
    "default_max_attempts" integer DEFAULT 3 NOT NULL,
    "default_lease_seconds" integer DEFAULT 300 NOT NULL,
    "payload_schema_version" "text" NOT NULL,
    "result_schema_version" "text",
    "user_visible" boolean DEFAULT true NOT NULL,
    "description" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "task_center_category" "text",
    "task_center_surface" "text",
    "presenter_key" "text",
    CONSTRAINT "worker_job_kinds_default_attempts_check" CHECK (("default_max_attempts" >= 0)),
    CONSTRAINT "worker_job_kinds_default_lease_check" CHECK ((("default_lease_seconds" >= 1) AND ("default_lease_seconds" <= 86400))),
    CONSTRAINT "worker_job_kinds_queue_check" CHECK (("worker_queue" = ANY (ARRAY['solver'::"text", 'review_submit'::"text", 'review_submit_gate'::"text", 'review_quality'::"text", 'package'::"text", 'maintenance'::"text"]))),
    CONSTRAINT "worker_job_kinds_runtime_check" CHECK (("worker_runtime" = 'calculator'::"text")),
    CONSTRAINT "worker_job_kinds_task_center_surface_check" CHECK ((("task_center_surface" IS NULL) OR ("task_center_surface" = ANY (ARRAY['global'::"text", 'inline'::"text"])))),
    CONSTRAINT "worker_job_kinds_visibility_check" CHECK (("default_visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);

ALTER TABLE "private"."worker_job_kinds" OWNER TO "postgres";

ALTER TABLE ONLY "private"."worker_job_kinds"
    ADD CONSTRAINT "worker_job_kinds_pkey" PRIMARY KEY ("job_kind");

ALTER TABLE "private"."worker_job_kinds" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."worker_job_kinds" TO "service_role";

GRANT SELECT ON TABLE "private"."worker_job_kinds" TO "api_internal_executor";
