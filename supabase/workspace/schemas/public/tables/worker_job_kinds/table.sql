CREATE TABLE IF NOT EXISTS "public"."worker_job_kinds" (
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
    CONSTRAINT "worker_job_kinds_default_attempts_check" CHECK (("default_max_attempts" >= 0)),
    CONSTRAINT "worker_job_kinds_default_lease_check" CHECK ((("default_lease_seconds" >= 1) AND ("default_lease_seconds" <= 86400))),
    CONSTRAINT "worker_job_kinds_queue_check" CHECK (("worker_queue" = ANY (ARRAY['solver'::"text", 'review_submit'::"text", 'review_submit_gate'::"text", 'package'::"text", 'maintenance'::"text"]))),
    CONSTRAINT "worker_job_kinds_runtime_check" CHECK (("worker_runtime" = 'calculator'::"text")),
    CONSTRAINT "worker_job_kinds_visibility_check" CHECK (("default_visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);

ALTER TABLE "public"."worker_job_kinds" OWNER TO "postgres";

ALTER TABLE ONLY "public"."worker_job_kinds"
    ADD CONSTRAINT "worker_job_kinds_pkey" PRIMARY KEY ("job_kind");

ALTER TABLE "public"."worker_job_kinds" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."worker_job_kinds" TO "service_role";
