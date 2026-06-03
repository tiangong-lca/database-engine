CREATE TABLE IF NOT EXISTS "public"."worker_job_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "event_type" "text" NOT NULL,
    "status" "text",
    "phase" "text",
    "progress" numeric,
    "worker_id" "text",
    "lease_token" "uuid",
    "message" "text",
    "details" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "worker_job_events_details_object_check" CHECK (("jsonb_typeof"("details") = 'object'::"text")),
    CONSTRAINT "worker_job_events_progress_check" CHECK ((("progress" IS NULL) OR (("progress" >= (0)::numeric) AND ("progress" <= (1)::numeric)))),
    CONSTRAINT "worker_job_events_status_check" CHECK ((("status" IS NULL) OR ("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'completed'::"text", 'blocked'::"text", 'stale'::"text", 'failed'::"text", 'cancelled'::"text"]))))
);

ALTER TABLE "public"."worker_job_events" OWNER TO "postgres";

ALTER TABLE ONLY "public"."worker_job_events"
    ADD CONSTRAINT "worker_job_events_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."worker_job_events"
    ADD CONSTRAINT "worker_job_events_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE CASCADE;

ALTER TABLE "public"."worker_job_events" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."worker_job_events" TO "service_role";
