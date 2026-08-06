CREATE TABLE IF NOT EXISTS "util"."dataset_extraction_job_failures" (
    "id" bigint NOT NULL,
    "queue_name" "text" DEFAULT 'dataset_extraction_jobs'::"text" NOT NULL,
    "msg_id" bigint NOT NULL,
    "read_count" integer DEFAULT 0 NOT NULL,
    "reason" "text" NOT NULL,
    "message" "jsonb" NOT NULL,
    "last_error" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);

ALTER TABLE "util"."dataset_extraction_job_failures" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_extraction_job_failures" IS 'Records compact dataset extraction jobs that exceeded retry caps or were marked terminal by the Edge worker.';

ALTER TABLE ONLY "util"."dataset_extraction_job_failures" ALTER COLUMN "id" SET DEFAULT "nextval"('"util"."dataset_extraction_job_failures_id_seq"'::"regclass");

ALTER TABLE ONLY "util"."dataset_extraction_job_failures"
    ADD CONSTRAINT "dataset_extraction_job_failures_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "util"."dataset_extraction_job_failures"
    ADD CONSTRAINT "dataset_extraction_job_failures_queue_name_msg_id_key" UNIQUE ("queue_name", "msg_id");

GRANT SELECT,INSERT,UPDATE ON TABLE "util"."dataset_extraction_job_failures" TO "service_role";
