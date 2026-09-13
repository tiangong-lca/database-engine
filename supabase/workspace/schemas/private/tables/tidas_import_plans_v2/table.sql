CREATE TABLE IF NOT EXISTS "private"."tidas_import_plans_v2" (
    "worker_job_id" "uuid" NOT NULL,
    "source_artifact_id" "uuid" NOT NULL,
    "source_sha256" "text" NOT NULL,
    "plan_sha256" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "tidas_import_plans_v2_plan_sha256_check" CHECK (("plan_sha256" ~ '^[0-9a-f]{64}$'::"text")),
    CONSTRAINT "tidas_import_plans_v2_source_sha256_check" CHECK (("source_sha256" ~ '^[0-9a-f]{64}$'::"text"))
);

ALTER TABLE "private"."tidas_import_plans_v2" OWNER TO "postgres";

ALTER TABLE ONLY "private"."tidas_import_plans_v2"
    ADD CONSTRAINT "tidas_import_plans_v2_pkey" PRIMARY KEY ("worker_job_id");

ALTER TABLE ONLY "private"."tidas_import_plans_v2"
    ADD CONSTRAINT "tidas_import_plans_v2_source_artifact_id_fkey" FOREIGN KEY ("source_artifact_id") REFERENCES "private"."lca_package_artifacts"("id");

ALTER TABLE ONLY "private"."tidas_import_plans_v2"
    ADD CONSTRAINT "tidas_import_plans_v2_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "private"."worker_jobs"("id");

ALTER TABLE "private"."tidas_import_plans_v2" ENABLE ROW LEVEL SECURITY;
