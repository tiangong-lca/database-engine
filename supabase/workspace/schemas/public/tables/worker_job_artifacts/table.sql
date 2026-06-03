CREATE TABLE IF NOT EXISTS "public"."worker_job_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "artifact_type" "text" NOT NULL,
    "storage_bucket" "text",
    "storage_path" "text",
    "content_type" "text",
    "byte_size" bigint,
    "checksum_sha256" "text",
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "visibility" "text" DEFAULT 'operator'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "expires_at" timestamp with time zone,
    CONSTRAINT "worker_job_artifacts_byte_size_check" CHECK ((("byte_size" IS NULL) OR ("byte_size" >= 0))),
    CONSTRAINT "worker_job_artifacts_checksum_check" CHECK ((("checksum_sha256" IS NULL) OR ("checksum_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "worker_job_artifacts_metadata_object_check" CHECK (("jsonb_typeof"("metadata") = 'object'::"text")),
    CONSTRAINT "worker_job_artifacts_visibility_check" CHECK (("visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);

ALTER TABLE "public"."worker_job_artifacts" OWNER TO "postgres";

ALTER TABLE ONLY "public"."worker_job_artifacts"
    ADD CONSTRAINT "worker_job_artifacts_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."worker_job_artifacts"
    ADD CONSTRAINT "worker_job_artifacts_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE CASCADE;

ALTER TABLE "public"."worker_job_artifacts" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."worker_job_artifacts" TO "service_role";
