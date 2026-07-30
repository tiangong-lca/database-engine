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
    "artifact_role" "text",
    "lifecycle_state" "text",
    "gc_claim_token" "uuid",
    "gc_claimed_at" timestamp with time zone,
    "gc_claim_expires_at" timestamp with time zone,
    "gc_failure_count" integer DEFAULT 0 NOT NULL,
    "gc_last_error" "text",
    "deleted_at" timestamp with time zone,
    "gc_cleanup_state" "text" DEFAULT 'none'::"text" NOT NULL,
    CONSTRAINT "worker_job_artifacts_artifact_role_check" CHECK ((NOT ("artifact_role" IS DISTINCT FROM "public"."lcia_scope_closure_artifact_role"("artifact_type")))),
    CONSTRAINT "worker_job_artifacts_byte_size_check" CHECK ((("byte_size" IS NULL) OR ("byte_size" >= 0))),
    CONSTRAINT "worker_job_artifacts_checksum_check" CHECK ((("checksum_sha256" IS NULL) OR ("checksum_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "worker_job_artifacts_gc_cleanup_lifecycle_check" CHECK (((("lifecycle_state" = 'deleted'::"text") AND ("gc_cleanup_state" = ANY (ARRAY['pending'::"text", 'complete'::"text"]))) OR (("lifecycle_state" IS DISTINCT FROM 'deleted'::"text") AND ("gc_cleanup_state" = 'none'::"text")) OR ("artifact_role" IS NULL))),
    CONSTRAINT "worker_job_artifacts_gc_cleanup_state_check" CHECK (("gc_cleanup_state" = ANY (ARRAY['none'::"text", 'pending'::"text", 'complete'::"text"]))),
    CONSTRAINT "worker_job_artifacts_gc_failure_count_check" CHECK (("gc_failure_count" >= 0)),
    CONSTRAINT "worker_job_artifacts_lifecycle_state_check" CHECK ((("lifecycle_state" IS NULL) OR ("lifecycle_state" = ANY (ARRAY['ready'::"text", 'expired'::"text", 'deleted'::"text"])))),
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
