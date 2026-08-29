CREATE TABLE IF NOT EXISTS "private"."lca_package_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "artifact_kind" "text" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "artifact_url" "text" NOT NULL,
    "artifact_sha256" "text",
    "artifact_byte_size" bigint,
    "artifact_format" "text" NOT NULL,
    "content_type" "text" NOT NULL,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "expires_at" timestamp with time zone,
    "is_pinned" boolean DEFAULT false NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "worker_job_id" "uuid",
    CONSTRAINT "lca_package_artifacts_format_chk" CHECK (("artifact_format" = ANY (ARRAY['tidas-package-zip:v1'::"text", 'tidas-package-export-report:v1'::"text", 'tidas-package-import-report:v1'::"text"]))),
    CONSTRAINT "lca_package_artifacts_kind_chk" CHECK (("artifact_kind" = ANY (ARRAY['import_source'::"text", 'export_zip'::"text", 'export_report'::"text", 'import_report'::"text"]))),
    CONSTRAINT "lca_package_artifacts_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0))),
    CONSTRAINT "lca_package_artifacts_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'ready'::"text", 'failed'::"text", 'deleted'::"text"]))),
    CONSTRAINT "lca_package_artifacts_url_chk" CHECK (("length"("btrim"("artifact_url")) > 0))
);

ALTER TABLE "private"."lca_package_artifacts" OWNER TO "postgres";

COMMENT ON TABLE "private"."lca_package_artifacts" IS 'Worker-produced TIDAS package artifact metadata. This is a domain artifact table, not a task lifecycle/job table; canonical execution state lives in public.worker_jobs.';

ALTER TABLE ONLY "private"."lca_package_artifacts"
    ADD CONSTRAINT "lca_package_artifacts_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lca_package_artifacts"
    ADD CONSTRAINT "lca_package_artifacts_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "private"."worker_jobs"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lca_package_artifacts" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lca_package_artifacts" TO "service_role";

GRANT SELECT ON TABLE "private"."lca_package_artifacts" TO "api_internal_executor";
