CREATE TABLE IF NOT EXISTS "archive"."worker_legacy_job_table_rows" (
    "archive_id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "archived_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "source_table" "text" NOT NULL,
    "source_row_id" "uuid",
    "source_created_at" timestamp with time zone,
    "source_modified_at" timestamp with time zone,
    "archive_reason" "text" DEFAULT 'worker_jobs_cutover_legacy_table_retirement'::"text" NOT NULL,
    "row_payload" "jsonb" NOT NULL,
    CONSTRAINT "worker_legacy_job_table_rows_payload_check" CHECK (("jsonb_typeof"("row_payload") = 'object'::"text")),
    CONSTRAINT "worker_legacy_job_table_rows_source_check" CHECK (("source_table" = ANY (ARRAY['public.lca_jobs'::"text", 'public.lca_package_jobs'::"text", 'public.dataset_review_submit_jobs'::"text"])))
);

ALTER TABLE "archive"."worker_legacy_job_table_rows" OWNER TO "postgres";

COMMENT ON TABLE "archive"."worker_legacy_job_table_rows" IS 'Manual rollback archive for retired legacy job table rows. Retained without automatic TTL deletion until explicit operator signoff because it is the last DB-local restore source for public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs.';

ALTER TABLE ONLY "archive"."worker_legacy_job_table_rows"
    ADD CONSTRAINT "worker_legacy_job_table_rows_pkey" PRIMARY KEY ("archive_id");

ALTER TABLE "archive"."worker_legacy_job_table_rows" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,INSERT,UPDATE ON TABLE "archive"."worker_legacy_job_table_rows" TO "service_role";
