CREATE TABLE IF NOT EXISTS "private"."lcia_document_validation_evidence" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_type" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "canonical_content_hash" "text" NOT NULL,
    "document_validator_version" "text" NOT NULL,
    "document_validation_profile" "text" NOT NULL,
    "validation_report_schema_version" "text" NOT NULL,
    "validator_engine_fingerprint" "text" NOT NULL,
    "tidas_schema_lock_sha256" "text" NOT NULL,
    "status" "text" NOT NULL,
    "summary" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "issue_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "issue_artifact_hash" "text",
    "source_worker_job_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_document_validation_evidence_issue_artifact_ref_check" CHECK (("jsonb_typeof"("issue_artifact_ref") = 'object'::"text")),
    CONSTRAINT "lcia_document_validation_evidence_status_check" CHECK (("status" = ANY (ARRAY['passed'::"text", 'failed'::"text"]))),
    CONSTRAINT "lcia_document_validation_evidence_summary_check" CHECK (("jsonb_typeof"("summary") = 'object'::"text"))
);

ALTER TABLE "private"."lcia_document_validation_evidence" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lcia_document_validation_evidence"
    ADD CONSTRAINT "lcia_document_validation_evid_dataset_type_dataset_id_datas_key" UNIQUE ("dataset_type", "dataset_id", "dataset_version", "canonical_content_hash", "document_validator_version", "document_validation_profile", "validation_report_schema_version", "validator_engine_fingerprint", "tidas_schema_lock_sha256");

ALTER TABLE ONLY "private"."lcia_document_validation_evidence"
    ADD CONSTRAINT "lcia_document_validation_evidence_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lcia_document_validation_evidence"
    ADD CONSTRAINT "lcia_document_validation_evidence_source_worker_job_id_fkey" FOREIGN KEY ("source_worker_job_id") REFERENCES "private"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE "private"."lcia_document_validation_evidence" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lcia_document_validation_evidence" TO "service_role";

GRANT SELECT ON TABLE "private"."lcia_document_validation_evidence" TO "api_internal_executor";
