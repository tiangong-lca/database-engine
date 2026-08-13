CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_checks" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "worker_job_id" "uuid" NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "request_idempotency_token" "text" NOT NULL,
    "request_key" "text" NOT NULL,
    "request_fingerprint" "text" NOT NULL,
    "requested_scope_hash" "text" NOT NULL,
    "effective_scope_hash" "text",
    "policy_fingerprint" "text" NOT NULL,
    "data_snapshot_token" "text" NOT NULL,
    "expected_validator_scanner_fingerprint" "text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "scan_completeness" "text",
    "certificate_status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "certificate_hash" "text",
    "report_artifact_id" "uuid",
    "result_summary" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "blocker_codes" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finished_at" timestamp with time zone,
    "requested_scope_manifest" "jsonb",
    "effective_scope_manifest" "jsonb",
    "certificate_schema_version" "text",
    "source_fingerprint" "text",
    "resolution_map_hash" "text",
    "closure_bundle_hash" "text",
    "snapshot_id" "uuid",
    "snapshot_hash" "text",
    "report_artifact_manifest_hash" "text",
    "evidence_hash" "text",
    "scan_execution_id" "uuid",
    "reused_from_check_id" "uuid",
    "closure_bundle_artifact_id" "uuid",
    "snapshot_artifact_id" "uuid",
    "snapshot_index_sha256" "text",
    "snapshot_build_contract_hash" "text",
    "complete_machine_result_artifact_id" "uuid",
    "valid_until" timestamp with time zone,
    CONSTRAINT "lcia_scope_closure_checks_certificate_check" CHECK (("certificate_status" = ANY (ARRAY['pending'::"text", 'valid'::"text", 'stale'::"text", 'revoked'::"text", 'unavailable'::"text"]))),
    CONSTRAINT "lcia_scope_closure_checks_completeness_check" CHECK ((("scan_completeness" IS NULL) OR ("scan_completeness" = ANY (ARRAY['complete'::"text", 'incomplete'::"text", 'unknown'::"text"])))),
    CONSTRAINT "lcia_scope_closure_checks_effective_scope_manifest_check" CHECK ((("effective_scope_manifest" IS NULL) OR ("jsonb_typeof"("effective_scope_manifest") = 'object'::"text"))),
    CONSTRAINT "lcia_scope_closure_checks_hash_check" CHECK ((("length"(TRIM(BOTH FROM "requested_scope_hash")) > 0) AND ("length"(TRIM(BOTH FROM "policy_fingerprint")) > 0))),
    CONSTRAINT "lcia_scope_closure_checks_request_token_check" CHECK ((("length"(TRIM(BOTH FROM "request_idempotency_token")) >= 1) AND ("length"(TRIM(BOTH FROM "request_idempotency_token")) <= 200))),
    CONSTRAINT "lcia_scope_closure_checks_requested_scope_manifest_check" CHECK ((("requested_scope_manifest" IS NULL) OR ("jsonb_typeof"("requested_scope_manifest") = 'object'::"text"))),
    CONSTRAINT "lcia_scope_closure_checks_result_summary_check" CHECK (("jsonb_typeof"("result_summary") = 'object'::"text")),
    CONSTRAINT "lcia_scope_closure_checks_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'passed'::"text", 'blocked'::"text", 'failed'::"text", 'cancelled'::"text"])))
);

ALTER TABLE "private"."lcia_scope_closure_checks" OWNER TO "postgres";

ALTER TABLE "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_certificate_snapshot_chk" CHECK ((("status" <> 'passed'::"text") OR ("certificate_status" <> ALL (ARRAY['valid'::"text", 'stale'::"text", 'revoked'::"text"])) OR (("scan_completeness" = 'complete'::"text") AND ("effective_scope_manifest" IS NOT NULL) AND ("effective_scope_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("source_fingerprint" IS NOT NULL) AND ("resolution_map_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("closure_bundle_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("closure_bundle_artifact_id" IS NOT NULL) AND ("snapshot_id" IS NOT NULL) AND ("snapshot_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("snapshot_artifact_id" IS NOT NULL) AND ("snapshot_index_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("snapshot_build_contract_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("evidence_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("certificate_hash" ~ '^[0-9a-f]{64}$'::"text")))) NOT VALID;

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_valid_until_check" CHECK ((("certificate_status" <> 'valid'::"text") OR (("complete_machine_result_artifact_id" IS NOT NULL) AND ("valid_until" IS NOT NULL) AND ("valid_until" > "finished_at")))) NOT VALID;

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_worker_job_id_key" UNIQUE ("worker_job_id");

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_closure_bundle_artifact_fkey" FOREIGN KEY ("closure_bundle_artifact_id") REFERENCES "private"."worker_job_artifacts"("id") ON DELETE RESTRICT NOT VALID;

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_report_artifact_id_fkey" FOREIGN KEY ("report_artifact_id") REFERENCES "private"."worker_job_artifacts"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_reused_from_check_id_fkey" FOREIGN KEY ("reused_from_check_id") REFERENCES "private"."lcia_scope_closure_checks"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_scan_execution_id_fkey" FOREIGN KEY ("scan_execution_id") REFERENCES "private"."lcia_scope_closure_scan_executions"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."lcia_scope_closure_checks"
    ADD CONSTRAINT "lcia_scope_closure_checks_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "private"."worker_jobs"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lcia_scope_closure_checks" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "private"."lcia_scope_closure_checks" TO "service_role";

GRANT SELECT ON TABLE "private"."lcia_scope_closure_checks" TO "api_internal_executor";
