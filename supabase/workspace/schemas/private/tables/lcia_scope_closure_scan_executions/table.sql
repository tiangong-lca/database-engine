CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_scan_executions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "request_fingerprint" "text" NOT NULL,
    "requested_scope_hash" "text" NOT NULL,
    "policy_fingerprint" "text" NOT NULL,
    "data_snapshot_token" "text" NOT NULL,
    "validator_scanner_fingerprint" "text" NOT NULL,
    "scan_key" "text",
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "lease_token" "uuid",
    "leased_by_job_id" "uuid",
    "lease_expires_at" timestamp with time zone,
    "completed_check_id" "uuid",
    "source_fingerprint" "text",
    "evidence_hash" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    "numerical_snapshot_id" "uuid" NOT NULL,
    CONSTRAINT "lcia_scope_closure_scan_executions_check" CHECK ((("status" = 'running'::"text") = (("lease_token" IS NOT NULL) AND ("leased_by_job_id" IS NOT NULL) AND ("lease_expires_at" IS NOT NULL)))),
    CONSTRAINT "lcia_scope_closure_scan_executions_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'completed'::"text", 'failed'::"text"])))
);

ALTER TABLE "private"."lcia_scope_closure_scan_executions" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_numerical_snapshot_uidx" UNIQUE ("numerical_snapshot_id");

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_request_fingerprint_key" UNIQUE ("request_fingerprint");

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_scan_key_key" UNIQUE ("scan_key");

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_completed_check_id_fkey" FOREIGN KEY ("completed_check_id") REFERENCES "private"."lcia_scope_closure_checks"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_data_snapshot_token_fkey" FOREIGN KEY ("data_snapshot_token") REFERENCES "private"."lcia_scope_closure_data_snapshots"("data_snapshot_token") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."lcia_scope_closure_scan_executions"
    ADD CONSTRAINT "lcia_scope_closure_scan_executions_leased_by_job_id_fkey" FOREIGN KEY ("leased_by_job_id") REFERENCES "private"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE "private"."lcia_scope_closure_scan_executions" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "private"."lcia_scope_closure_scan_executions" TO "service_role";

GRANT SELECT ON TABLE "private"."lcia_scope_closure_scan_executions" TO "api_internal_executor";
