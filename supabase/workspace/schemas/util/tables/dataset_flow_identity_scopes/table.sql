CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_scopes" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "receipt_id" "uuid" DEFAULT (NULLIF("current_setting"('app.dataset_flow_identity_receipt_id'::"text", true), ''::"text"))::"uuid" NOT NULL,
    "receipt_proof_sha256" "text" DEFAULT NULLIF("current_setting"('app.dataset_flow_identity_receipt_proof_sha256'::"text", true), ''::"text") NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "actor_email" "text" NOT NULL,
    "request_id" "uuid" NOT NULL,
    "environment" "text" NOT NULL,
    "project_ref" "text" NOT NULL,
    "target_visibility" "text" NOT NULL,
    "user_state_claim" "text" DEFAULT 'authenticated_actor_state_100_plus_own_state_0'::"text" NOT NULL,
    "operation_id" "text" NOT NULL,
    "plan_sha256" "text" NOT NULL,
    "freeze_sha256" "text" NOT NULL,
    "approval_identity_sha256" "text" NOT NULL,
    "approval_text_sha256" "text" NOT NULL,
    "policy_approval_text_sha256" "text" DEFAULT NULLIF("current_setting"('app.dataset_flow_identity_policy_approval_sha256'::"text", true), ''::"text") NOT NULL,
    "execution_approval_request_sha256" "text" DEFAULT NULLIF("current_setting"('app.dataset_flow_identity_execution_request_sha256'::"text", true), ''::"text") NOT NULL,
    "toolchain_evidence_sha256" "text" NOT NULL,
    "compatibility_policy" "jsonb" NOT NULL,
    "support_snapshot_set_sha256" "text" NOT NULL,
    "support_snapshots" "jsonb" NOT NULL,
    "source_universe_sha256" "text" NOT NULL,
    "source_universe" "jsonb" NOT NULL,
    "source_universe_count" integer NOT NULL,
    "mapping_set_sha256" "text" NOT NULL,
    "process_manifest_sha256" "text" NOT NULL,
    "protected_closure_sha256" "text" NOT NULL,
    "protected_closure" "jsonb" NOT NULL,
    "preflight_request_sha256" "text" DEFAULT NULLIF("current_setting"('app.dataset_flow_identity_preflight_request_sha256'::"text", true), ''::"text") NOT NULL,
    "scope_request_sha256" "text" NOT NULL,
    "scope_proof_sha256" "text" NOT NULL,
    "status" "text" DEFAULT 'sealed'::"text" NOT NULL,
    "mapping_count" integer NOT NULL,
    "process_count" integer NOT NULL,
    "rewrite_count" integer NOT NULL,
    "final_request_sha256" "text",
    "cancel_request_sha256" "text",
    "terminal_proof_sha256" "text",
    "final_wrapper_invocation_id" "uuid",
    "final_permit_generation_before" integer,
    "last_error" "jsonb",
    "sealed_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    "primary_completed_at" timestamp with time zone,
    "completed_at" timestamp with time zone,
    "updated_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "dataset_flow_identity_scope_approval_domains_chk" CHECK ((("policy_approval_text_sha256" <> "execution_approval_request_sha256") AND ("policy_approval_text_sha256" <> "approval_text_sha256") AND ("policy_approval_text_sha256" <> "approval_identity_sha256") AND ("execution_approval_request_sha256" <> "approval_text_sha256") AND ("execution_approval_request_sha256" <> "approval_identity_sha256") AND ("approval_text_sha256" <> "approval_identity_sha256"))),
    CONSTRAINT "dataset_flow_identity_scope_counts_chk" CHECK ((("mapping_count" > 0) AND ("process_count" > 0) AND ("rewrite_count" > 0))),
    CONSTRAINT "dataset_flow_identity_scope_environment_chk" CHECK (("environment" = ANY (ARRAY['local'::"text", 'preview'::"text", 'production'::"text"]))),
    CONSTRAINT "dataset_flow_identity_scope_final_permit_generation_chk" CHECK ((("final_permit_generation_before" IS NULL) OR ("final_permit_generation_before" >= 0))),
    CONSTRAINT "dataset_flow_identity_scope_hashes_chk" CHECK ((("plan_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("freeze_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_identity_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_text_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("policy_approval_text_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("execution_approval_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("receipt_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("toolchain_evidence_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("support_snapshot_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("source_universe_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("mapping_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("process_manifest_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("protected_closure_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("preflight_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("scope_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("scope_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND (("final_request_sha256" IS NULL) OR ("final_request_sha256" ~ '^[a-f0-9]{64}$'::"text")) AND (("cancel_request_sha256" IS NULL) OR ("cancel_request_sha256" ~ '^[a-f0-9]{64}$'::"text")) AND (("terminal_proof_sha256" IS NULL) OR ("terminal_proof_sha256" ~ '^[a-f0-9]{64}$'::"text")))),
    CONSTRAINT "dataset_flow_identity_scope_status_chk" CHECK (("status" = ANY (ARRAY['sealed'::"text", 'running'::"text", 'primary_complete'::"text", 'derivatives_pending'::"text", 'completed'::"text", 'failed'::"text", 'cancelled'::"text"]))),
    CONSTRAINT "dataset_flow_identity_scope_universe_chk" CHECK (("source_universe_count" = 305)),
    CONSTRAINT "dataset_flow_identity_scope_user_state_claim_chk" CHECK (("user_state_claim" = 'authenticated_actor_state_100_plus_own_state_0'::"text")),
    CONSTRAINT "dataset_flow_identity_scope_visibility_chk" CHECK (("target_visibility" = 'owner_draft'::"text"))
);

ALTER TABLE "util"."dataset_flow_identity_scopes" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_flow_identity_scopes" IS 'Private durable Step 3 scope seals. approval_identity_sha256 and approval_text_sha256 are execution-approval hashes; policy_approval_text_sha256 is independently bound from the capture receipt. Scope rows bind exact mapping, process, and protected pending/blocker closure evidence.';

ALTER TABLE ONLY "util"."dataset_flow_identity_scopes"
    ADD CONSTRAINT "dataset_flow_identity_scopes_actor_user_id_operation_id_key" UNIQUE ("actor_user_id", "operation_id");

ALTER TABLE ONLY "util"."dataset_flow_identity_scopes"
    ADD CONSTRAINT "dataset_flow_identity_scopes_actor_user_id_plan_sha256_key" UNIQUE ("actor_user_id", "plan_sha256");

ALTER TABLE ONLY "util"."dataset_flow_identity_scopes"
    ADD CONSTRAINT "dataset_flow_identity_scopes_actor_user_id_request_id_key" UNIQUE ("actor_user_id", "request_id");

ALTER TABLE ONLY "util"."dataset_flow_identity_scopes"
    ADD CONSTRAINT "dataset_flow_identity_scopes_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "util"."dataset_flow_identity_scopes"
    ADD CONSTRAINT "dataset_flow_identity_scope_final_invocation_fk" FOREIGN KEY ("final_wrapper_invocation_id") REFERENCES "util"."dataset_flow_identity_wrapper_invocations"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "util"."dataset_flow_identity_scopes"
    ADD CONSTRAINT "dataset_flow_identity_scopes_receipt_id_fkey" FOREIGN KEY ("receipt_id") REFERENCES "util"."dataset_flow_identity_capture_receipts"("id") ON DELETE RESTRICT;
