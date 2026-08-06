CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_capture_receipts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "actor_email" "text" NOT NULL,
    "request_id" "uuid" NOT NULL,
    "environment" "text" NOT NULL,
    "project_ref" "text" NOT NULL,
    "target_visibility" "text" NOT NULL,
    "operation_id" "text" NOT NULL,
    "compatibility_policy" "jsonb" NOT NULL,
    "policy_approval_text_sha256" "text" NOT NULL,
    "artifact_evidence" "jsonb" NOT NULL,
    "protected_closure" "jsonb" NOT NULL,
    "protected_closure_sha256" "text" NOT NULL,
    "source_universe" "jsonb" NOT NULL,
    "source_universe_sha256" "text" NOT NULL,
    "support_snapshot_set_sha256" "text" NOT NULL,
    "source_guard_set_sha256" "text" NOT NULL,
    "target_guard_set_sha256" "text" NOT NULL,
    "mapping_guard_set_sha256" "text" NOT NULL,
    "process_intent_set_sha256" "text" NOT NULL,
    "mapping_set_sha256" "text" NOT NULL,
    "process_manifest_sha256" "text" NOT NULL,
    "capture_request_sha256" "text" NOT NULL,
    "receipt_proof_sha256" "text" NOT NULL,
    "whole_scope_proof_sha256" "text" NOT NULL,
    "source_count" integer NOT NULL,
    "target_count" integer NOT NULL,
    "support_count" integer NOT NULL,
    "mapping_count" integer NOT NULL,
    "process_count" integer NOT NULL,
    "rewrite_count" integer NOT NULL,
    "captured_at" timestamp with time zone NOT NULL,
    "expires_at" timestamp with time zone NOT NULL,
    "created_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "dataset_flow_identity_capture_counts_chk" CHECK ((("source_count" = 305) AND ("target_count" > 0) AND ("support_count" >= 2) AND ("mapping_count" > 0) AND ("process_count" > 0) AND ("rewrite_count" > 0))),
    CONSTRAINT "dataset_flow_identity_capture_environment_chk" CHECK (("environment" = ANY (ARRAY['local'::"text", 'preview'::"text", 'production'::"text"]))),
    CONSTRAINT "dataset_flow_identity_capture_hashes_chk" CHECK ((("policy_approval_text_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("protected_closure_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("source_universe_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("support_snapshot_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("source_guard_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("target_guard_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("mapping_guard_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("process_intent_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("mapping_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("process_manifest_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("capture_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("receipt_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("whole_scope_proof_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "dataset_flow_identity_capture_lifetime_chk" CHECK ((("expires_at" > "captured_at") AND ("expires_at" <= ("captured_at" + '7 days'::interval)))),
    CONSTRAINT "dataset_flow_identity_capture_visibility_chk" CHECK (("target_visibility" = 'owner_draft'::"text"))
);

ALTER TABLE "util"."dataset_flow_identity_capture_receipts" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_receipts"
    ADD CONSTRAINT "dataset_flow_identity_capture_actor_user_id_receipt_proof_s_key" UNIQUE ("actor_user_id", "receipt_proof_sha256");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_receipts"
    ADD CONSTRAINT "dataset_flow_identity_capture_rece_actor_user_id_request_id_key" UNIQUE ("actor_user_id", "request_id");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_receipts"
    ADD CONSTRAINT "dataset_flow_identity_capture_receipts_pkey" PRIMARY KEY ("id");
