CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_process_ledger" (
    "scope_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "process_id" "uuid" NOT NULL,
    "process_version" "text" NOT NULL,
    "manifest" "jsonb" NOT NULL,
    "process_template_sha256" "text" NOT NULL,
    "process_intent_proof_sha256" "text" NOT NULL,
    "process_request_sha256" "text",
    "rewrite_count" integer NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "active" boolean DEFAULT true NOT NULL,
    "mutation_nonce" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "audit_id" bigint,
    "before_payload_sha256" "text" NOT NULL,
    "after_payload_sha256" "text",
    "after_exchange_set_sha256" "text",
    "derivative_batch_id" "uuid",
    "derivative_admission" "jsonb",
    "wrapper_invocation_id" "uuid",
    "permit_generation_before" integer,
    "completed_at" timestamp with time zone,
    "last_error" "jsonb",
    CONSTRAINT "dataset_flow_identity_process_hash_chk" CHECK ((("process_template_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("process_intent_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND (("process_request_sha256" IS NULL) OR ("process_request_sha256" ~ '^[a-f0-9]{64}$'::"text")) AND ("before_payload_sha256" ~ '^[a-f0-9]{64}$'::"text") AND (("after_payload_sha256" IS NULL) OR ("after_payload_sha256" ~ '^[a-f0-9]{64}$'::"text")) AND (("after_exchange_set_sha256" IS NULL) OR ("after_exchange_set_sha256" ~ '^[a-f0-9]{64}$'::"text")))),
    CONSTRAINT "dataset_flow_identity_process_ordinal_chk" CHECK (("ordinal" > 0)),
    CONSTRAINT "dataset_flow_identity_process_permit_generation_chk" CHECK ((("permit_generation_before" IS NULL) OR ("permit_generation_before" >= 0))),
    CONSTRAINT "dataset_flow_identity_process_rewrite_count_chk" CHECK (("rewrite_count" > 0)),
    CONSTRAINT "dataset_flow_identity_process_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'completed'::"text", 'failed'::"text"]))),
    CONSTRAINT "dataset_flow_identity_process_version_chk" CHECK (("process_version" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text"))
);

ALTER TABLE "util"."dataset_flow_identity_process_ledger" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_flow_identity_process_ledger" IS 'Private ordered one-process Step 3 replay ledger. Active rows exclude overlapping live scopes; completed rows bind one primary audit and one protected derivative batch.';

ALTER TABLE ONLY "util"."dataset_flow_identity_process_ledger"
    ADD CONSTRAINT "dataset_flow_identity_process_ledger_pkey" PRIMARY KEY ("scope_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_process_ledger"
    ADD CONSTRAINT "dataset_flow_identity_process_scope_id_process_id_process_v_key" UNIQUE ("scope_id", "process_id", "process_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_process_ledger"
    ADD CONSTRAINT "dataset_flow_identity_process_invocation_fk" FOREIGN KEY ("wrapper_invocation_id") REFERENCES "util"."dataset_flow_identity_wrapper_invocations"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "util"."dataset_flow_identity_process_ledger"
    ADD CONSTRAINT "dataset_flow_identity_process_ledger_audit_id_fkey" FOREIGN KEY ("audit_id") REFERENCES "private"."command_audit_log"("id");

ALTER TABLE ONLY "util"."dataset_flow_identity_process_ledger"
    ADD CONSTRAINT "dataset_flow_identity_process_ledger_scope_id_fkey" FOREIGN KEY ("scope_id") REFERENCES "util"."dataset_flow_identity_scopes"("id") ON DELETE RESTRICT;
