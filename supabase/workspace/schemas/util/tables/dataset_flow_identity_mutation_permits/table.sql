CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_mutation_permits" (
    "transaction_id" bigint NOT NULL,
    "scope_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "process_id" "uuid" NOT NULL,
    "process_version" "text" NOT NULL,
    "mutation_nonce" "uuid" NOT NULL,
    "before_payload_sha256" "text" NOT NULL,
    "after_payload_sha256" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "dataset_flow_identity_mutation_permit_hashes_chk" CHECK ((("before_payload_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("after_payload_sha256" ~ '^[a-f0-9]{64}$'::"text")))
);

ALTER TABLE "util"."dataset_flow_identity_mutation_permits" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_mutation_permits"
    ADD CONSTRAINT "dataset_flow_identity_mutation_permits_pkey" PRIMARY KEY ("transaction_id", "scope_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_mutation_permits"
    ADD CONSTRAINT "dataset_flow_identity_mutation_permits_scope_id_ordinal_fkey" FOREIGN KEY ("scope_id", "ordinal") REFERENCES "util"."dataset_flow_identity_process_ledger"("scope_id", "ordinal") ON DELETE RESTRICT;
