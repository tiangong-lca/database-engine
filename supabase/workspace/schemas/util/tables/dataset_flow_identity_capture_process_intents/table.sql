CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_capture_process_intents" (
    "receipt_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "process_id" "uuid" NOT NULL,
    "process_version" "text" NOT NULL,
    "intent_proof_sha256" "text" NOT NULL,
    "manifest" "jsonb" NOT NULL,
    CONSTRAINT "dataset_flow_identity_capture_process_ordinal_chk" CHECK (("ordinal" > 0)),
    CONSTRAINT "dataset_flow_identity_capture_process_proof_chk" CHECK (("intent_proof_sha256" ~ '^[a-f0-9]{64}$'::"text"))
);

ALTER TABLE "util"."dataset_flow_identity_capture_process_intents" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_process_intents"
    ADD CONSTRAINT "dataset_flow_identity_capture_process_intents_pkey" PRIMARY KEY ("receipt_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_process_intents"
    ADD CONSTRAINT "dataset_flow_identity_capture_receipt_id_process_id_process_key" UNIQUE ("receipt_id", "process_id", "process_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_process_intents"
    ADD CONSTRAINT "dataset_flow_identity_capture_process_intents_receipt_id_fkey" FOREIGN KEY ("receipt_id") REFERENCES "util"."dataset_flow_identity_capture_receipts"("id") ON DELETE RESTRICT;
