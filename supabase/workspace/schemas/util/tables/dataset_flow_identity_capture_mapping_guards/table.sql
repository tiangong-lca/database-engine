CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_capture_mapping_guards" (
    "receipt_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "mapping_id" "text" NOT NULL,
    "source_id" "uuid" NOT NULL,
    "source_version" "text" NOT NULL,
    "target_id" "uuid" NOT NULL,
    "target_version" "text" NOT NULL,
    "mapping" "jsonb" NOT NULL,
    CONSTRAINT "dataset_flow_identity_capture_mapping_hash_chk" CHECK (("mapping_id" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_flow_identity_capture_mapping_ordinal_chk" CHECK (("ordinal" > 0))
);

ALTER TABLE "util"."dataset_flow_identity_capture_mapping_guards" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_mapping_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_mapping_guards_pkey" PRIMARY KEY ("receipt_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_mapping_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_mapping_receipt_id_mapping_id_key" UNIQUE ("receipt_id", "mapping_id");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_mapping_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_receipt_id_source_id_source__key1" UNIQUE ("receipt_id", "source_id", "source_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_mapping_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_mapping_guards_receipt_id_fkey" FOREIGN KEY ("receipt_id") REFERENCES "util"."dataset_flow_identity_capture_receipts"("id") ON DELETE RESTRICT;
