CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_capture_target_guards" (
    "receipt_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "target_id" "uuid" NOT NULL,
    "target_version" "text" NOT NULL,
    "guard" "jsonb" NOT NULL,
    CONSTRAINT "dataset_flow_identity_capture_target_ordinal_chk" CHECK (("ordinal" > 0))
);

ALTER TABLE "util"."dataset_flow_identity_capture_target_guards" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_target_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_receipt_id_target_id_target_v_key" UNIQUE ("receipt_id", "target_id", "target_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_target_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_target_guards_pkey" PRIMARY KEY ("receipt_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_target_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_target_guards_receipt_id_fkey" FOREIGN KEY ("receipt_id") REFERENCES "util"."dataset_flow_identity_capture_receipts"("id") ON DELETE RESTRICT;
