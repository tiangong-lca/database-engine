CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_capture_support_guards" (
    "receipt_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "support_table" "text" NOT NULL,
    "support_id" "uuid" NOT NULL,
    "support_version" "text" NOT NULL,
    "guard" "jsonb" NOT NULL,
    CONSTRAINT "dataset_flow_identity_capture_support_ordinal_chk" CHECK (("ordinal" > 0)),
    CONSTRAINT "dataset_flow_identity_capture_support_table_chk" CHECK (("support_table" = ANY (ARRAY['flowproperties'::"text", 'unitgroups'::"text"])))
);

ALTER TABLE "util"."dataset_flow_identity_capture_support_guards" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_support_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_receipt_id_support_table_supp_key" UNIQUE ("receipt_id", "support_table", "support_id", "support_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_support_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_support_guards_pkey" PRIMARY KEY ("receipt_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_support_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_support_guards_receipt_id_fkey" FOREIGN KEY ("receipt_id") REFERENCES "util"."dataset_flow_identity_capture_receipts"("id") ON DELETE RESTRICT;
