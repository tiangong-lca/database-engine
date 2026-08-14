CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_capture_source_guards" (
    "receipt_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "disposition" "text" NOT NULL,
    "source_id" "uuid" NOT NULL,
    "source_version" "text" NOT NULL,
    "guard" "jsonb" NOT NULL,
    "evidence_sha256" "text" NOT NULL,
    CONSTRAINT "dataset_flow_identity_capture_source_disposition_chk" CHECK (("disposition" = ANY (ARRAY['mapped'::"text", 'pending'::"text", 'blocker'::"text", 'orphan'::"text"]))),
    CONSTRAINT "dataset_flow_identity_capture_source_evidence_chk" CHECK (("evidence_sha256" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_flow_identity_capture_source_ordinal_chk" CHECK (("ordinal" > 0))
);

ALTER TABLE "util"."dataset_flow_identity_capture_source_guards" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_source_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_receipt_id_source_id_source_v_key" UNIQUE ("receipt_id", "source_id", "source_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_source_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_source_guards_pkey" PRIMARY KEY ("receipt_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_capture_source_guards"
    ADD CONSTRAINT "dataset_flow_identity_capture_source_guards_receipt_id_fkey" FOREIGN KEY ("receipt_id") REFERENCES "util"."dataset_flow_identity_capture_receipts"("id") ON DELETE RESTRICT;
