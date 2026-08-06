CREATE TABLE IF NOT EXISTS "util"."dataset_alias_execution_gate_receipts" (
    "preflight_id" "uuid" NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "gate_name" "text" NOT NULL,
    "expected_sha256" "text" NOT NULL,
    "observed_sha256" "text" NOT NULL,
    "material" "jsonb" NOT NULL,
    "status" "text" NOT NULL,
    "captured_at" timestamp with time zone NOT NULL,
    "receipt_sha256" "text" NOT NULL,
    CONSTRAINT "dataset_alias_execution_gate_hashes_check" CHECK ((("expected_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("observed_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("receipt_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "dataset_alias_execution_gate_name_check" CHECK (("gate_name" = ANY (ARRAY['primary_support_plan'::"text", 'execution_unused'::"text", 'derivative_quiescence'::"text"]))),
    CONSTRAINT "dataset_alias_execution_gate_status_check" CHECK (("status" = 'passed'::"text"))
);

ALTER TABLE "util"."dataset_alias_execution_gate_receipts" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_alias_execution_gate_receipts" IS 'Private, one-per-name server receipts for the three post-preflight live gates. Admission accepts only exact receipts persisted inside the same 180-second window.';

ALTER TABLE ONLY "util"."dataset_alias_execution_gate_receipts"
    ADD CONSTRAINT "dataset_alias_execution_gate_receipts_pkey" PRIMARY KEY ("preflight_id", "gate_name");

ALTER TABLE ONLY "util"."dataset_alias_execution_gate_receipts"
    ADD CONSTRAINT "dataset_alias_execution_gate_receipts_preflight_id_fkey" FOREIGN KEY ("preflight_id") REFERENCES "util"."dataset_alias_execution_preflights"("id") ON DELETE CASCADE;
