CREATE TABLE IF NOT EXISTS "util"."dataset_derivative_rebuild_permits" (
    "request_id" "uuid" NOT NULL,
    "proposal_id" bigint NOT NULL,
    "permit_kind" "text" NOT NULL,
    "backend_pid" integer NOT NULL,
    "transaction_id" bigint NOT NULL,
    "created_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "dataset_derivative_rebuild_permit_kind_check" CHECK (("permit_kind" = ANY (ARRAY['markdown'::"text", 'embedding'::"text"])))
);

ALTER TABLE "util"."dataset_derivative_rebuild_permits" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_derivative_rebuild_permits" IS 'Transaction- and backend-bound permits used only by the private coordinator to commit one staged derivative proposal.';

ALTER TABLE ONLY "util"."dataset_derivative_rebuild_permits"
    ADD CONSTRAINT "dataset_derivative_rebuild_permits_pkey" PRIMARY KEY ("request_id", "proposal_id", "permit_kind");

ALTER TABLE ONLY "util"."dataset_derivative_rebuild_permits"
    ADD CONSTRAINT "dataset_derivative_rebuild_permits_proposal_id_fkey" FOREIGN KEY ("proposal_id") REFERENCES "util"."dataset_derivative_rebuild_proposals"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "util"."dataset_derivative_rebuild_permits"
    ADD CONSTRAINT "dataset_derivative_rebuild_permits_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "util"."dataset_derivative_rebuild_requests"("id") ON DELETE CASCADE;
