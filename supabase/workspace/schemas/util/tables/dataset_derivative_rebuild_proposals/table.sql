CREATE TABLE IF NOT EXISTS "util"."dataset_derivative_rebuild_proposals" (
    "id" bigint NOT NULL,
    "request_id" "uuid" NOT NULL,
    "proposal_kind" "text" NOT NULL,
    "extracted_md" "text",
    "extracted_md_sha256" "text",
    "embedding_ft" "extensions"."vector"(1024),
    "embedding_ft_sha256" "text",
    "embedding_ft_at" timestamp with time zone,
    "source_extracted_md_sha256" "text",
    "source_role" "text" DEFAULT CURRENT_USER NOT NULL,
    "source_backend_pid" integer DEFAULT "pg_backend_pid"() NOT NULL,
    "status" "text" DEFAULT 'captured'::"text" NOT NULL,
    "captured_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    "committed_at" timestamp with time zone,
    "discarded_at" timestamp with time zone,
    CONSTRAINT "dataset_derivative_rebuild_proposal_kind_check" CHECK (("proposal_kind" = ANY (ARRAY['markdown'::"text", 'embedding'::"text"]))),
    CONSTRAINT "dataset_derivative_rebuild_proposal_payload_check" CHECK (((("proposal_kind" = 'markdown'::"text") AND ("extracted_md" IS NOT NULL) AND ("extracted_md_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("embedding_ft" IS NULL) AND ("embedding_ft_sha256" IS NULL) AND ("embedding_ft_at" IS NULL)) OR (("proposal_kind" = 'embedding'::"text") AND ("extracted_md" IS NULL) AND ("extracted_md_sha256" IS NULL) AND ("embedding_ft" IS NOT NULL) AND ("embedding_ft_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("embedding_ft_at" IS NOT NULL) AND ("source_extracted_md_sha256" ~ '^[a-f0-9]{64}$'::"text")))),
    CONSTRAINT "dataset_derivative_rebuild_proposal_status_check" CHECK (("status" = ANY (ARRAY['captured'::"text", 'accepted'::"text", 'committed'::"text", 'discarded'::"text"])))
);

ALTER TABLE "util"."dataset_derivative_rebuild_proposals" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_derivative_rebuild_proposals" IS 'Private staging area for external Markdown/vector writes captured while a derivative rebuild target is fenced.';

ALTER TABLE "util"."dataset_derivative_rebuild_proposals" ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "util"."dataset_derivative_rebuild_proposals_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY "util"."dataset_derivative_rebuild_proposals"
    ADD CONSTRAINT "dataset_derivative_rebuild_proposals_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "util"."dataset_derivative_rebuild_proposals"
    ADD CONSTRAINT "dataset_derivative_rebuild_proposals_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "util"."dataset_derivative_rebuild_requests"("id") ON DELETE CASCADE;
