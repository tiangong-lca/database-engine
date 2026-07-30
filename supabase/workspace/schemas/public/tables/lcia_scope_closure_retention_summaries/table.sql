CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_retention_summaries" (
    "closure_check_id" "uuid" NOT NULL,
    "issue_count" bigint NOT NULL,
    "occurrence_count" bigint NOT NULL,
    "affected_root_count" bigint NOT NULL,
    "issue_content_hash" "text" NOT NULL,
    "compact_result_summary" "jsonb" NOT NULL,
    "retained_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_retention_counts_check" CHECK ((("issue_count" >= 0) AND ("occurrence_count" >= 0) AND ("affected_root_count" >= 0))),
    CONSTRAINT "lcia_scope_closure_retention_hash_check" CHECK (("issue_content_hash" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "lcia_scope_closure_retention_summary_check" CHECK (("jsonb_typeof"("compact_result_summary") = 'object'::"text"))
);

ALTER TABLE "public"."lcia_scope_closure_retention_summaries" OWNER TO "postgres";

COMMENT ON TABLE "public"."lcia_scope_closure_retention_summaries" IS 'Compact counts, result summary, and content hash retained after high-cardinality closure detail GC.';

ALTER TABLE ONLY "public"."lcia_scope_closure_retention_summaries"
    ADD CONSTRAINT "lcia_scope_closure_retention_summaries_pkey" PRIMARY KEY ("closure_check_id");

ALTER TABLE ONLY "public"."lcia_scope_closure_retention_summaries"
    ADD CONSTRAINT "lcia_scope_closure_retention_summaries_closure_check_id_fkey" FOREIGN KEY ("closure_check_id") REFERENCES "public"."lcia_scope_closure_checks"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lcia_scope_closure_retention_summaries" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lcia_scope_closure_retention_summaries" TO "service_role";
