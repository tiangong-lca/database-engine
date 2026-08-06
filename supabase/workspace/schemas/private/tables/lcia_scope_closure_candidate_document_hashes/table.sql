CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_candidate_document_hashes" (
    "dataset_type" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "source_locator_id" "uuid" NOT NULL,
    "role" "text" NOT NULL,
    "canonical_content_hash" "text" NOT NULL,
    "source_modified_at" timestamp with time zone,
    "refreshed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_candidate_docum_canonical_content_hash_check" CHECK (("canonical_content_hash" ~ '^[0-9a-f]{64}$'::"text")),
    CONSTRAINT "lcia_scope_closure_candidate_document_hashes_dataset_type_check" CHECK (("dataset_type" = ANY (ARRAY['contacts'::"text", 'flowproperties'::"text", 'flows'::"text", 'lciamethods'::"text", 'lifecyclemodels'::"text", 'processes'::"text", 'sources'::"text", 'unitgroups'::"text"]))),
    CONSTRAINT "lcia_scope_closure_candidate_document_hashes_role_check" CHECK (("role" = ANY (ARRAY['unit_process'::"text", 'support'::"text"])))
);

ALTER TABLE "private"."lcia_scope_closure_candidate_document_hashes" OWNER TO "postgres";

COMMENT ON TABLE "private"."lcia_scope_closure_candidate_document_hashes" IS 'Incremental Worker-canonical content hashes for candidate-public-state closure snapshots. Backfilled once by migration and maintained by table triggers.';

ALTER TABLE ONLY "private"."lcia_scope_closure_candidate_document_hashes"
    ADD CONSTRAINT "lcia_scope_closure_candidate__dataset_type_source_locator_i_key" UNIQUE ("dataset_type", "source_locator_id", "dataset_version");

ALTER TABLE ONLY "private"."lcia_scope_closure_candidate_document_hashes"
    ADD CONSTRAINT "lcia_scope_closure_candidate_document_hashes_pkey" PRIMARY KEY ("dataset_type", "dataset_id", "dataset_version");

GRANT SELECT ON TABLE "private"."lcia_scope_closure_candidate_document_hashes" TO "api_internal_executor";
