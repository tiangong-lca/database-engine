CREATE TABLE IF NOT EXISTS "private"."lca_snapshot_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "artifact_url" "text" NOT NULL,
    "artifact_sha256" "text" NOT NULL,
    "artifact_byte_size" bigint NOT NULL,
    "artifact_format" "text" NOT NULL,
    "process_count" integer NOT NULL,
    "flow_count" integer NOT NULL,
    "impact_count" integer NOT NULL,
    "a_nnz" bigint NOT NULL,
    "b_nnz" bigint NOT NULL,
    "c_nnz" bigint NOT NULL,
    "coverage" "jsonb",
    "status" "text" DEFAULT 'ready'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "snapshot_index_sha256" "text",
    "snapshot_build_contract_hash" "text",
    "effective_scope_hash" "text",
    "data_snapshot_token" "text",
    "closure_bundle_hash" "text",
    CONSTRAINT "lca_snapshot_artifacts_certificate_hashes_chk" CHECK (((("snapshot_index_sha256" IS NULL) OR ("snapshot_index_sha256" ~ '^[0-9a-f]{64}$'::"text")) AND (("snapshot_build_contract_hash" IS NULL) OR ("snapshot_build_contract_hash" ~ '^[0-9a-f]{64}$'::"text")) AND (("effective_scope_hash" IS NULL) OR ("effective_scope_hash" ~ '^[0-9a-f]{64}$'::"text")) AND (("closure_bundle_hash" IS NULL) OR ("closure_bundle_hash" ~ '^[0-9a-f]{64}$'::"text")))),
    CONSTRAINT "lca_snapshot_artifacts_counts_chk" CHECK ((("process_count" >= 0) AND ("flow_count" >= 0) AND ("impact_count" >= 0) AND ("a_nnz" >= 0) AND ("b_nnz" >= 0) AND ("c_nnz" >= 0))),
    CONSTRAINT "lca_snapshot_artifacts_size_chk" CHECK (("artifact_byte_size" >= 0)),
    CONSTRAINT "lca_snapshot_artifacts_status_chk" CHECK (("status" = ANY (ARRAY['ready'::"text", 'stale'::"text", 'failed'::"text"])))
);

ALTER TABLE "private"."lca_snapshot_artifacts" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "private"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "private"."lca_snapshot_artifacts" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lca_snapshot_artifacts" TO "service_role";

GRANT SELECT ON TABLE "private"."lca_snapshot_artifacts" TO "api_internal_executor";
