CREATE TABLE IF NOT EXISTS "public"."lca_snapshot_artifacts" (
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
    CONSTRAINT "lca_snapshot_artifacts_counts_chk" CHECK ((("process_count" >= 0) AND ("flow_count" >= 0) AND ("impact_count" >= 0) AND ("a_nnz" >= 0) AND ("b_nnz" >= 0) AND ("c_nnz" >= 0))),
    CONSTRAINT "lca_snapshot_artifacts_size_chk" CHECK (("artifact_byte_size" >= 0)),
    CONSTRAINT "lca_snapshot_artifacts_status_chk" CHECK (("status" = ANY (ARRAY['ready'::"text", 'stale'::"text", 'failed'::"text"])))
);

ALTER TABLE "public"."lca_snapshot_artifacts" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_snapshot_artifacts" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_snapshot_artifacts" TO "anon";

GRANT ALL ON TABLE "public"."lca_snapshot_artifacts" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_snapshot_artifacts" TO "service_role";
