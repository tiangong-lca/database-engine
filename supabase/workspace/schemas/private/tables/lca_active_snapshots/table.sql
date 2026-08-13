CREATE TABLE IF NOT EXISTS "private"."lca_active_snapshots" (
    "scope" "text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "source_hash" "text" NOT NULL,
    "activated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "activated_by" "uuid",
    "note" "text"
);

ALTER TABLE "private"."lca_active_snapshots" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_pkey" PRIMARY KEY ("scope");

ALTER TABLE ONLY "private"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "private"."lca_network_snapshots"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lca_active_snapshots" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lca_active_snapshots" TO "service_role";

GRANT SELECT ON TABLE "private"."lca_active_snapshots" TO "api_internal_executor";
