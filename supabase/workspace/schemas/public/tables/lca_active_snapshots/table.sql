CREATE TABLE IF NOT EXISTS "public"."lca_active_snapshots" (
    "scope" "text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "source_hash" "text" NOT NULL,
    "activated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "activated_by" "uuid",
    "note" "text"
);

ALTER TABLE "public"."lca_active_snapshots" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_pkey" PRIMARY KEY ("scope");

ALTER TABLE ONLY "public"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE RESTRICT;

ALTER TABLE "public"."lca_active_snapshots" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_active_snapshots" TO "anon";

GRANT ALL ON TABLE "public"."lca_active_snapshots" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_active_snapshots" TO "service_role";
