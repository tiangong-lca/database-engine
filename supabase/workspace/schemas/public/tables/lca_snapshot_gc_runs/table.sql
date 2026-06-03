CREATE TABLE IF NOT EXISTS "public"."lca_snapshot_gc_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "mode" "text" NOT NULL,
    "status" "text" DEFAULT 'running'::"text" NOT NULL,
    "started_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finished_at" timestamp with time zone,
    "as_of" timestamp with time zone DEFAULT "now"() NOT NULL,
    "snapshot_retention_window" interval DEFAULT '30 days'::interval NOT NULL,
    "orphan_retention_window" interval DEFAULT '30 days'::interval NOT NULL,
    "max_snapshots" integer DEFAULT 100 NOT NULL,
    "max_orphan_dirs" integer DEFAULT 200 NOT NULL,
    "max_bytes" bigint DEFAULT '2147483648'::bigint NOT NULL,
    "candidate_snapshot_count" integer DEFAULT 0 NOT NULL,
    "candidate_orphan_dir_count" integer DEFAULT 0 NOT NULL,
    "candidate_object_count" integer DEFAULT 0 NOT NULL,
    "candidate_storage_bytes" bigint DEFAULT 0 NOT NULL,
    "storage_deleted_count" integer DEFAULT 0 NOT NULL,
    "storage_failed_count" integer DEFAULT 0 NOT NULL,
    "db_snapshot_deleted_count" integer DEFAULT 0 NOT NULL,
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_snapshot_gc_runs_caps_chk" CHECK ((("max_snapshots" > 0) AND ("max_orphan_dirs" > 0) AND ("max_bytes" > 0))),
    CONSTRAINT "lca_snapshot_gc_runs_counts_chk" CHECK ((("candidate_snapshot_count" >= 0) AND ("candidate_orphan_dir_count" >= 0) AND ("candidate_object_count" >= 0) AND ("candidate_storage_bytes" >= 0) AND ("storage_deleted_count" >= 0) AND ("storage_failed_count" >= 0) AND ("db_snapshot_deleted_count" >= 0))),
    CONSTRAINT "lca_snapshot_gc_runs_mode_chk" CHECK (("mode" = ANY (ARRAY['dry_run'::"text", 'execute'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_runs_status_chk" CHECK (("status" = ANY (ARRAY['running'::"text", 'succeeded'::"text", 'failed'::"text", 'skipped'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_runs_windows_chk" CHECK ((("snapshot_retention_window" >= '1 day'::interval) AND ("orphan_retention_window" >= '1 day'::interval)))
);

ALTER TABLE "public"."lca_snapshot_gc_runs" OWNER TO "postgres";

COMMENT ON TABLE "public"."lca_snapshot_gc_runs" IS 'Audit header for worker-driven lca-results/snapshots object-aware garbage collection runs.';

ALTER TABLE ONLY "public"."lca_snapshot_gc_runs"
    ADD CONSTRAINT "lca_snapshot_gc_runs_pkey" PRIMARY KEY ("id");

ALTER TABLE "public"."lca_snapshot_gc_runs" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_snapshot_gc_runs" TO "anon";

GRANT ALL ON TABLE "public"."lca_snapshot_gc_runs" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_snapshot_gc_runs" TO "service_role";
