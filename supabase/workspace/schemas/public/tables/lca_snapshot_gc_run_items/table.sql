CREATE TABLE IF NOT EXISTS "public"."lca_snapshot_gc_run_items" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "candidate_type" "text" NOT NULL,
    "snapshot_id" "uuid",
    "bucket_id" "text" NOT NULL,
    "object_name" "text" NOT NULL,
    "storage_bytes" bigint DEFAULT 0 NOT NULL,
    "reason" "text" NOT NULL,
    "delete_db_snapshot" boolean DEFAULT false NOT NULL,
    "action_status" "text" DEFAULT 'planned'::"text" NOT NULL,
    "error_message" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_snapshot_gc_run_items_action_status_chk" CHECK (("action_status" = ANY (ARRAY['planned'::"text", 'dry_run'::"text", 'storage_deleted'::"text", 'storage_missing'::"text", 'storage_failed'::"text", 'db_deleted'::"text", 'skipped'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_run_items_candidate_type_chk" CHECK (("candidate_type" = ANY (ARRAY['snapshot_directory'::"text", 'orphan_storage_directory'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_run_items_storage_bytes_chk" CHECK (("storage_bytes" >= 0))
);

ALTER TABLE "public"."lca_snapshot_gc_run_items" OWNER TO "postgres";

COMMENT ON TABLE "public"."lca_snapshot_gc_run_items" IS 'Per-object audit items for worker-driven lca-results/snapshots object-aware garbage collection runs.';

ALTER TABLE ONLY "public"."lca_snapshot_gc_run_items"
    ADD CONSTRAINT "lca_snapshot_gc_run_items_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_snapshot_gc_run_items"
    ADD CONSTRAINT "lca_snapshot_gc_run_items_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."lca_snapshot_gc_runs"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_snapshot_gc_run_items" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_snapshot_gc_run_items" TO "anon";

GRANT ALL ON TABLE "public"."lca_snapshot_gc_run_items" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_snapshot_gc_run_items" TO "service_role";
