CREATE TABLE IF NOT EXISTS "private"."lca_results" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "payload" "jsonb",
    "diagnostics" "jsonb",
    "artifact_url" "text",
    "artifact_sha256" "text",
    "artifact_byte_size" bigint,
    "artifact_format" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "worker_job_id" "uuid",
    "expires_at" timestamp with time zone DEFAULT ("now"() + '30 days'::interval) NOT NULL,
    "is_pinned" boolean DEFAULT false NOT NULL,
    CONSTRAINT "lca_results_artifact_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0)))
);

ALTER TABLE "private"."lca_results" OWNER TO "postgres";

COMMENT ON TABLE "private"."lca_results" IS 'Worker-produced LCA result artifact metadata. This is domain result state, not a task lifecycle/job table.';

ALTER TABLE ONLY "private"."lca_results"
    ADD CONSTRAINT "lca_results_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lca_results"
    ADD CONSTRAINT "lca_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "private"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "private"."lca_results"
    ADD CONSTRAINT "lca_results_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "private"."worker_jobs"("id") ON DELETE SET NULL;

ALTER TABLE "private"."lca_results" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lca_results" TO "service_role";

GRANT SELECT ON TABLE "private"."lca_results" TO "api_internal_executor";
