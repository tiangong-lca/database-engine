CREATE TABLE IF NOT EXISTS "public"."lca_results" (
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
    CONSTRAINT "lca_results_artifact_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0)))
);

ALTER TABLE "public"."lca_results" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_results" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_results" TO "anon";

GRANT ALL ON TABLE "public"."lca_results" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_results" TO "service_role";
