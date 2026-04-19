CREATE TABLE IF NOT EXISTS "public"."lca_latest_all_unit_results" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "job_id" "uuid" NOT NULL,
    "result_id" "uuid" NOT NULL,
    "query_artifact_url" "text" NOT NULL,
    "query_artifact_sha256" "text" NOT NULL,
    "query_artifact_byte_size" bigint NOT NULL,
    "query_artifact_format" "text" NOT NULL,
    "status" "text" DEFAULT 'ready'::"text" NOT NULL,
    "computed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_latest_all_unit_results_size_chk" CHECK (("query_artifact_byte_size" >= 0)),
    CONSTRAINT "lca_latest_all_unit_results_status_chk" CHECK (("status" = ANY (ARRAY['ready'::"text", 'stale'::"text", 'failed'::"text"])))
);

ALTER TABLE "public"."lca_latest_all_unit_results" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_snapshot_uk" UNIQUE ("snapshot_id");

ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_result_fk" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_latest_all_unit_results" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_latest_all_unit_results" TO "anon";

GRANT ALL ON TABLE "public"."lca_latest_all_unit_results" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_latest_all_unit_results" TO "service_role";
