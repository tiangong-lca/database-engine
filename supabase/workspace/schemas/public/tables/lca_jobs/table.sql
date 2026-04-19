CREATE TABLE IF NOT EXISTS "public"."lca_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_type" "text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "payload" "jsonb",
    "diagnostics" "jsonb",
    "attempt" integer DEFAULT 0 NOT NULL,
    "max_attempt" integer DEFAULT 3 NOT NULL,
    "requested_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "request_key" "text",
    "idempotency_key" "text",
    CONSTRAINT "lca_jobs_attempt_chk" CHECK ((("attempt" >= 0) AND ("max_attempt" >= 0) AND ("attempt" <= "max_attempt"))),
    CONSTRAINT "lca_jobs_status_chk" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'ready'::"text", 'completed'::"text", 'failed'::"text", 'stale'::"text"]))),
    CONSTRAINT "lca_jobs_type_chk" CHECK (("job_type" = ANY (ARRAY['prepare_factorization'::"text", 'solve_one'::"text", 'solve_batch'::"text", 'solve_all_unit'::"text", 'invalidate_factorization'::"text", 'rebuild_factorization'::"text", 'build_snapshot'::"text", 'analyze_contribution_path'::"text"])))
);

ALTER TABLE "public"."lca_jobs" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_jobs"
    ADD CONSTRAINT "lca_jobs_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_jobs"
    ADD CONSTRAINT "lca_jobs_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_jobs" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_jobs" TO "anon";

GRANT ALL ON TABLE "public"."lca_jobs" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_jobs" TO "service_role";
