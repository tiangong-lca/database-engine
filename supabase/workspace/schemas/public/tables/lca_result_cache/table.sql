CREATE TABLE IF NOT EXISTS "public"."lca_result_cache" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope" "text" DEFAULT 'prod'::"text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "request_key" "text" NOT NULL,
    "request_payload" "jsonb" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "job_id" "uuid",
    "result_id" "uuid",
    "error_code" "text",
    "error_message" "text",
    "hit_count" bigint DEFAULT 0 NOT NULL,
    "last_accessed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_result_cache_hit_count_chk" CHECK (("hit_count" >= 0)),
    CONSTRAINT "lca_result_cache_request_key_chk" CHECK (("length"("request_key") > 0)),
    CONSTRAINT "lca_result_cache_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);

ALTER TABLE "public"."lca_result_cache" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_scope_snapshot_request_key_uk" UNIQUE ("scope", "snapshot_id", "request_key");

ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_result_fk" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_result_cache" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_result_cache" TO "anon";

GRANT ALL ON TABLE "public"."lca_result_cache" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_result_cache" TO "service_role";
