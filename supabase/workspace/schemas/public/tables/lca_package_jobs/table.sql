CREATE TABLE IF NOT EXISTS "public"."lca_package_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_type" "text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "payload" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "attempt" integer DEFAULT 0 NOT NULL,
    "max_attempt" integer DEFAULT 3 NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "scope" "text",
    "root_count" integer DEFAULT 0 NOT NULL,
    "request_key" "text",
    "idempotency_key" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_package_jobs_attempt_chk" CHECK ((("attempt" >= 0) AND ("max_attempt" >= 0) AND ("attempt" <= "max_attempt"))),
    CONSTRAINT "lca_package_jobs_idempotency_key_chk" CHECK ((("idempotency_key" IS NULL) OR ("length"("btrim"("idempotency_key")) > 0))),
    CONSTRAINT "lca_package_jobs_request_key_chk" CHECK ((("request_key" IS NULL) OR ("length"("btrim"("request_key")) > 0))),
    CONSTRAINT "lca_package_jobs_root_count_chk" CHECK (("root_count" >= 0)),
    CONSTRAINT "lca_package_jobs_status_chk" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'ready'::"text", 'completed'::"text", 'failed'::"text", 'stale'::"text"]))),
    CONSTRAINT "lca_package_jobs_type_chk" CHECK (("job_type" = ANY (ARRAY['export_package'::"text", 'import_package'::"text"])))
);

ALTER TABLE "public"."lca_package_jobs" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_package_jobs"
    ADD CONSTRAINT "lca_package_jobs_pkey" PRIMARY KEY ("id");

ALTER TABLE "public"."lca_package_jobs" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_package_jobs" TO "anon";

GRANT ALL ON TABLE "public"."lca_package_jobs" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_package_jobs" TO "service_role";
