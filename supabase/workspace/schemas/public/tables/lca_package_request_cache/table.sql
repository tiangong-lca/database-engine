CREATE TABLE IF NOT EXISTS "public"."lca_package_request_cache" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "operation" "text" NOT NULL,
    "request_key" "text" NOT NULL,
    "request_payload" "jsonb" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "job_id" "uuid",
    "export_artifact_id" "uuid",
    "report_artifact_id" "uuid",
    "error_code" "text",
    "error_message" "text",
    "hit_count" bigint DEFAULT 0 NOT NULL,
    "last_accessed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_package_request_cache_hit_count_chk" CHECK (("hit_count" >= 0)),
    CONSTRAINT "lca_package_request_cache_operation_chk" CHECK (("operation" = ANY (ARRAY['export_package'::"text", 'import_package'::"text"]))),
    CONSTRAINT "lca_package_request_cache_request_key_chk" CHECK (("length"("btrim"("request_key")) > 0)),
    CONSTRAINT "lca_package_request_cache_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);

ALTER TABLE "public"."lca_package_request_cache" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_user_op_request_uk" UNIQUE ("requested_by", "operation", "request_key");

ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_export_artifact_fk" FOREIGN KEY ("export_artifact_id") REFERENCES "public"."lca_package_artifacts"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_package_jobs"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_report_artifact_fk" FOREIGN KEY ("report_artifact_id") REFERENCES "public"."lca_package_artifacts"("id") ON DELETE SET NULL;

ALTER TABLE "public"."lca_package_request_cache" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_package_request_cache" TO "anon";

GRANT ALL ON TABLE "public"."lca_package_request_cache" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_package_request_cache" TO "service_role";
