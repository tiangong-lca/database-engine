CREATE TABLE IF NOT EXISTS "public"."lca_factorization_registry" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope" "text" DEFAULT 'prod'::"text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "backend" "text" DEFAULT 'umfpack'::"text" NOT NULL,
    "numeric_options_hash" "text" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "owner_worker_id" "text",
    "lease_until" timestamp with time zone,
    "prepared_job_id" "uuid",
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "prepared_at" timestamp with time zone,
    "last_used_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_factorization_registry_backend_chk" CHECK (("backend" = ANY (ARRAY['umfpack'::"text", 'cholmod'::"text", 'spqr'::"text"]))),
    CONSTRAINT "lca_factorization_registry_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'building'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);

ALTER TABLE "public"."lca_factorization_registry" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_scope_snapshot_backend_opts_uk" UNIQUE ("scope", "snapshot_id", "backend", "numeric_options_hash");

ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_prepared_job_fk" FOREIGN KEY ("prepared_job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lca_factorization_registry" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_factorization_registry" TO "anon";

GRANT ALL ON TABLE "public"."lca_factorization_registry" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_factorization_registry" TO "service_role";
