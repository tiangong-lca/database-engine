CREATE TABLE IF NOT EXISTS "private"."lca_release_publications" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "release_run_id" "uuid" NOT NULL,
    "release_version" "text" NOT NULL,
    "publication_series_key" "text" DEFAULT 'global'::"text" NOT NULL,
    "publication_channel" "text" DEFAULT 'public'::"text" NOT NULL,
    "visibility_scope" "text" DEFAULT 'public'::"text" NOT NULL,
    "status" "text" DEFAULT 'current'::"text" NOT NULL,
    "is_current" boolean DEFAULT true NOT NULL,
    "approval_id" "uuid" NOT NULL,
    "approval_hash" "text" NOT NULL,
    "publish_plan_hash" "text" NOT NULL,
    "release_manifest_hash" "text" NOT NULL,
    "artifact_set_hash" "text" NOT NULL,
    "approved_by" "uuid" NOT NULL,
    "executed_by" "uuid" NOT NULL,
    "credential_fingerprint" "text" NOT NULL,
    "idempotency_key" "text" NOT NULL,
    "published_at" timestamp with time zone NOT NULL,
    "superseded_by" "uuid",
    "superseded_at" timestamp with time zone,
    "unpublished_by" "uuid",
    "unpublished_at" timestamp with time zone,
    "reason" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_release_publications_hashes_chk" CHECK ((("approval_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("publish_plan_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("release_manifest_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("artifact_set_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("credential_fingerprint" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "lca_release_publications_scope_chk" CHECK ((("publication_series_key" = 'global'::"text") AND ("publication_channel" = 'public'::"text") AND ("visibility_scope" = 'public'::"text"))),
    CONSTRAINT "lca_release_publications_status_chk" CHECK (("status" = ANY (ARRAY['current'::"text", 'superseded'::"text", 'unpublished'::"text"])))
);

ALTER TABLE "private"."lca_release_publications" OWNER TO "postgres";

COMMENT ON TABLE "private"."lca_release_publications" IS 'Public release facts with distinct approved_by and executed_by audit identities.';

ALTER TABLE ONLY "private"."lca_release_publications"
    ADD CONSTRAINT "lca_release_publications_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lca_release_publications"
    ADD CONSTRAINT "lca_release_publications_release_run_id_key" UNIQUE ("release_run_id");

ALTER TABLE ONLY "private"."lca_release_publications"
    ADD CONSTRAINT "lca_release_publications_release_version_key" UNIQUE ("release_version");

ALTER TABLE ONLY "private"."lca_release_publications"
    ADD CONSTRAINT "lca_release_publications_approval_id_fkey" FOREIGN KEY ("approval_id") REFERENCES "private"."lca_release_approvals"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."lca_release_publications"
    ADD CONSTRAINT "lca_release_publications_release_run_id_fkey" FOREIGN KEY ("release_run_id") REFERENCES "private"."lca_release_runs"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."lca_release_publications"
    ADD CONSTRAINT "lca_release_publications_superseded_by_fkey" FOREIGN KEY ("superseded_by") REFERENCES "private"."lca_release_publications"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lca_release_publications" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."lca_release_publications" TO "api_internal_executor";
