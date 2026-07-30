CREATE TABLE IF NOT EXISTS "public"."lca_release_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "release_run_id" "uuid" NOT NULL,
    "profile_id" "text" NOT NULL,
    "artifact_format" "text" NOT NULL,
    "storage_bucket" "text" NOT NULL,
    "object_key" "text" NOT NULL,
    "sha256" "text" NOT NULL,
    "byte_size" bigint NOT NULL,
    "media_type" "text" NOT NULL,
    "closure_hash" "text" NOT NULL,
    "verified_at" timestamp with time zone NOT NULL,
    "pinned" boolean DEFAULT false NOT NULL,
    "published_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_release_artifacts_format_chk" CHECK (("artifact_format" = ANY (ARRAY['tidas'::"text", 'ilcd'::"text"]))),
    CONSTRAINT "lca_release_artifacts_profile_chk" CHECK (("profile_id" = ANY (ARRAY['unit-process-full-closure.v1'::"text", 'standalone-lifecyclemodel-result-full-closure.v1'::"text"]))),
    CONSTRAINT "lca_release_artifacts_sha_chk" CHECK ((("sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("closure_hash" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "lca_release_artifacts_size_chk" CHECK (("byte_size" >= 0)),
    CONSTRAINT "lca_release_artifacts_storage_chk" CHECK ((("length"(TRIM(BOTH FROM "storage_bucket")) > 0) AND ("length"(TRIM(BOTH FROM "object_key")) > 0)))
);

ALTER TABLE "public"."lca_release_artifacts" OWNER TO "postgres";

COMMENT ON TABLE "public"."lca_release_artifacts" IS 'Verified immutable TIDAS/ILCD package refs. Published rows remain pinned after supersede or unpublish.';

ALTER TABLE ONLY "public"."lca_release_artifacts"
    ADD CONSTRAINT "lca_release_artifacts_object_unique" UNIQUE ("storage_bucket", "object_key");

ALTER TABLE ONLY "public"."lca_release_artifacts"
    ADD CONSTRAINT "lca_release_artifacts_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_release_artifacts"
    ADD CONSTRAINT "lca_release_artifacts_profile_format_unique" UNIQUE ("release_run_id", "profile_id", "artifact_format");

ALTER TABLE ONLY "public"."lca_release_artifacts"
    ADD CONSTRAINT "lca_release_artifacts_release_run_id_fkey" FOREIGN KEY ("release_run_id") REFERENCES "public"."lca_release_runs"("id") ON DELETE RESTRICT;

ALTER TABLE "public"."lca_release_artifacts" ENABLE ROW LEVEL SECURITY;
