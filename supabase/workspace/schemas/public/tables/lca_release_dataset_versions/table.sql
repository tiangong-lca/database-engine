CREATE TABLE IF NOT EXISTS "public"."lca_release_dataset_versions" (
    "id" bigint NOT NULL,
    "release_run_id" "uuid" NOT NULL,
    "dataset_type" "text" NOT NULL,
    "dataset_role" "text" NOT NULL,
    "dataset_uuid" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "source_process_uuid" "uuid",
    "source_process_version" "text",
    "version_significant_hash" "text" NOT NULL,
    "semantic_hash" "text" NOT NULL,
    "canonical_content_hash" "text" NOT NULL,
    "artifact_ref" "jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_release_dataset_artifact_chk" CHECK (("jsonb_typeof"("artifact_ref") = 'object'::"text")),
    CONSTRAINT "lca_release_dataset_hashes_chk" CHECK ((("version_significant_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("semantic_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("canonical_content_hash" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "lca_release_dataset_role_chk" CHECK (("dataset_role" = ANY (ARRAY['unit_process'::"text", 'result_process'::"text", 'lifecycle_model'::"text", 'support'::"text"]))),
    CONSTRAINT "lca_release_dataset_role_type_chk" CHECK (((("dataset_role" = ANY (ARRAY['unit_process'::"text", 'result_process'::"text"])) AND ("dataset_type" = 'process'::"text")) OR (("dataset_role" = 'lifecycle_model'::"text") AND ("dataset_type" = 'lifecyclemodel'::"text")) OR ("dataset_role" = 'support'::"text"))),
    CONSTRAINT "lca_release_dataset_source_process_chk" CHECK (((("dataset_role" = ANY (ARRAY['unit_process'::"text", 'result_process'::"text", 'lifecycle_model'::"text"])) AND ("source_process_uuid" IS NOT NULL) AND ("source_process_version" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text")) OR (("dataset_role" = 'support'::"text") AND ("source_process_uuid" IS NULL) AND ("source_process_version" IS NULL)))),
    CONSTRAINT "lca_release_dataset_type_chk" CHECK (("dataset_type" = ANY (ARRAY['process'::"text", 'lifecyclemodel'::"text", 'flow'::"text", 'flowproperty'::"text", 'unitgroup'::"text", 'lciamethod'::"text", 'source'::"text", 'contact'::"text"]))),
    CONSTRAINT "lca_release_dataset_version_chk" CHECK (("dataset_version" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text")),
    CONSTRAINT "lca_release_unit_source_self_chk" CHECK ((("dataset_role" <> 'unit_process'::"text") OR (("source_process_uuid" = "dataset_uuid") AND ("source_process_version" = "dataset_version"))))
);

ALTER TABLE "public"."lca_release_dataset_versions" OWNER TO "postgres";

COMMENT ON TABLE "public"."lca_release_dataset_versions" IS 'Read/index projection for exact UUID+version release datasets; generated Model/Result datasets are not inserted into authoring tables.';

ALTER TABLE "public"."lca_release_dataset_versions" ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "public"."lca_release_dataset_versions_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY "public"."lca_release_dataset_versions"
    ADD CONSTRAINT "lca_release_dataset_key_unique" UNIQUE ("release_run_id", "dataset_type", "dataset_uuid", "dataset_version");

ALTER TABLE ONLY "public"."lca_release_dataset_versions"
    ADD CONSTRAINT "lca_release_dataset_versions_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_release_dataset_versions"
    ADD CONSTRAINT "lca_release_dataset_versions_release_run_id_fkey" FOREIGN KEY ("release_run_id") REFERENCES "public"."lca_release_runs"("id") ON DELETE RESTRICT;

ALTER TABLE "public"."lca_release_dataset_versions" ENABLE ROW LEVEL SECURITY;
