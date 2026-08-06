CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_mappings" (
    "scope_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "mapping_id" "text" NOT NULL,
    "source_id" "uuid" NOT NULL,
    "source_version" "text" NOT NULL,
    "target_id" "uuid" NOT NULL,
    "target_version" "text" NOT NULL,
    "mapping" "jsonb" NOT NULL,
    CONSTRAINT "dataset_flow_identity_mapping_hash_chk" CHECK (("mapping_id" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_flow_identity_mapping_ordinal_chk" CHECK (("ordinal" > 0)),
    CONSTRAINT "dataset_flow_identity_mapping_versions_chk" CHECK ((("source_version" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text") AND ("target_version" ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'::"text")))
);

ALTER TABLE "util"."dataset_flow_identity_mappings" OWNER TO "postgres";

ALTER TABLE ONLY "util"."dataset_flow_identity_mappings"
    ADD CONSTRAINT "dataset_flow_identity_mapping_scope_id_source_id_source_ver_key" UNIQUE ("scope_id", "source_id", "source_version");

ALTER TABLE ONLY "util"."dataset_flow_identity_mappings"
    ADD CONSTRAINT "dataset_flow_identity_mappings_pkey" PRIMARY KEY ("scope_id", "ordinal");

ALTER TABLE ONLY "util"."dataset_flow_identity_mappings"
    ADD CONSTRAINT "dataset_flow_identity_mappings_scope_id_mapping_id_key" UNIQUE ("scope_id", "mapping_id");

ALTER TABLE ONLY "util"."dataset_flow_identity_mappings"
    ADD CONSTRAINT "dataset_flow_identity_mappings_scope_id_fkey" FOREIGN KEY ("scope_id") REFERENCES "util"."dataset_flow_identity_scopes"("id") ON DELETE RESTRICT;
