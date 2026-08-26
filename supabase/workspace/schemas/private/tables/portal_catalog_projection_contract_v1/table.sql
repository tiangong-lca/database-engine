CREATE TABLE IF NOT EXISTS "private"."portal_catalog_projection_contract_v1" (
    "contract_version" smallint NOT NULL,
    "manifest_schema" "text" NOT NULL,
    "function_identities" "text"[] NOT NULL,
    "manifest_sha256" "text" NOT NULL,
    "created_by_migration" "text" NOT NULL,
    CONSTRAINT "portal_catalog_projection_contract_v1_contract_version_check" CHECK (("contract_version" = 1)),
    CONSTRAINT "portal_catalog_projection_contract_v1_function_identities_check" CHECK (("cardinality"("function_identities") = 11)),
    CONSTRAINT "portal_catalog_projection_contract_v1_manifest_schema_check" CHECK (("manifest_schema" = 'portal.catalog-projection-function-manifest.v1'::"text")),
    CONSTRAINT "portal_catalog_projection_contract_v1_manifest_sha256_check" CHECK (("manifest_sha256" = 'b5e0aff9abbffcc8d2dacaf559a5d1a8c993c20b647d0c70f0e4fa18eb06d2dc'::"text")),
    CONSTRAINT "portal_catalog_projection_contract_v_created_by_migration_check" CHECK (("created_by_migration" = '20260826060422'::"text"))
);

ALTER TABLE ONLY "private"."portal_catalog_projection_contract_v1" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_projection_contract_v1" OWNER TO "postgres";

ALTER TABLE ONLY "private"."portal_catalog_projection_contract_v1"
    ADD CONSTRAINT "portal_catalog_projection_contract_v1_pkey" PRIMARY KEY ("contract_version");

ALTER TABLE "private"."portal_catalog_projection_contract_v1" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."portal_catalog_projection_contract_v1" TO "api_internal_executor";
