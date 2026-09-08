CREATE TABLE IF NOT EXISTS "private"."portal_catalog_projection_contract_v2" (
    "contract_version" smallint NOT NULL,
    "manifest_schema" "text" NOT NULL,
    "function_identities" "text"[] NOT NULL,
    "manifest_sha256" "text" NOT NULL,
    "created_by_migration" "text" NOT NULL,
    CONSTRAINT "portal_catalog_projection_contract_v1_contract_version_check" CHECK (("contract_version" = 2)),
    CONSTRAINT "portal_catalog_projection_contract_v1_function_identities_check" CHECK (("cardinality"("function_identities") = 12)),
    CONSTRAINT "portal_catalog_projection_contract_v1_manifest_schema_check" CHECK (("manifest_schema" = 'portal.catalog-projection-function-manifest.v2'::"text")),
    CONSTRAINT "portal_catalog_projection_contract_v1_manifest_sha256_check" CHECK (("manifest_sha256" = '5260ed0b5662bf6b4bdae5250d0971fca369d8f36766f32207df47acd68e3500'::"text")),
    CONSTRAINT "portal_catalog_projection_contract_v2_created_by_migration_chec" CHECK (("created_by_migration" = '20260908090000'::"text"))
);

ALTER TABLE ONLY "private"."portal_catalog_projection_contract_v2" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_projection_contract_v2" OWNER TO "postgres";

ALTER TABLE ONLY "private"."portal_catalog_projection_contract_v2"
    ADD CONSTRAINT "portal_catalog_projection_contract_v2_pkey" PRIMARY KEY ("contract_version");

ALTER TABLE "private"."portal_catalog_projection_contract_v2" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."portal_catalog_projection_contract_v2" TO "api_internal_executor";
