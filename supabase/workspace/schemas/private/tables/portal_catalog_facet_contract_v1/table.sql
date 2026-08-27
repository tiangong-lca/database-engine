CREATE TABLE IF NOT EXISTS "private"."portal_catalog_facet_contract_v1" (
    "contract_version" smallint NOT NULL,
    "manifest_schema" "text" NOT NULL,
    "function_identities" "text"[] NOT NULL,
    "manifest_sha256" "text" NOT NULL,
    "created_by_migration" "text" NOT NULL,
    CONSTRAINT "portal_catalog_facet_contract_digest_v1_chk" CHECK (("manifest_sha256" = 'b238e9573ef08a9339062a2fa3092c0776318d13979ec8bf54ffc7a1ba0c7e3a'::"text")),
    CONSTRAINT "portal_catalog_facet_contract_functions_v1_chk" CHECK (("function_identities" = ARRAY['private.portal_catalog_facet_facts_v1(text,jsonb)'::"text", 'private.sync_portal_catalog_facet_row_v1()'::"text"])),
    CONSTRAINT "portal_catalog_facet_contract_migration_v1_chk" CHECK (("created_by_migration" = '20260827020000'::"text")),
    CONSTRAINT "portal_catalog_facet_contract_schema_v1_chk" CHECK (("manifest_schema" = 'portal.catalog-facet-function-manifest.v1'::"text")),
    CONSTRAINT "portal_catalog_facet_contract_version_v1_chk" CHECK (("contract_version" = 1))
);

ALTER TABLE ONLY "private"."portal_catalog_facet_contract_v1" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_facet_contract_v1" OWNER TO "postgres";

ALTER TABLE ONLY "private"."portal_catalog_facet_contract_v1"
    ADD CONSTRAINT "portal_catalog_facet_contract_v1_pkey" PRIMARY KEY ("contract_version");

ALTER TABLE "private"."portal_catalog_facet_contract_v1" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."portal_catalog_facet_contract_v1" TO "api_internal_executor";
