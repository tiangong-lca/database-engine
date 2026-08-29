CREATE TABLE IF NOT EXISTS "private"."portal_catalog_character_rows_v1" (
    "dataset_kind" "text" NOT NULL,
    "id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "state_code" integer NOT NULL,
    "modified_at" timestamp with time zone NOT NULL,
    "document_characters" "text" NOT NULL,
    "name_characters" "text" NOT NULL,
    "name_exact_characters" "text" NOT NULL,
    "classification_characters" "text" NOT NULL,
    "classification_exact_characters" "text" NOT NULL,
    "character_contract_version" smallint DEFAULT 1 NOT NULL,
    CONSTRAINT "portal_catalog_character_rows__character_contract_version_check" CHECK (("character_contract_version" = 1)),
    CONSTRAINT "portal_catalog_character_rows_v1_dataset_kind_check" CHECK (("dataset_kind" = ANY (ARRAY['process'::"text", 'flow'::"text"]))),
    CONSTRAINT "portal_catalog_character_rows_v1_state_code_check" CHECK (("state_code" = ANY (ARRAY[100, 200]))),
    CONSTRAINT "portal_catalog_character_rows_v1_version_check" CHECK (("version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text"))
);

ALTER TABLE ONLY "private"."portal_catalog_character_rows_v1" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_character_rows_v1" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_catalog_character_rows_v1" IS 'Narrow exact-version public character sets for bounded one-code-point Search pre-limit; parent FK and INSERT/UPDATE trigger keep it synchronized.';

ALTER TABLE ONLY "private"."portal_catalog_character_rows_v1"
    ADD CONSTRAINT "portal_catalog_character_rows_v1_pkey" PRIMARY KEY ("dataset_kind", "id", "version");

ALTER TABLE ONLY "private"."portal_catalog_character_rows_v1"
    ADD CONSTRAINT "portal_catalog_character_parent_v1_fk" FOREIGN KEY ("dataset_kind", "id", "version") REFERENCES "private"."portal_catalog_search_rows_v1"("dataset_kind", "id", "version") ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE "private"."portal_catalog_character_rows_v1" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "private"."portal_catalog_character_rows_v1" TO "api_internal_executor";

GRANT SELECT("dataset_kind") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("id") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("version") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("state_code") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("modified_at") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("document_characters") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("name_characters") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("name_exact_characters") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("classification_characters") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";

GRANT SELECT("classification_exact_characters") ON TABLE "private"."portal_catalog_character_rows_v1" TO "portal_public_executor";
