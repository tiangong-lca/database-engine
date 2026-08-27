CREATE TABLE IF NOT EXISTS "private"."portal_sitemap_latest_rows_v1" (
    "dataset_kind" "text" NOT NULL,
    "id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "modified_at" timestamp with time zone NOT NULL,
    "shard_no" smallint NOT NULL,
    "contract_version" smallint NOT NULL,
    CONSTRAINT "portal_sitemap_latest_rows_v1_contract_version_check" CHECK (("contract_version" = 1)),
    CONSTRAINT "portal_sitemap_latest_rows_v1_dataset_kind_check" CHECK (("dataset_kind" = ANY (ARRAY['process'::"text", 'flow'::"text"]))),
    CONSTRAINT "portal_sitemap_latest_rows_v1_shard_no_check" CHECK ((("shard_no" >= 0) AND ("shard_no" <= 63))),
    CONSTRAINT "portal_sitemap_latest_rows_v1_version_check" CHECK (("version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text"))
);

ALTER TABLE ONLY "private"."portal_sitemap_latest_rows_v1" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_sitemap_latest_rows_v1" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_sitemap_latest_rows_v1" IS 'Latest visible exact Process/Flow identity and stable 64-way sitemap bucket; contains no card, document, actor, credential, or locator.';

ALTER TABLE ONLY "private"."portal_sitemap_latest_rows_v1"
    ADD CONSTRAINT "portal_sitemap_latest_rows_v1_pkey" PRIMARY KEY ("dataset_kind", "id");

ALTER TABLE "private"."portal_sitemap_latest_rows_v1" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "api_internal_executor";

GRANT SELECT("dataset_kind") ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "portal_public_executor";

GRANT SELECT("id") ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "portal_public_executor";

GRANT SELECT("version") ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "portal_public_executor";

GRANT SELECT("modified_at") ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "portal_public_executor";

GRANT SELECT("shard_no") ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "portal_public_executor";

GRANT SELECT("contract_version") ON TABLE "private"."portal_sitemap_latest_rows_v1" TO "portal_public_executor";
