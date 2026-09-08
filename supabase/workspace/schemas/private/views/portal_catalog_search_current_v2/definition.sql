CREATE OR REPLACE VIEW "private"."portal_catalog_search_current_v2" WITH ("security_invoker"='true') AS
 SELECT "portal_catalog_search_rows_v2"."dataset_kind",
    "portal_catalog_search_rows_v2"."id",
    "portal_catalog_search_rows_v2"."version",
    "portal_catalog_search_rows_v2"."state_code",
    "portal_catalog_search_rows_v2"."modified_at",
    "portal_catalog_search_rows_v2"."card",
    "portal_catalog_search_rows_v2"."document"
   FROM "private"."portal_catalog_search_rows_v2"
  WHERE ("portal_catalog_search_rows_v2"."dataset_kind" = 'process'::"text")
UNION ALL
 SELECT "portal_catalog_search_rows_v1"."dataset_kind",
    "portal_catalog_search_rows_v1"."id",
    "portal_catalog_search_rows_v1"."version",
    "portal_catalog_search_rows_v1"."state_code",
    "portal_catalog_search_rows_v1"."modified_at",
    "portal_catalog_search_rows_v1"."card",
    "portal_catalog_search_rows_v1"."document"
   FROM "private"."portal_catalog_search_rows_v1"
  WHERE ("portal_catalog_search_rows_v1"."dataset_kind" = 'flow'::"text");

ALTER VIEW "private"."portal_catalog_search_current_v2" OWNER TO "postgres";
