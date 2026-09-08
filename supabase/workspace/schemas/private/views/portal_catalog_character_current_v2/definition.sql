CREATE OR REPLACE VIEW "private"."portal_catalog_character_current_v2" WITH ("security_invoker"='true') AS
 SELECT "portal_catalog_character_rows_v2"."dataset_kind",
    "portal_catalog_character_rows_v2"."id",
    "portal_catalog_character_rows_v2"."version",
    "portal_catalog_character_rows_v2"."state_code",
    "portal_catalog_character_rows_v2"."modified_at",
    "portal_catalog_character_rows_v2"."document_characters",
    "portal_catalog_character_rows_v2"."name_characters",
    "portal_catalog_character_rows_v2"."name_exact_characters",
    "portal_catalog_character_rows_v2"."classification_characters",
    "portal_catalog_character_rows_v2"."classification_exact_characters"
   FROM "private"."portal_catalog_character_rows_v2"
  WHERE ("portal_catalog_character_rows_v2"."dataset_kind" = 'process'::"text")
UNION ALL
 SELECT "portal_catalog_character_rows_v1"."dataset_kind",
    "portal_catalog_character_rows_v1"."id",
    "portal_catalog_character_rows_v1"."version",
    "portal_catalog_character_rows_v1"."state_code",
    "portal_catalog_character_rows_v1"."modified_at",
    "portal_catalog_character_rows_v1"."document_characters",
    "portal_catalog_character_rows_v1"."name_characters",
    "portal_catalog_character_rows_v1"."name_exact_characters",
    "portal_catalog_character_rows_v1"."classification_characters",
    "portal_catalog_character_rows_v1"."classification_exact_characters"
   FROM "private"."portal_catalog_character_rows_v1"
  WHERE ("portal_catalog_character_rows_v1"."dataset_kind" = 'flow'::"text");

ALTER VIEW "private"."portal_catalog_character_current_v2" OWNER TO "postgres";
