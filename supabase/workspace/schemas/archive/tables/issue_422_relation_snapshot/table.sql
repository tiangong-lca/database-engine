CREATE TABLE IF NOT EXISTS "archive"."issue_422_relation_snapshot" (
    "object_oid" "oid",
    "object_name" "name",
    "object_kind" "char",
    "target_schema" "name" COLLATE "pg_catalog"."default",
    "exact_rows" bigint
);

ALTER TABLE "archive"."issue_422_relation_snapshot" OWNER TO "postgres";
