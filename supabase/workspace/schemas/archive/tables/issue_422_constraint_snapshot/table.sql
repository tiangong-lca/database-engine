CREATE TABLE IF NOT EXISTS "archive"."issue_422_constraint_snapshot" (
    "oid" "oid",
    "conrelid" "oid",
    "confrelid" "oid",
    "contype" "char",
    "convalidated" boolean
);

ALTER TABLE "archive"."issue_422_constraint_snapshot" OWNER TO "postgres";
