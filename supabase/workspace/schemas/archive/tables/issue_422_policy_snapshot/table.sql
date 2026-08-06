CREATE TABLE IF NOT EXISTS "archive"."issue_422_policy_snapshot" (
    "oid" "oid",
    "polrelid" "oid",
    "polname" "name",
    "polcmd" "char",
    "polpermissive" boolean,
    "polroles" "oid"[],
    "polqual" "pg_node_tree" COLLATE "pg_catalog"."C",
    "polwithcheck" "pg_node_tree" COLLATE "pg_catalog"."C"
);

ALTER TABLE "archive"."issue_422_policy_snapshot" OWNER TO "postgres";
