CREATE TABLE IF NOT EXISTS "private"."oauth_relation_capability_grants" (
    "relation_schema" "name" NOT NULL,
    "relation_name" "name" NOT NULL,
    "command" "text" NOT NULL,
    "capability_id" "text" NOT NULL,
    CONSTRAINT "oauth_relation_capability_grants_capability_check" CHECK ((("capability_id" = "btrim"("capability_id")) AND (("length"("capability_id") >= 1) AND ("length"("capability_id") <= 128)))),
    CONSTRAINT "oauth_relation_capability_grants_command_check" CHECK (("command" = ANY (ARRAY['select'::"text", 'insert'::"text", 'update'::"text", 'delete'::"text"])))
);

ALTER TABLE ONLY "private"."oauth_relation_capability_grants" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."oauth_relation_capability_grants" OWNER TO "postgres";

ALTER TABLE ONLY "private"."oauth_relation_capability_grants"
    ADD CONSTRAINT "oauth_relation_capability_grants_pkey" PRIMARY KEY ("relation_schema", "relation_name", "command");

ALTER TABLE "private"."oauth_relation_capability_grants" ENABLE ROW LEVEL SECURITY;
