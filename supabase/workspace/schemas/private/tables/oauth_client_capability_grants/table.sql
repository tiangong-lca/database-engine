CREATE TABLE IF NOT EXISTS "private"."oauth_client_capability_grants" (
    "client_id" "text" NOT NULL,
    "capability_id" "text" NOT NULL,
    "allowed" boolean DEFAULT true NOT NULL,
    "granted_at" timestamp with time zone DEFAULT "statement_timestamp"() NOT NULL,
    CONSTRAINT "oauth_client_capability_grants_capability_check" CHECK ((("capability_id" = "btrim"("capability_id")) AND (("length"("capability_id") >= 1) AND ("length"("capability_id") <= 128))))
);

ALTER TABLE ONLY "private"."oauth_client_capability_grants" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."oauth_client_capability_grants" OWNER TO "postgres";

ALTER TABLE ONLY "private"."oauth_client_capability_grants"
    ADD CONSTRAINT "oauth_client_capability_grants_pkey" PRIMARY KEY ("client_id", "capability_id");

ALTER TABLE ONLY "private"."oauth_client_capability_grants"
    ADD CONSTRAINT "oauth_client_capability_grants_client_id_fkey" FOREIGN KEY ("client_id") REFERENCES "private"."oauth_client_registry"("client_id") ON DELETE CASCADE;

ALTER TABLE "private"."oauth_client_capability_grants" ENABLE ROW LEVEL SECURITY;
