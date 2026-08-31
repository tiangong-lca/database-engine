CREATE TABLE IF NOT EXISTS "private"."oauth_client_registry" (
    "client_id" "text" NOT NULL,
    "client_kind" "text" NOT NULL,
    "enabled" boolean DEFAULT true NOT NULL,
    "created_at" timestamp with time zone DEFAULT "statement_timestamp"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "statement_timestamp"() NOT NULL,
    "disabled_at" timestamp with time zone,
    CONSTRAINT "oauth_client_registry_client_id_check" CHECK ((("client_id" = "btrim"("client_id")) AND (("length"("client_id") >= 1) AND ("length"("client_id") <= 256)))),
    CONSTRAINT "oauth_client_registry_client_kind_check" CHECK (("client_kind" ~ '^[a-z][a-z0-9_-]{1,63}$'::"text")),
    CONSTRAINT "oauth_client_registry_disabled_at_check" CHECK ((("enabled" AND ("disabled_at" IS NULL)) OR ((NOT "enabled") AND ("disabled_at" IS NOT NULL))))
);

ALTER TABLE ONLY "private"."oauth_client_registry" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."oauth_client_registry" OWNER TO "postgres";

ALTER TABLE ONLY "private"."oauth_client_registry"
    ADD CONSTRAINT "oauth_client_registry_pkey" PRIMARY KEY ("client_id");

ALTER TABLE "private"."oauth_client_registry" ENABLE ROW LEVEL SECURITY;
