CREATE TABLE IF NOT EXISTS "private"."users" (
    "id" "uuid" NOT NULL,
    "raw_user_meta_data" "jsonb",
    "contact" "jsonb",
    CONSTRAINT "users_organization_metadata_contract" CHECK ((("raw_user_meta_data" IS NULL) OR (NOT ("raw_user_meta_data" ? 'organization'::"text")) OR (("jsonb_typeof"(("raw_user_meta_data" -> 'organization'::"text")) = 'string'::"text") AND (("raw_user_meta_data" ->> 'organization'::"text") !~ '^[[:space:]]|[[:space:]]$'::"text") AND ("char_length"(("raw_user_meta_data" ->> 'organization'::"text")) <= 200))))
);

ALTER TABLE "private"."users" OWNER TO "postgres";

ALTER TABLE ONLY "private"."users"
    ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."users" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."users" TO "service_role";

GRANT SELECT ON TABLE "private"."users" TO "api_internal_executor";
