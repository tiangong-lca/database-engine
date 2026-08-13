CREATE TABLE IF NOT EXISTS "private"."users" (
    "id" "uuid" NOT NULL,
    "raw_user_meta_data" "jsonb",
    "contact" "jsonb"
);

ALTER TABLE "private"."users" OWNER TO "postgres";

ALTER TABLE ONLY "private"."users"
    ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."users" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."users" TO "service_role";

GRANT SELECT ON TABLE "private"."users" TO "api_internal_executor";
