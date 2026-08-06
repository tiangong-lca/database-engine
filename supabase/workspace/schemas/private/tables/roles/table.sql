CREATE TABLE IF NOT EXISTS "private"."roles" (
    "user_id" "uuid" NOT NULL,
    "team_id" "uuid" NOT NULL,
    "role" character varying(255) NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone,
    CONSTRAINT "roles_role_check" CHECK ((("role")::"text" = ANY (ARRAY['owner'::"text", 'admin'::"text", 'member'::"text", 'is_invited'::"text", 'rejected'::"text", 'review-admin'::"text", 'review-member'::"text", 'data_product_manager'::"text"])))
);

ALTER TABLE "private"."roles" OWNER TO "postgres";

ALTER TABLE ONLY "private"."roles"
    ADD CONSTRAINT "roles_pkey" PRIMARY KEY ("user_id", "team_id");

ALTER TABLE ONLY "private"."roles"
    ADD CONSTRAINT "roles_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");

ALTER TABLE "private"."roles" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."roles" TO "service_role";

GRANT SELECT ON TABLE "private"."roles" TO "api_internal_executor";

GRANT SELECT ON TABLE "private"."roles" TO "authenticated";
