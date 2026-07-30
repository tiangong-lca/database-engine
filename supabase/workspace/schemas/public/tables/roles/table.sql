CREATE TABLE IF NOT EXISTS "public"."roles" (
    "user_id" "uuid" NOT NULL,
    "team_id" "uuid" NOT NULL,
    "role" character varying(255) NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone,
    CONSTRAINT "roles_role_check" CHECK ((("role")::"text" = ANY (ARRAY['owner'::"text", 'admin'::"text", 'member'::"text", 'is_invited'::"text", 'rejected'::"text", 'review-admin'::"text", 'review-member'::"text", 'data_product_manager'::"text"])))
);

ALTER TABLE "public"."roles" OWNER TO "postgres";

ALTER TABLE ONLY "public"."roles"
    ADD CONSTRAINT "roles_pkey" PRIMARY KEY ("user_id", "team_id");

ALTER TABLE ONLY "public"."roles"
    ADD CONSTRAINT "roles_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");

ALTER TABLE "public"."roles" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."roles" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."roles" TO "authenticated";

GRANT ALL ON TABLE "public"."roles" TO "service_role";
