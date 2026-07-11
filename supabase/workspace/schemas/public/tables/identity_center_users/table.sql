CREATE TABLE IF NOT EXISTS "public"."identity_center_users" (
    "keycloak_sub" "text" NOT NULL,
    "user_id" "uuid",
    "status" character varying(32) DEFAULT 'active'::character varying NOT NULL,
    "desired_role" character varying(64),
    "metadata" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone
);

ALTER TABLE "public"."identity_center_users" OWNER TO "postgres";

COMMENT ON TABLE "public"."identity_center_users" IS 'Identity Center SSO local user mapping (keycloak_sub is the cross-system key; email is never used). Service-role only: RLS enabled with no policies.';

ALTER TABLE ONLY "public"."identity_center_users"
    ADD CONSTRAINT "identity_center_users_pkey" PRIMARY KEY ("keycloak_sub");

ALTER TABLE ONLY "public"."identity_center_users"
    ADD CONSTRAINT "identity_center_users_user_id_key" UNIQUE ("user_id");

ALTER TABLE ONLY "public"."identity_center_users"
    ADD CONSTRAINT "identity_center_users_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");

ALTER TABLE "public"."identity_center_users" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."identity_center_users" TO "service_role";
