CREATE TABLE IF NOT EXISTS "private"."api_capability_grants" (
    "routine_identity" "text" NOT NULL,
    "capability_id" "text" NOT NULL,
    "allow_anon" boolean DEFAULT false NOT NULL,
    "allow_authenticated" boolean DEFAULT false NOT NULL,
    "allow_service_role" boolean DEFAULT false NOT NULL,
    CONSTRAINT "api_capability_grants_has_role_check" CHECK (("allow_anon" OR "allow_authenticated" OR "allow_service_role"))
);

ALTER TABLE "private"."api_capability_grants" OWNER TO "postgres";

ALTER TABLE ONLY "private"."api_capability_grants"
    ADD CONSTRAINT "api_capability_grants_pkey" PRIMARY KEY ("routine_identity");
