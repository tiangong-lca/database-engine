CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_config" (
    "singleton" boolean DEFAULT true NOT NULL,
    "expected_validator_scanner_fingerprint" "text" NOT NULL,
    "require_certificate_for_builds" boolean DEFAULT false NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_config_singleton_check" CHECK ("singleton")
);

ALTER TABLE "private"."lcia_scope_closure_config" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lcia_scope_closure_config"
    ADD CONSTRAINT "lcia_scope_closure_config_pkey" PRIMARY KEY ("singleton");

ALTER TABLE "private"."lcia_scope_closure_config" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."lcia_scope_closure_config" TO "service_role";

GRANT SELECT ON TABLE "private"."lcia_scope_closure_config" TO "api_internal_executor";
