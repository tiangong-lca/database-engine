CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_reviewed_lcia_methods" (
    "method_id" "uuid" NOT NULL,
    "method_version" "text" NOT NULL,
    "artifact_locator_id" "uuid" NOT NULL
);

ALTER TABLE "private"."lcia_scope_closure_reviewed_lcia_methods" OWNER TO "postgres";

COMMENT ON TABLE "private"."lcia_scope_closure_reviewed_lcia_methods" IS 'Database copy of the deployed Worker RELEASE_METHOD_IDENTITIES allowlist used only to freeze compatible first-release candidate snapshots.';

ALTER TABLE ONLY "private"."lcia_scope_closure_reviewed_lcia_methods"
    ADD CONSTRAINT "lcia_scope_closure_reviewed_l_artifact_locator_id_method_ve_key" UNIQUE ("artifact_locator_id", "method_version");

ALTER TABLE ONLY "private"."lcia_scope_closure_reviewed_lcia_methods"
    ADD CONSTRAINT "lcia_scope_closure_reviewed_lcia_methods_pkey" PRIMARY KEY ("method_id", "method_version");

GRANT SELECT ON TABLE "private"."lcia_scope_closure_reviewed_lcia_methods" TO "api_internal_executor";
