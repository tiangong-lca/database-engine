CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_artifact_write_sets" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "closure_check_id" "uuid" NOT NULL,
    "worker_job_id" "uuid" NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "publication_mode" "text" NOT NULL,
    "reused_from_check_id" "uuid",
    "idempotency_key" "text" NOT NULL,
    "request_sha256" "text" NOT NULL,
    "status" "text" DEFAULT 'staging'::"text" NOT NULL,
    "write_token" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "staging_expires_at" timestamp with time zone NOT NULL,
    "reconcile_token" "uuid",
    "reconcile_claimed_at" timestamp with time zone,
    "reconcile_expires_at" timestamp with time zone,
    "failure_reason" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finalized_at" timestamp with time zone,
    "cleaned_at" timestamp with time zone,
    "contract_version" "text",
    "request_id" "uuid",
    "expected_descriptor_count" integer,
    "descriptor_set_sha256" "text",
    "required_primary_roles" "jsonb",
    "worker_lease_token_sha256" "text",
    "sealed_at" timestamp with time zone,
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_deadline_check" CHECK (("staging_expires_at" > "created_at")),
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_hash_check" CHECK (("request_sha256" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_key_check" CHECK ((("length"(TRIM(BOTH FROM "idempotency_key")) >= 1) AND ("length"(TRIM(BOTH FROM "idempotency_key")) <= 200))),
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_mode_check" CHECK (((("publication_mode" = 'fresh'::"text") AND ("reused_from_check_id" IS NULL)) OR (("publication_mode" = 'reused'::"text") AND ("reused_from_check_id" IS NOT NULL)))),
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_status_check" CHECK ((("status" = ANY (ARRAY['registration_open'::"text", 'staging'::"text", 'ready'::"text", 'cleanup_pending'::"text", 'cleaned'::"text"])) AND (("status" <> 'registration_open'::"text") OR ("contract_version" IS NOT NULL)))),
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_v2_seal_check" CHECK ((("contract_version" IS NULL) OR ("status" = ANY (ARRAY['registration_open'::"text", 'cleanup_pending'::"text", 'cleaned'::"text"])) OR ("sealed_at" IS NOT NULL))),
    CONSTRAINT "lcia_scope_closure_artifact_write_sets_v2_shape_check" CHECK (((("contract_version" IS NULL) AND ("request_id" IS NULL) AND ("expected_descriptor_count" IS NULL) AND ("descriptor_set_sha256" IS NULL) AND ("required_primary_roles" IS NULL) AND ("worker_lease_token_sha256" IS NULL) AND ("sealed_at" IS NULL)) OR (("contract_version" = 'lcia.scope-closure-artifact-write-set.v2'::"text") AND ("request_id" IS NOT NULL) AND (("expected_descriptor_count" >= 1) AND ("expected_descriptor_count" <= 100000)) AND ("descriptor_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("jsonb_typeof"("required_primary_roles") = 'array'::"text") AND ("worker_lease_token_sha256" ~ '^[a-f0-9]{64}$'::"text"))))
);

ALTER TABLE "public"."lcia_scope_closure_artifact_write_sets" OWNER TO "postgres";

COMMENT ON TABLE "public"."lcia_scope_closure_artifact_write_sets" IS 'DB-first scope-closure publication registry. Every possible uploaded object is registered before upload and becomes ready only through atomic finalize.';

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_sets"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_sets_owner_uidx" UNIQUE ("closure_check_id", "idempotency_key");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_sets"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_sets_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_sets"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_sets_request_uidx" UNIQUE ("closure_check_id", "request_id");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_sets"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_reused_from_check_id_fkey" FOREIGN KEY ("reused_from_check_id") REFERENCES "public"."lcia_scope_closure_checks"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_sets"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_sets_closure_check_id_fkey" FOREIGN KEY ("closure_check_id") REFERENCES "public"."lcia_scope_closure_checks"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_sets"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_sets_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE RESTRICT;

ALTER TABLE "public"."lcia_scope_closure_artifact_write_sets" ENABLE ROW LEVEL SECURITY;
