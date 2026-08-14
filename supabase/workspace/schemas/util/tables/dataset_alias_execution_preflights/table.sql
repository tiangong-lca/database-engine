CREATE TABLE IF NOT EXISTS "util"."dataset_alias_execution_preflights" (
    "id" "uuid" NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "actor_email" "text" NOT NULL,
    "environment" "text" NOT NULL,
    "project_ref" "text" NOT NULL,
    "target_visibility" "text" DEFAULT 'owner_draft'::"text" NOT NULL,
    "plan" "jsonb" NOT NULL,
    "freeze_envelope" "jsonb" NOT NULL,
    "approval_envelope" "jsonb" NOT NULL,
    "plan_sha256" "text" NOT NULL,
    "operation_id" "text" NOT NULL,
    "plan_request_sha256" "text" NOT NULL,
    "bindings" "jsonb" NOT NULL,
    "bindings_sha256" "text" NOT NULL,
    "expected" "jsonb" NOT NULL,
    "expected_sha256" "text" NOT NULL,
    "derivative_targets" "jsonb" NOT NULL,
    "derivative_targets_sha256" "text" NOT NULL,
    "gate_expectations" "jsonb" NOT NULL,
    "gate_expectations_sha256" "text" NOT NULL,
    "failure_baseline_sha256" "text" NOT NULL,
    "preflight_request_sha256" "text" NOT NULL,
    "preflight_proof_sha256" "text" NOT NULL,
    "freeze_sha256" "text" NOT NULL,
    "approval_identity_sha256" "text" NOT NULL,
    "token_sha256" "text" NOT NULL,
    "completed_at" timestamp with time zone NOT NULL,
    "expires_at" timestamp with time zone NOT NULL,
    "consumed_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "dataset_alias_execution_preflight_environment_check" CHECK (("environment" = ANY (ARRAY['production'::"text", 'preview'::"text", 'local'::"text"]))),
    CONSTRAINT "dataset_alias_execution_preflight_hashes_check" CHECK ((("plan_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("plan_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("bindings_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("expected_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("derivative_targets_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("gate_expectations_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("failure_baseline_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("preflight_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("preflight_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("freeze_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_identity_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("token_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "dataset_alias_execution_preflight_visibility_check" CHECK (("target_visibility" = 'owner_draft'::"text")),
    CONSTRAINT "dataset_alias_execution_preflight_window_check" CHECK ((("expires_at" = ("completed_at" + '00:03:00'::interval)) AND (("consumed_at" IS NULL) OR (("consumed_at" >= "completed_at") AND ("consumed_at" <= "expires_at")))))
);

ALTER TABLE "util"."dataset_alias_execution_preflights" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_alias_execution_preflights" IS 'Private server-clock proofs for rollback-only validation of one immutable owner-draft alias plan and its exact derivative target set.';

ALTER TABLE ONLY "util"."dataset_alias_execution_preflights"
    ADD CONSTRAINT "dataset_alias_execution_preflights_pkey" PRIMARY KEY ("id");
