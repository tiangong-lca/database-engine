CREATE TABLE IF NOT EXISTS "util"."dataset_alias_execution_requests" (
    "id" "uuid" NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "plan_sha256" "text" NOT NULL,
    "operation_id" "text" NOT NULL,
    "plan_request_sha256" "text" NOT NULL,
    "freeze_sha256" "text" NOT NULL,
    "approval_identity_sha256" "text" NOT NULL,
    "approval_text_sha256" "text" NOT NULL,
    "derivative_target_set_sha256" "text" NOT NULL,
    "preflight_proof_sha256" "text" NOT NULL,
    "admission_request_sha256" "text" NOT NULL,
    "gate_results" "jsonb" NOT NULL,
    "gate_results_sha256" "text" NOT NULL,
    "nonce_sha256" "text" NOT NULL,
    "attempt_count" smallint DEFAULT 1 NOT NULL,
    "dispatch_count" smallint DEFAULT 0 NOT NULL,
    "net_request_id" bigint,
    "status" "text" DEFAULT 'dispatching'::"text" NOT NULL,
    "admitted_at" timestamp with time zone NOT NULL,
    "dispatched_at" timestamp with time zone,
    "started_at" timestamp with time zone,
    "primary_committed_at" timestamp with time zone,
    "terminal_at" timestamp with time zone,
    "alias_result" "jsonb",
    "derivative_admission" "jsonb",
    "terminal_proof" "jsonb",
    "last_error" "jsonb",
    "created_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "dataset_alias_execution_request_attempt_check" CHECK ((("attempt_count" = 1) AND ("dispatch_count" = ANY (ARRAY[0, 1])) AND ((("dispatch_count" = 0) AND ("net_request_id" IS NULL) AND ("dispatched_at" IS NULL)) OR (("dispatch_count" = 1) AND ("net_request_id" IS NOT NULL) AND ("dispatched_at" IS NOT NULL))))),
    CONSTRAINT "dataset_alias_execution_request_hashes_check" CHECK ((("plan_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("plan_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("freeze_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_identity_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_text_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("derivative_target_set_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("preflight_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("admission_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("gate_results_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("nonce_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "dataset_alias_execution_request_status_check" CHECK (("status" = ANY (ARRAY['dispatching'::"text", 'dispatched'::"text", 'running'::"text", 'derivatives_pending'::"text", 'completed'::"text", 'failed'::"text", 'indeterminate'::"text"]))),
    CONSTRAINT "dataset_alias_execution_request_terminal_check" CHECK (((("status" = ANY (ARRAY['completed'::"text", 'failed'::"text", 'indeterminate'::"text"])) AND ("terminal_at" IS NOT NULL)) OR (("status" <> ALL (ARRAY['completed'::"text", 'failed'::"text", 'indeterminate'::"text"])) AND ("terminal_at" IS NULL))))
);

ALTER TABLE "util"."dataset_alias_execution_requests" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_alias_execution_requests" IS 'Private one-attempt ledger. A sealed approval identity can create at most one row and at most one pg_net dispatch; status/readback never redispatches.';

ALTER TABLE ONLY "util"."dataset_alias_execution_requests"
    ADD CONSTRAINT "dataset_alias_execution_requests_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "util"."dataset_alias_execution_requests"
    ADD CONSTRAINT "dataset_alias_execution_requests_id_fkey" FOREIGN KEY ("id") REFERENCES "util"."dataset_alias_execution_preflights"("id");
