CREATE TABLE IF NOT EXISTS "util"."dataset_flow_identity_wrapper_invocations" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope_id" "uuid" NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "approval_kind" "text" NOT NULL,
    "approval_request_sha256" "text" NOT NULL,
    "approval_text_sha256" "text" NOT NULL,
    "approval_identity_sha256" "text" NOT NULL,
    "admission_request_sha256" "text" NOT NULL,
    "baseline_whole_scope_proof_sha256" "text" NOT NULL,
    "generation" integer DEFAULT 0 NOT NULL,
    "token_sha256" "text" NOT NULL,
    "maximum_process_posts" integer NOT NULL,
    "successful_process_posts" integer DEFAULT 0 NOT NULL,
    "maximum_finalize_posts" integer DEFAULT 1 NOT NULL,
    "successful_finalize_posts" integer DEFAULT 0 NOT NULL,
    "status" "text" DEFAULT 'active'::"text" NOT NULL,
    "admitted_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    "closed_at" timestamp with time zone,
    CONSTRAINT "dataset_flow_identity_invocation_approval_domains_chk" CHECK ((("approval_request_sha256" <> "approval_text_sha256") AND ("approval_request_sha256" <> "approval_identity_sha256") AND ("approval_text_sha256" <> "approval_identity_sha256"))),
    CONSTRAINT "dataset_flow_identity_invocation_counts_chk" CHECK ((("generation" >= 0) AND ("maximum_process_posts" >= 0) AND ("successful_process_posts" >= 0) AND ("successful_process_posts" <= "maximum_process_posts") AND ("maximum_finalize_posts" = 1) AND ("successful_finalize_posts" >= 0) AND ("successful_finalize_posts" <= "maximum_finalize_posts"))),
    CONSTRAINT "dataset_flow_identity_invocation_hashes_chk" CHECK ((("approval_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_text_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("approval_identity_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("admission_request_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("baseline_whole_scope_proof_sha256" ~ '^[a-f0-9]{64}$'::"text") AND ("token_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "dataset_flow_identity_invocation_kind_chk" CHECK (("approval_kind" = ANY (ARRAY['initial'::"text", 'recovery'::"text"]))),
    CONSTRAINT "dataset_flow_identity_invocation_status_chk" CHECK (("status" = ANY (ARRAY['active'::"text", 'superseded'::"text", 'completed'::"text", 'cancelled'::"text"])))
);

ALTER TABLE "util"."dataset_flow_identity_wrapper_invocations" OWNER TO "postgres";

COMMENT ON TABLE "util"."dataset_flow_identity_wrapper_invocations" IS 'Private one-wrapper approval-consumption ledger. Fresh initial/recovery admission returns one memory-only bearer token; replay returns no token, successful writes rotate it, and superseded/terminal tokens can never write.';

ALTER TABLE ONLY "util"."dataset_flow_identity_wrapper_invocations"
    ADD CONSTRAINT "dataset_flow_identity_wrapper_actor_user_id_approval_identi_key" UNIQUE ("actor_user_id", "approval_identity_sha256");

ALTER TABLE ONLY "util"."dataset_flow_identity_wrapper_invocations"
    ADD CONSTRAINT "dataset_flow_identity_wrapper_actor_user_id_approval_reques_key" UNIQUE ("actor_user_id", "approval_request_sha256");

ALTER TABLE ONLY "util"."dataset_flow_identity_wrapper_invocations"
    ADD CONSTRAINT "dataset_flow_identity_wrapper_actor_user_id_approval_text_s_key" UNIQUE ("actor_user_id", "approval_text_sha256");

ALTER TABLE ONLY "util"."dataset_flow_identity_wrapper_invocations"
    ADD CONSTRAINT "dataset_flow_identity_wrapper_invocations_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "util"."dataset_flow_identity_wrapper_invocations"
    ADD CONSTRAINT "dataset_flow_identity_wrapper_invocations_scope_id_fkey" FOREIGN KEY ("scope_id") REFERENCES "util"."dataset_flow_identity_scopes"("id") ON DELETE RESTRICT;
