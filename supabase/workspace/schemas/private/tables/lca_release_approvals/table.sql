CREATE TABLE IF NOT EXISTS "private"."lca_release_approvals" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "release_run_id" "uuid" NOT NULL,
    "publish_plan_hash" "text" NOT NULL,
    "approval_hash" "text" NOT NULL,
    "status" "text" DEFAULT 'approved'::"text" NOT NULL,
    "approved_by" "uuid" NOT NULL,
    "approved_at" timestamp with time zone NOT NULL,
    "expires_at" timestamp with time zone NOT NULL,
    "reason" "text",
    "consumed_by" "uuid",
    "consumed_at" timestamp with time zone,
    "revoked_at" timestamp with time zone,
    "audit_correlation" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    CONSTRAINT "lca_release_approvals_audit_chk" CHECK (("jsonb_typeof"("audit_correlation") = 'object'::"text")),
    CONSTRAINT "lca_release_approvals_expiry_chk" CHECK (("expires_at" > "approved_at")),
    CONSTRAINT "lca_release_approvals_hash_chk" CHECK ((("publish_plan_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("approval_hash" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "lca_release_approvals_status_chk" CHECK (("status" = ANY (ARRAY['approved'::"text", 'consumed'::"text", 'expired'::"text", 'revoked'::"text"])))
);

ALTER TABLE "private"."lca_release_approvals" OWNER TO "postgres";

COMMENT ON TABLE "private"."lca_release_approvals" IS 'Durable human approval receipts bound to an exact immutable publish-plan hash.';

ALTER TABLE ONLY "private"."lca_release_approvals"
    ADD CONSTRAINT "lca_release_approvals_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lca_release_approvals"
    ADD CONSTRAINT "lca_release_approvals_release_run_id_fkey" FOREIGN KEY ("release_run_id") REFERENCES "private"."lca_release_runs"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lca_release_approvals" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."lca_release_approvals" TO "api_internal_executor";
