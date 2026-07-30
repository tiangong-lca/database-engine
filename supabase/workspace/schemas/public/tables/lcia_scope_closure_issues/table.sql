CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_issues" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "closure_check_id" "uuid" NOT NULL,
    "issue_key" "text" NOT NULL,
    "severity" "text" NOT NULL,
    "blocking" boolean DEFAULT false NOT NULL,
    "issue_code" "text" NOT NULL,
    "source_dataset_type" "text",
    "source_dataset_id" "uuid",
    "source_dataset_version" "text",
    "json_path" "text",
    "reference_role" "text",
    "requested_target_type" "text",
    "requested_target_id" "uuid",
    "requested_target_version" "text",
    "message" "text" NOT NULL,
    "suggested_action" "text",
    "occurrence_count" integer DEFAULT 1 NOT NULL,
    "affected_root_count" integer DEFAULT 0 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "details" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    CONSTRAINT "lcia_scope_closure_issues_counts_check" CHECK ((("occurrence_count" > 0) AND ("affected_root_count" >= 0))),
    CONSTRAINT "lcia_scope_closure_issues_severity_check" CHECK (("severity" = ANY (ARRAY['blocker'::"text", 'warning'::"text", 'info'::"text"])))
);

ALTER TABLE "public"."lcia_scope_closure_issues" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lcia_scope_closure_issues"
    ADD CONSTRAINT "lcia_scope_closure_issues_key_uidx" UNIQUE ("closure_check_id", "issue_key");

ALTER TABLE ONLY "public"."lcia_scope_closure_issues"
    ADD CONSTRAINT "lcia_scope_closure_issues_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lcia_scope_closure_issues"
    ADD CONSTRAINT "lcia_scope_closure_issues_closure_check_id_fkey" FOREIGN KEY ("closure_check_id") REFERENCES "public"."lcia_scope_closure_checks"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lcia_scope_closure_issues" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lcia_scope_closure_issues" TO "service_role";
