CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_issue_roots" (
    "closure_issue_id" "uuid" NOT NULL,
    "root_dataset_type" "text" NOT NULL,
    "root_dataset_id" "uuid" NOT NULL,
    "root_dataset_version" "text" NOT NULL,
    "impact_role" "text" DEFAULT 'root'::"text" NOT NULL,
    "witness_path" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_issue_roots_witness_path_check" CHECK (("jsonb_typeof"("witness_path") = 'array'::"text"))
);

ALTER TABLE "public"."lcia_scope_closure_issue_roots" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lcia_scope_closure_issue_roots"
    ADD CONSTRAINT "lcia_scope_closure_issue_roots_pkey" PRIMARY KEY ("closure_issue_id", "root_dataset_type", "root_dataset_id", "root_dataset_version", "impact_role");

ALTER TABLE ONLY "public"."lcia_scope_closure_issue_roots"
    ADD CONSTRAINT "lcia_scope_closure_issue_roots_closure_issue_id_fkey" FOREIGN KEY ("closure_issue_id") REFERENCES "public"."lcia_scope_closure_issues"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lcia_scope_closure_issue_roots" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lcia_scope_closure_issue_roots" TO "service_role";
