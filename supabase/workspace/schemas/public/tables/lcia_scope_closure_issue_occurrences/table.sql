CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_issue_occurrences" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "closure_issue_id" "uuid" NOT NULL,
    "occurrence_key" "text" NOT NULL,
    "source_dataset_type" "text",
    "source_dataset_id" "uuid",
    "source_dataset_version" "text",
    "json_path" "text",
    "reference_role" "text",
    "details" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_issue_occurrences_details_check" CHECK (("jsonb_typeof"("details") = 'object'::"text"))
);

ALTER TABLE "public"."lcia_scope_closure_issue_occurrences" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lcia_scope_closure_issue_occurrences"
    ADD CONSTRAINT "lcia_scope_closure_issue_occu_closure_issue_id_occurrence_k_key" UNIQUE ("closure_issue_id", "occurrence_key");

ALTER TABLE ONLY "public"."lcia_scope_closure_issue_occurrences"
    ADD CONSTRAINT "lcia_scope_closure_issue_occurrences_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lcia_scope_closure_issue_occurrences"
    ADD CONSTRAINT "lcia_scope_closure_issue_occurrences_closure_issue_id_fkey" FOREIGN KEY ("closure_issue_id") REFERENCES "public"."lcia_scope_closure_issues"("id") ON DELETE CASCADE;

ALTER TABLE "public"."lcia_scope_closure_issue_occurrences" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lcia_scope_closure_issue_occurrences" TO "service_role";
