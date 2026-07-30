CREATE TABLE IF NOT EXISTS "public"."lcia_result_packages" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "build_id" "uuid" NOT NULL,
    "build_worker_job_id" "uuid" NOT NULL,
    "package_version" "text" NOT NULL,
    "coverage_mode" "text" NOT NULL,
    "input_status_filter" "jsonb" DEFAULT '{"state_code": {"between": [100, 199]}}'::"jsonb" NOT NULL,
    "eligibility_definition" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "eligibility_resolved_at" timestamp with time zone NOT NULL,
    "eligible_input_count" integer NOT NULL,
    "included_input_count" integer NOT NULL,
    "input_manifest_hash" "text" NOT NULL,
    "input_manifest" "jsonb" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "result_id" "uuid" NOT NULL,
    "latest_all_unit_result_id" "uuid",
    "result_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "query_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "artifact_manifest" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "package_result_hash" "text",
    "lcia_method_set" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "available_impact_categories" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "postprocess_manifest" "jsonb" DEFAULT '{"postprocess_mode": "skipped"}'::"jsonb" NOT NULL,
    "default_impact_category" "text",
    "status" "text" DEFAULT 'preview_ready'::"text" NOT NULL,
    "created_by" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "closure_check_id" "uuid",
    "closure_certificate_hash" "text",
    "closure_snapshot_hash" "text",
    CONSTRAINT "lcia_result_packages_artifact_refs_object_chk" CHECK ((("jsonb_typeof"("result_artifact_ref") = 'object'::"text") AND ("jsonb_typeof"("query_artifact_ref") = 'object'::"text") AND ("jsonb_typeof"("artifact_manifest") = 'object'::"text") AND ("jsonb_typeof"("postprocess_manifest") = 'object'::"text"))),
    CONSTRAINT "lcia_result_packages_available_impacts_chk" CHECK (("jsonb_typeof"("available_impact_categories") = 'array'::"text")),
    CONSTRAINT "lcia_result_packages_counts_chk" CHECK ((("eligible_input_count" >= 0) AND ("included_input_count" >= 0))),
    CONSTRAINT "lcia_result_packages_coverage_chk" CHECK (("coverage_mode" = ANY (ARRAY['subset'::"text", 'global_eligible'::"text"]))),
    CONSTRAINT "lcia_result_packages_input_manifest_object_chk" CHECK (("jsonb_typeof"("input_manifest") = 'object'::"text")),
    CONSTRAINT "lcia_result_packages_lcia_method_set_chk" CHECK (("jsonb_typeof"("lcia_method_set") = 'array'::"text")),
    CONSTRAINT "lcia_result_packages_status_chk" CHECK (("status" = ANY (ARRAY['preview_ready'::"text", 'deprecated'::"text", 'failed'::"text"])))
);

ALTER TABLE "public"."lcia_result_packages" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lcia_result_packages"
    ADD CONSTRAINT "lcia_result_packages_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lcia_result_packages"
    ADD CONSTRAINT "lcia_result_packages_build_worker_job_id_fkey" FOREIGN KEY ("build_worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "public"."lcia_result_packages"
    ADD CONSTRAINT "lcia_result_packages_closure_check_id_fkey" FOREIGN KEY ("closure_check_id") REFERENCES "public"."lcia_scope_closure_checks"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "public"."lcia_result_packages"
    ADD CONSTRAINT "lcia_result_packages_latest_all_unit_result_id_fkey" FOREIGN KEY ("latest_all_unit_result_id") REFERENCES "public"."lca_latest_all_unit_results"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."lcia_result_packages"
    ADD CONSTRAINT "lcia_result_packages_result_id_fkey" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE RESTRICT;

ALTER TABLE ONLY "public"."lcia_result_packages"
    ADD CONSTRAINT "lcia_result_packages_snapshot_id_fkey" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE RESTRICT;

ALTER TABLE "public"."lcia_result_packages" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lcia_result_packages" TO "service_role";
