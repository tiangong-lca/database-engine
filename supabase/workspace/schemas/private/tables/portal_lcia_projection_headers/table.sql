CREATE TABLE IF NOT EXISTS "private"."portal_lcia_projection_headers" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "build_worker_job_id" "uuid" NOT NULL,
    "stage_lease_token" "uuid" NOT NULL,
    "projection_contract_version" "text" NOT NULL,
    "status" "text" DEFAULT 'staging'::"text" NOT NULL,
    "process_count" integer NOT NULL,
    "impact_count" integer NOT NULL,
    "expected_value_count" bigint GENERATED ALWAYS AS ((("process_count")::bigint * ("impact_count")::bigint)) STORED,
    "input_manifest_hash" "text" NOT NULL,
    "closure_certificate_hash" "text" NOT NULL,
    "snapshot_hash" "text" NOT NULL,
    "closure_bundle_hash" "text" NOT NULL,
    "snapshot_index_sha256" "text" NOT NULL,
    "snapshot_build_contract_hash" "text" NOT NULL,
    "bundle_content_hash" "text" NOT NULL,
    "bundle_manifest_sha256" "text" NOT NULL,
    "lcia_chunk_set_sha256" "text" NOT NULL,
    "result_artifact_sha256" "text" NOT NULL,
    "query_artifact_sha256" "text" NOT NULL,
    "process_axis_hash" "text",
    "impact_axis_hash" "text",
    "value_grid_hash" "text",
    "relation_hash" "text",
    "content_hash" "text",
    "failure_code" "text",
    "failure_message" "text",
    "created_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    "prepared_at" timestamp with time zone,
    "failed_at" timestamp with time zone,
    CONSTRAINT "portal_lcia_projection_headers_contract_chk" CHECK (("projection_contract_version" = 'portal.lcia-projection.v1'::"text")),
    CONSTRAINT "portal_lcia_projection_headers_counts_chk" CHECK (((("process_count" >= 1) AND ("process_count" <= 1000000)) AND (("impact_count" >= 1) AND ("impact_count" <= 10000)) AND ((("process_count")::bigint * ("impact_count")::bigint) <= 100000000))),
    CONSTRAINT "portal_lcia_projection_headers_hashes_chk" CHECK ((("input_manifest_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("closure_certificate_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("snapshot_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("closure_bundle_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("snapshot_index_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("snapshot_build_contract_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("bundle_content_hash" ~ '^[0-9a-f]{64}$'::"text") AND ("bundle_manifest_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("lcia_chunk_set_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("result_artifact_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("query_artifact_sha256" ~ '^[0-9a-f]{64}$'::"text") AND (("process_axis_hash" IS NULL) OR ("process_axis_hash" ~ '^[0-9a-f]{64}$'::"text")) AND (("impact_axis_hash" IS NULL) OR ("impact_axis_hash" ~ '^[0-9a-f]{64}$'::"text")) AND (("value_grid_hash" IS NULL) OR ("value_grid_hash" ~ '^[0-9a-f]{64}$'::"text")) AND (("relation_hash" IS NULL) OR ("relation_hash" ~ '^[0-9a-f]{64}$'::"text")) AND (("content_hash" IS NULL) OR ("content_hash" ~ '^[0-9a-f]{64}$'::"text")))),
    CONSTRAINT "portal_lcia_projection_headers_status_chk" CHECK (("status" = ANY (ARRAY['staging'::"text", 'prepared'::"text", 'failed'::"text"]))),
    CONSTRAINT "portal_lcia_projection_headers_terminal_shape_chk" CHECK (((("status" = 'staging'::"text") AND ("process_axis_hash" IS NULL) AND ("impact_axis_hash" IS NULL) AND ("value_grid_hash" IS NULL) AND ("relation_hash" IS NULL) AND ("content_hash" IS NULL) AND ("prepared_at" IS NULL) AND ("failed_at" IS NULL) AND ("failure_code" IS NULL) AND ("failure_message" IS NULL)) OR (("status" = 'prepared'::"text") AND ("process_axis_hash" IS NOT NULL) AND ("impact_axis_hash" IS NOT NULL) AND ("value_grid_hash" IS NOT NULL) AND ("relation_hash" IS NOT NULL) AND ("content_hash" IS NOT NULL) AND ("prepared_at" IS NOT NULL) AND ("failed_at" IS NULL) AND ("failure_code" IS NULL) AND ("failure_message" IS NULL)) OR (("status" = 'failed'::"text") AND ("failed_at" IS NOT NULL) AND (NULLIF("btrim"("failure_code"), ''::"text") IS NOT NULL) AND ("prepared_at" IS NULL) AND ("process_axis_hash" IS NULL) AND ("impact_axis_hash" IS NULL) AND ("value_grid_hash" IS NULL) AND ("relation_hash" IS NULL) AND ("content_hash" IS NULL))))
);

ALTER TABLE "private"."portal_lcia_projection_headers" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_lcia_projection_headers" IS 'Lease-fenced, locator-free typed Portal LCIA projection preparation header. Each Worker attempt has its own immutable identity.';

ALTER TABLE ONLY "private"."portal_lcia_projection_headers"
    ADD CONSTRAINT "portal_lcia_projection_headers_job_lease_uidx" UNIQUE ("build_worker_job_id", "stage_lease_token");

ALTER TABLE ONLY "private"."portal_lcia_projection_headers"
    ADD CONSTRAINT "portal_lcia_projection_headers_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."portal_lcia_projection_headers"
    ADD CONSTRAINT "portal_lcia_projection_headers_build_worker_job_id_fkey" FOREIGN KEY ("build_worker_job_id") REFERENCES "private"."worker_jobs"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."portal_lcia_projection_headers" ENABLE ROW LEVEL SECURITY;

GRANT SELECT("id") ON TABLE "private"."portal_lcia_projection_headers" TO "portal_public_executor";

GRANT SELECT("status") ON TABLE "private"."portal_lcia_projection_headers" TO "portal_public_executor";

GRANT SELECT("process_count") ON TABLE "private"."portal_lcia_projection_headers" TO "portal_public_executor";

GRANT SELECT("impact_count") ON TABLE "private"."portal_lcia_projection_headers" TO "portal_public_executor";

GRANT SELECT("expected_value_count") ON TABLE "private"."portal_lcia_projection_headers" TO "portal_public_executor";

GRANT SELECT("content_hash") ON TABLE "private"."portal_lcia_projection_headers" TO "portal_public_executor";
