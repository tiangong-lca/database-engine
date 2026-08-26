CREATE TABLE IF NOT EXISTS "private"."portal_lcia_projection_process_axis" (
    "projection_id" "uuid" NOT NULL,
    "process_index" integer NOT NULL,
    "process_id" "uuid" NOT NULL,
    "process_version" "text" NOT NULL,
    "reference_flow_id" "uuid" NOT NULL,
    "reference_flow_version" "text" NOT NULL,
    "reference_exchange_internal_id" "text" NOT NULL,
    "reference_flow_amount" "text" NOT NULL,
    "reference_flow_direction" "text" NOT NULL,
    "functional_unit_amount" "text" NOT NULL,
    "functional_unit_unit" "text" NOT NULL,
    "functional_unit_description" "jsonb" NOT NULL,
    "geography_code" "text" NOT NULL,
    "geography_precision" "text" NOT NULL,
    "reference_year" integer NOT NULL,
    "process_document_sha256" "text" NOT NULL,
    "record_hash" "text" NOT NULL,
    CONSTRAINT "portal_lcia_projection_process_decimal_chk" CHECK (("functional_unit_amount" = "private"."portal_canonical_decimal_v1"("functional_unit_amount"))),
    CONSTRAINT "portal_lcia_projection_process_description_chk" CHECK ("private"."portal_lcia_localized_text_valid_v1"("functional_unit_description")),
    CONSTRAINT "portal_lcia_projection_process_geography_chk" CHECK (("private"."portal_lcia_public_text_valid_v1"("geography_code", 128) AND ("geography_precision" = ANY (ARRAY['country'::"text", 'province'::"text", 'city'::"text", 'other'::"text", 'unknown'::"text"])))),
    CONSTRAINT "portal_lcia_projection_process_hash_chk" CHECK ((("process_document_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("record_hash" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "portal_lcia_projection_process_index_chk" CHECK (("process_index" >= 0)),
    CONSTRAINT "portal_lcia_projection_process_reference_chk" CHECK ((("reference_exchange_internal_id" ~ '^(0|[1-9]\d{0,5})$'::"text") AND ("reference_flow_amount" = "private"."portal_canonical_decimal_v1"("reference_flow_amount")) AND ("reference_flow_direction" = ANY (ARRAY['input'::"text", 'output'::"text"])))),
    CONSTRAINT "portal_lcia_projection_process_unit_chk" CHECK ("private"."portal_lcia_public_text_valid_v1"("functional_unit_unit", 128)),
    CONSTRAINT "portal_lcia_projection_process_version_chk" CHECK ((("process_version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text") AND ("reference_flow_version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text"))),
    CONSTRAINT "portal_lcia_projection_process_year_chk" CHECK ((("reference_year" >= 0) AND ("reference_year" <= 9999)))
);

ALTER TABLE "private"."portal_lcia_projection_process_axis" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_lcia_projection_process_axis" IS 'Exact Process identity and complete public numeric context for one prepared Portal LCIA projection.';

ALTER TABLE ONLY "private"."portal_lcia_projection_process_axis"
    ADD CONSTRAINT "portal_lcia_projection_process_axis_pkey" PRIMARY KEY ("projection_id", "process_index");

ALTER TABLE ONLY "private"."portal_lcia_projection_process_axis"
    ADD CONSTRAINT "portal_lcia_projection_process_identity_uidx" UNIQUE ("projection_id", "process_id", "process_version");

ALTER TABLE ONLY "private"."portal_lcia_projection_process_axis"
    ADD CONSTRAINT "portal_lcia_projection_process_axis_projection_id_fkey" FOREIGN KEY ("projection_id") REFERENCES "private"."portal_lcia_projection_headers"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."portal_lcia_projection_process_axis" ENABLE ROW LEVEL SECURITY;

GRANT SELECT("projection_id") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("process_index") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("process_id") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("process_version") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("functional_unit_amount") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("functional_unit_unit") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("functional_unit_description") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("geography_code") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("geography_precision") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";

GRANT SELECT("reference_year") ON TABLE "private"."portal_lcia_projection_process_axis" TO "portal_public_executor";
