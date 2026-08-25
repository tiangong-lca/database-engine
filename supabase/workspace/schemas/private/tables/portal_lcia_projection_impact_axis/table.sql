CREATE TABLE IF NOT EXISTS "private"."portal_lcia_projection_impact_axis" (
    "projection_id" "uuid" NOT NULL,
    "impact_index" integer NOT NULL,
    "method_id" "uuid" NOT NULL,
    "method_version" "text" NOT NULL,
    "impact_id" "text" NOT NULL,
    "impact_name" "jsonb" NOT NULL,
    "unit" "text" NOT NULL,
    "method_document_sha256" "text" NOT NULL,
    "record_hash" "text" NOT NULL,
    CONSTRAINT "portal_lcia_projection_impact_hash_chk" CHECK ((("method_document_sha256" ~ '^[0-9a-f]{64}$'::"text") AND ("record_hash" ~ '^[0-9a-f]{64}$'::"text"))),
    CONSTRAINT "portal_lcia_projection_impact_id_chk" CHECK ("private"."portal_lcia_public_text_valid_v1"("impact_id", 512)),
    CONSTRAINT "portal_lcia_projection_impact_index_chk" CHECK (("impact_index" >= 0)),
    CONSTRAINT "portal_lcia_projection_impact_name_chk" CHECK ("private"."portal_lcia_localized_text_valid_v1"("impact_name")),
    CONSTRAINT "portal_lcia_projection_impact_unit_chk" CHECK ("private"."portal_lcia_public_text_valid_v1"("unit", 128)),
    CONSTRAINT "portal_lcia_projection_method_version_chk" CHECK (("method_version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text"))
);

ALTER TABLE "private"."portal_lcia_projection_impact_axis" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_lcia_projection_impact_axis" IS 'Exact LCIA method/impact identity and unit context for one prepared Portal LCIA projection.';

ALTER TABLE ONLY "private"."portal_lcia_projection_impact_axis"
    ADD CONSTRAINT "portal_lcia_projection_impact_axis_pkey" PRIMARY KEY ("projection_id", "impact_index");

ALTER TABLE ONLY "private"."portal_lcia_projection_impact_axis"
    ADD CONSTRAINT "portal_lcia_projection_impact_identity_uidx" UNIQUE ("projection_id", "method_id", "method_version", "impact_id");

ALTER TABLE ONLY "private"."portal_lcia_projection_impact_axis"
    ADD CONSTRAINT "portal_lcia_projection_impact_axis_projection_id_fkey" FOREIGN KEY ("projection_id") REFERENCES "private"."portal_lcia_projection_headers"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."portal_lcia_projection_impact_axis" ENABLE ROW LEVEL SECURITY;

GRANT SELECT("projection_id") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";

GRANT SELECT("impact_index") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";

GRANT SELECT("method_id") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";

GRANT SELECT("method_version") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";

GRANT SELECT("impact_id") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";

GRANT SELECT("impact_name") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";

GRANT SELECT("unit") ON TABLE "private"."portal_lcia_projection_impact_axis" TO "portal_public_executor";
