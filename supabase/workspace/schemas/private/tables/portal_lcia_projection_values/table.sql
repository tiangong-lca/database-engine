CREATE TABLE IF NOT EXISTS "private"."portal_lcia_projection_values" (
    "projection_id" "uuid" NOT NULL,
    "ordinal" bigint NOT NULL,
    "process_index" integer NOT NULL,
    "impact_index" integer NOT NULL,
    "value_text" "text" NOT NULL,
    "value_numeric" numeric NOT NULL,
    "record_hash" "text" NOT NULL,
    CONSTRAINT "portal_lcia_projection_value_decimal_chk" CHECK ((("value_text" = "private"."portal_canonical_decimal_v1"("value_text")) AND ("value_numeric" = ("value_text")::numeric))),
    CONSTRAINT "portal_lcia_projection_value_hash_chk" CHECK (("record_hash" ~ '^[0-9a-f]{64}$'::"text")),
    CONSTRAINT "portal_lcia_projection_value_indexes_chk" CHECK ((("ordinal" > 0) AND ("process_index" >= 0) AND ("impact_index" >= 0)))
);

ALTER TABLE "private"."portal_lcia_projection_values" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_lcia_projection_values" IS 'Dense canonical-decimal Process-by-impact value grid. Explicit zero rows are retained.';

ALTER TABLE ONLY "private"."portal_lcia_projection_values"
    ADD CONSTRAINT "portal_lcia_projection_value_cell_uidx" UNIQUE ("projection_id", "process_index", "impact_index");

ALTER TABLE ONLY "private"."portal_lcia_projection_values"
    ADD CONSTRAINT "portal_lcia_projection_values_pkey" PRIMARY KEY ("projection_id", "ordinal");

ALTER TABLE ONLY "private"."portal_lcia_projection_values"
    ADD CONSTRAINT "portal_lcia_projection_value_impact_fk" FOREIGN KEY ("projection_id", "impact_index") REFERENCES "private"."portal_lcia_projection_impact_axis"("projection_id", "impact_index") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."portal_lcia_projection_values"
    ADD CONSTRAINT "portal_lcia_projection_value_process_fk" FOREIGN KEY ("projection_id", "process_index") REFERENCES "private"."portal_lcia_projection_process_axis"("projection_id", "process_index") ON DELETE RESTRICT;

ALTER TABLE ONLY "private"."portal_lcia_projection_values"
    ADD CONSTRAINT "portal_lcia_projection_values_projection_id_fkey" FOREIGN KEY ("projection_id") REFERENCES "private"."portal_lcia_projection_headers"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."portal_lcia_projection_values" ENABLE ROW LEVEL SECURITY;

GRANT SELECT("projection_id") ON TABLE "private"."portal_lcia_projection_values" TO "portal_public_executor";

GRANT SELECT("ordinal") ON TABLE "private"."portal_lcia_projection_values" TO "portal_public_executor";

GRANT SELECT("process_index") ON TABLE "private"."portal_lcia_projection_values" TO "portal_public_executor";

GRANT SELECT("impact_index") ON TABLE "private"."portal_lcia_projection_values" TO "portal_public_executor";

GRANT SELECT("value_text") ON TABLE "private"."portal_lcia_projection_values" TO "portal_public_executor";

GRANT SELECT("value_numeric") ON TABLE "private"."portal_lcia_projection_values" TO "portal_public_executor";
