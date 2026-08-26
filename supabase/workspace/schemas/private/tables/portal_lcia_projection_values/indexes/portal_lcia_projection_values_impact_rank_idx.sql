CREATE INDEX "portal_lcia_projection_values_impact_rank_idx" ON "private"."portal_lcia_projection_values" USING "btree" ("projection_id", "impact_index", "value_numeric" DESC, "ordinal");
