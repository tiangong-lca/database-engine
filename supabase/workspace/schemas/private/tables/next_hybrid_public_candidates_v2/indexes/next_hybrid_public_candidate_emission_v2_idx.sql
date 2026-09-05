CREATE INDEX "next_hybrid_public_candidate_emission_v2_idx" ON "private"."next_hybrid_public_candidates_v2" USING "btree" ("is_emission", "dataset_kind", "state_code", "team_id", "id", "version");
