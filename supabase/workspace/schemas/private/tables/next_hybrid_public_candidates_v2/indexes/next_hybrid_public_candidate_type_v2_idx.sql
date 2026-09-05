CREATE INDEX "next_hybrid_public_candidate_type_v2_idx" ON "private"."next_hybrid_public_candidates_v2" USING "btree" ("dataset_type", "dataset_kind", "state_code", "team_id", "id", "version");
