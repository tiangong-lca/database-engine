CREATE INDEX "next_hybrid_public_candidate_classification_v2_idx" ON "private"."next_hybrid_public_candidates_v2" USING "gin" ("classification_codes");
