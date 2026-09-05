CREATE INDEX "next_hybrid_public_candidate_elementary_v2_idx" ON "private"."next_hybrid_public_candidates_v2" USING "gin" ("elementary_codes");
