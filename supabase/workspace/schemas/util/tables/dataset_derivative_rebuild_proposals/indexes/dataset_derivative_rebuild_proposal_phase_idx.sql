CREATE INDEX "dataset_derivative_rebuild_proposal_phase_idx" ON "util"."dataset_derivative_rebuild_proposals" USING "btree" ("request_id", "proposal_kind", "status", "captured_at", "id");
