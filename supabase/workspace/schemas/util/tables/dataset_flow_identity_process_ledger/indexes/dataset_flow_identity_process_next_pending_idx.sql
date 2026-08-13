CREATE INDEX "dataset_flow_identity_process_next_pending_idx" ON "util"."dataset_flow_identity_process_ledger" USING "btree" ("scope_id", "status", "ordinal");
