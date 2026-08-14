CREATE INDEX "dataset_flow_identity_process_ledger_audit_idx" ON "util"."dataset_flow_identity_process_ledger" USING "btree" ("audit_id") WHERE ("audit_id" IS NOT NULL);
