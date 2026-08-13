CREATE INDEX "dataset_alias_execution_gate_actor_read_idx" ON "util"."dataset_alias_execution_gate_receipts" USING "btree" ("actor_user_id", "preflight_id", "captured_at");
