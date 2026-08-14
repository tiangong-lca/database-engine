CREATE INDEX "dataset_flow_identity_mutation_permits_scope_ordinal_idx" ON "util"."dataset_flow_identity_mutation_permits" USING "btree" ("scope_id", "ordinal");
