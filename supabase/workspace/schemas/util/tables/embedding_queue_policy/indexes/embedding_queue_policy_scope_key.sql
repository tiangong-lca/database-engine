CREATE UNIQUE INDEX "embedding_queue_policy_scope_key" ON "util"."embedding_queue_policy" USING "btree" ("scope_schema", "scope_table", "scope_edge_function", "scope_embedding_column");
