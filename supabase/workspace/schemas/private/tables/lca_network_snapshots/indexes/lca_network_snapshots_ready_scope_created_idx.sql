CREATE INDEX "lca_network_snapshots_ready_scope_created_idx" ON "private"."lca_network_snapshots" USING "btree" ("scope", "created_at" DESC, "id") WHERE ("status" = 'ready'::"text");
