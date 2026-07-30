CREATE INDEX "lca_network_snapshots_lcia_idx" ON "public"."lca_network_snapshots" USING "btree" ("lcia_method_id", "lcia_method_version") WHERE ("lcia_method_id" IS NOT NULL);
