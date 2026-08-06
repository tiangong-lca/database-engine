CREATE INDEX "lca_factorization_registry_status_lease_idx" ON "private"."lca_factorization_registry" USING "btree" ("status", "lease_until");
