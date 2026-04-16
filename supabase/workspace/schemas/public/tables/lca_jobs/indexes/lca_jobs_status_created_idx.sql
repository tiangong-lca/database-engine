CREATE INDEX "lca_jobs_status_created_idx" ON "public"."lca_jobs" USING "btree" ("status", "created_at");
