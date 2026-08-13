CREATE INDEX "embedding_job_failures_failed_at_idx" ON "util"."embedding_job_failures" USING "btree" ("failed_at" DESC);
