CREATE INDEX "worker_job_artifacts_job_created_idx" ON "public"."worker_job_artifacts" USING "btree" ("job_id", "created_at" DESC);
