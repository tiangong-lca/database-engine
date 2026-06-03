CREATE INDEX "worker_jobs_parent_idx" ON "public"."worker_jobs" USING "btree" ("parent_job_id") WHERE ("parent_job_id" IS NOT NULL);
