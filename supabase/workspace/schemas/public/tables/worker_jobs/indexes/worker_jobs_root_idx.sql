CREATE INDEX "worker_jobs_root_idx" ON "public"."worker_jobs" USING "btree" ("root_job_id") WHERE ("root_job_id" IS NOT NULL);
