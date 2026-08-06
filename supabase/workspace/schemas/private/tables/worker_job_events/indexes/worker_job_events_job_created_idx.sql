CREATE INDEX "worker_job_events_job_created_idx" ON "private"."worker_job_events" USING "btree" ("job_id", "created_at" DESC);
