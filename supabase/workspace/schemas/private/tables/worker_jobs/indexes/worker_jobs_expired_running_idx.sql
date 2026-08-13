CREATE INDEX "worker_jobs_expired_running_idx" ON "private"."worker_jobs" USING "btree" ("worker_runtime", "worker_queue", "lease_expires_at") WHERE ("status" = 'running'::"text");
