CREATE INDEX "worker_jobs_requested_kind_updated_idx" ON "public"."worker_jobs" USING "btree" ("requested_by", "job_kind", "updated_at" DESC) WHERE ("requested_by" IS NOT NULL);
