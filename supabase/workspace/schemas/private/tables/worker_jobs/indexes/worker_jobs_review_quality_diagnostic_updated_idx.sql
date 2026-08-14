CREATE INDEX "worker_jobs_review_quality_diagnostic_updated_idx" ON "private"."worker_jobs" USING "btree" ("updated_at" DESC, "id" DESC) WHERE ("job_kind" = 'review.quality_diagnostic'::"text");
