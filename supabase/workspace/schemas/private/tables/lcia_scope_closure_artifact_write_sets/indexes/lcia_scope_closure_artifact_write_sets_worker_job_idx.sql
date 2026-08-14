CREATE INDEX "lcia_scope_closure_artifact_write_sets_worker_job_idx" ON "private"."lcia_scope_closure_artifact_write_sets" USING "btree" ("worker_job_id", "status", "created_at", "id");
