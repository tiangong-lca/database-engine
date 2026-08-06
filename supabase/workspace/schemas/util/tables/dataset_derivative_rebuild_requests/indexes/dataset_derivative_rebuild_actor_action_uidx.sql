CREATE UNIQUE INDEX "dataset_derivative_rebuild_actor_action_uidx" ON "util"."dataset_derivative_rebuild_requests" USING "btree" ("actor_user_id", "plan_request_sha256", "action_request_sha256");
