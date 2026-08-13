CREATE INDEX "roles_team_id_user_id_role_idx" ON "private"."roles" USING "btree" ("team_id", "user_id", "role");
