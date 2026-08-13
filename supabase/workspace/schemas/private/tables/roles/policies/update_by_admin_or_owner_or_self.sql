CREATE POLICY "update by admin or owner or self" ON "private"."roles" FOR UPDATE TO "authenticated" USING ("api"."policy_roles_update"("user_id", "team_id", ("role")::"text"));
