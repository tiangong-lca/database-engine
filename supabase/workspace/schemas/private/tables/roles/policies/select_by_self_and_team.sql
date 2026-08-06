CREATE POLICY "select by self and team" ON "private"."roles" FOR SELECT TO "authenticated" USING ("api"."policy_roles_select"("team_id", ("role")::"text"));
