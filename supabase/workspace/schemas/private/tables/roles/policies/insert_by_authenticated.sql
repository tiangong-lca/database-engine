CREATE POLICY "insert by authenticated" ON "private"."roles" FOR INSERT TO "authenticated" WITH CHECK ("api"."policy_roles_insert"("user_id", "team_id", ("role")::"text"));
