CREATE POLICY "select by self and team" ON "public"."roles" FOR SELECT TO "authenticated" USING ("public"."policy_roles_select"("team_id", ("role")::"text"));
