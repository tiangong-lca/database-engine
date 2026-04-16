CREATE POLICY "insert by authenticated" ON "public"."roles" FOR INSERT TO "authenticated" WITH CHECK ("public"."policy_roles_insert"("user_id", "team_id", ("role")::"text"));
