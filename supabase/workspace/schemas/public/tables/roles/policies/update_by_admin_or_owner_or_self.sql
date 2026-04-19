CREATE POLICY "update by admin or owner or self" ON "public"."roles" FOR UPDATE TO "authenticated" USING ("public"."policy_roles_update"("user_id", "team_id", ("role")::"text"));
