CREATE POLICY "delete by owner and admin" ON "public"."roles" FOR DELETE TO "authenticated" USING ("public"."policy_roles_delete"("user_id", "team_id", ("role")::"text"));
