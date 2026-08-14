CREATE POLICY "delete by owner and admin" ON "private"."roles" FOR DELETE TO "authenticated" USING ("api"."policy_roles_delete"("user_id", "team_id", ("role")::"text"));
