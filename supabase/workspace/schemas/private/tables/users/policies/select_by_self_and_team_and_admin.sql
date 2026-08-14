CREATE POLICY "select by self and team and admin" ON "private"."users" FOR SELECT TO "authenticated" USING ((("id" = ( SELECT "auth"."uid"() AS "uid")) OR ("id" IN ( SELECT "r"."user_id"
   FROM "private"."roles" "r"
  WHERE ((("r"."role")::"text" = 'owner'::"text") AND ("api"."policy_is_team_public"("r"."team_id") = true)))) OR ("id" IN ( SELECT "r0"."user_id"
   FROM "private"."roles" "r0"
  WHERE ("r0"."team_id" IN ( SELECT "r"."team_id"
           FROM "private"."roles" "r"
          WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("r"."role")::"text" <> 'rejected'::"text")))))) OR "api"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['admin'::"text", 'review-admin'::"text", 'review-member'::"text"])));
