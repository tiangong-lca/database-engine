CREATE POLICY "update by owner and admin" ON "public"."teams" FOR UPDATE TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND ("r"."team_id" = "teams"."id") AND (("r"."role")::"text" = ANY (ARRAY['owner'::"text", 'admin'::"text"])))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND ("r"."team_id" = "teams"."id") AND (("r"."role")::"text" = ANY (ARRAY['owner'::"text", 'admin'::"text"]))))));
