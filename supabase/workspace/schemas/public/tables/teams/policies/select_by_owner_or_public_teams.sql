CREATE POLICY "select by owner or public teams" ON "public"."teams" FOR SELECT TO "authenticated" USING (("is_public" OR ("rank" > 0) OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE ((("roles"."team_id" = "teams"."id") OR ("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid")) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" <> 'rejected'::"text"))))));
