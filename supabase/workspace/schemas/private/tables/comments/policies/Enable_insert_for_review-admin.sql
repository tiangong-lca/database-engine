CREATE POLICY "Enable insert for review-admin" ON "private"."comments" FOR INSERT TO "authenticated" WITH CHECK ((EXISTS ( SELECT 1
   FROM "private"."roles"
  WHERE (("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" = 'review-admin'::"text")))));
