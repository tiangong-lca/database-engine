CREATE POLICY "Enable insert for review-admin" ON "public"."comments" FOR INSERT TO "authenticated" WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" = 'review-admin'::"text")))));
