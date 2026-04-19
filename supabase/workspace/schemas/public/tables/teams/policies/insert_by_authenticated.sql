CREATE POLICY "insert by authenticated" ON "public"."teams" FOR INSERT TO "authenticated" WITH CHECK ((( SELECT "count"(1) AS "count"
   FROM "public"."roles"
  WHERE (("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" <> 'rejected'::"text") AND ("roles"."team_id" <> '00000000-0000-0000-0000-000000000000'::"uuid"))) = 0));
