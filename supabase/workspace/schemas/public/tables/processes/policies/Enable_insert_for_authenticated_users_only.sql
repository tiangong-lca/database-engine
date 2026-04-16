CREATE POLICY "Enable insert for authenticated users only" ON "public"."processes" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));
