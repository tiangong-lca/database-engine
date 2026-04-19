CREATE POLICY "Enable delete for users based on user_id" ON "public"."flowproperties" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));
