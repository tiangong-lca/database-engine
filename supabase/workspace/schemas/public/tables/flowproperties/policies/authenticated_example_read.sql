CREATE POLICY "authenticated_example_read" ON "public"."flowproperties" FOR SELECT TO "authenticated" USING ((("state_code" = '-1'::integer) AND (( SELECT "auth"."uid"() AS "uid") IS NOT NULL)));
