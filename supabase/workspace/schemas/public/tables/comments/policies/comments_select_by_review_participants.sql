CREATE POLICY "comments select by review participants" ON "public"."comments" FOR SELECT TO "authenticated" USING (((( SELECT "auth"."uid"() AS "uid") IS NOT NULL) AND "public"."policy_review_can_read"("review_id", ( SELECT "auth"."uid"() AS "uid")) AND ("public"."cmd_review_is_review_admin"(( SELECT "auth"."uid"() AS "uid")) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" = "comments"."review_id") AND (((("r"."json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = ( SELECT "auth"."uid"() AS "uid"))))) OR ("reviewer_id" = ( SELECT "auth"."uid"() AS "uid")))));
