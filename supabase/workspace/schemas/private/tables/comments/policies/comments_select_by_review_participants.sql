CREATE POLICY "comments select by review participants" ON "private"."comments" FOR SELECT TO "authenticated" USING (((( SELECT "auth"."uid"() AS "uid") IS NOT NULL) AND "api"."policy_review_can_read"("review_id", ( SELECT "auth"."uid"() AS "uid")) AND ("api"."cmd_review_is_review_admin"(( SELECT "auth"."uid"() AS "uid")) OR (EXISTS ( SELECT 1
   FROM "private"."reviews" "r"
  WHERE (("r"."id" = "comments"."review_id") AND (((("r"."json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = ( SELECT "auth"."uid"() AS "uid"))))) OR ("reviewer_id" = ( SELECT "auth"."uid"() AS "uid")))));
