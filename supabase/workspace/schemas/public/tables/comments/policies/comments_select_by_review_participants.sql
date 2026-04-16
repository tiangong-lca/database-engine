CREATE POLICY "comments select by review participants" ON "public"."comments" FOR SELECT TO "authenticated" USING ((("auth"."uid"() IS NOT NULL) AND "public"."policy_review_can_read"("review_id", "auth"."uid"()) AND ("public"."cmd_review_is_review_admin"("auth"."uid"()) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" = "comments"."review_id") AND (((("r"."json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = "auth"."uid"())))) OR ("reviewer_id" = "auth"."uid"()))));
