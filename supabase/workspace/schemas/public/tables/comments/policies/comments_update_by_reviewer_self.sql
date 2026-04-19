CREATE POLICY "comments update by reviewer self" ON "public"."comments" FOR UPDATE TO "authenticated" USING (("reviewer_id" = "auth"."uid"())) WITH CHECK (("reviewer_id" = "auth"."uid"()));
