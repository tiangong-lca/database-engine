CREATE POLICY "notifications_update_sender" ON "public"."notifications" FOR UPDATE TO "authenticated" USING (("auth"."uid"() = "sender_user_id")) WITH CHECK (("auth"."uid"() = "sender_user_id"));
