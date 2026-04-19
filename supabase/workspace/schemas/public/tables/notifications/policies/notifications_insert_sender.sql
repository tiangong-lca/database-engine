CREATE POLICY "notifications_insert_sender" ON "public"."notifications" FOR INSERT TO "authenticated" WITH CHECK (("auth"."uid"() = "sender_user_id"));
