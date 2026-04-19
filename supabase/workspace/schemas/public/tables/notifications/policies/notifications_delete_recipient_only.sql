CREATE POLICY "notifications_delete_recipient_only" ON "public"."notifications" FOR DELETE TO "authenticated" USING (("auth"."uid"() = "recipient_user_id"));
