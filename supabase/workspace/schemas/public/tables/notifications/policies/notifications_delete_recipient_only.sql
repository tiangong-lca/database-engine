CREATE POLICY "notifications_delete_recipient_only" ON "public"."notifications" FOR DELETE TO "authenticated" USING ((( SELECT "auth"."uid"() AS "uid") = "recipient_user_id"));
