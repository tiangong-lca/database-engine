CREATE POLICY "notifications_insert_sender" ON "private"."notifications" FOR INSERT TO "authenticated" WITH CHECK ((( SELECT "auth"."uid"() AS "uid") = "sender_user_id"));
