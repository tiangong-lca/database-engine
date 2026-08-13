CREATE INDEX "notifications_recipient_type_modified_idx" ON "private"."notifications" USING "btree" ("recipient_user_id", "type", "modified_at" DESC);
