CREATE INDEX "comments_reviewer_queue_state_review_idx" ON "private"."comments" USING "btree" ("reviewer_id", "state_code", "review_id");
