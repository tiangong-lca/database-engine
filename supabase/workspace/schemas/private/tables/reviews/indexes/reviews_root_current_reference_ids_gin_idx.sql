CREATE INDEX "reviews_root_current_reference_ids_gin_idx" ON "private"."reviews" USING "gin" ("current_reference_review_ids") WHERE ("review_kind" = 'root'::"text");
