-- Independent concurrent index build; exact definition/readiness checked at cutover.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "portal_catalog_search_process_exact_rank_v2_gin" ON "private"."portal_catalog_search_rows_v2" USING "gin" ("private"."portal_process_rank_name_keys_v1"("card"), "private"."portal_process_rank_classification_keys_v1"("card")) WHERE ("dataset_kind" = 'process'::"text");
