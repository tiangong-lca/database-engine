-- Independent concurrent index build; exact definition/readiness checked at cutover.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "portal_catalog_search_process_document_v2_pgroonga" ON "private"."portal_catalog_search_rows_v2" USING "pgroonga" ("document") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto') WHERE ("dataset_kind" = 'process'::"text");
