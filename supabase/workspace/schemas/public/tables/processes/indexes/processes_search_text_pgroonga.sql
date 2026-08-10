CREATE INDEX "processes_search_text_pgroonga" ON "public"."processes" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
