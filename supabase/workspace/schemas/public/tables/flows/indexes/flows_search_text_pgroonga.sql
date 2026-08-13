CREATE INDEX "flows_search_text_pgroonga" ON "public"."flows" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
