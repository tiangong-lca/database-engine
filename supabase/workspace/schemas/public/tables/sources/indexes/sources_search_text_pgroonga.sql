CREATE INDEX "sources_search_text_pgroonga" ON "public"."sources" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
