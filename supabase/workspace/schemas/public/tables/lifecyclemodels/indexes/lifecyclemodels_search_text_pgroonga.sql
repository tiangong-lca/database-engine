CREATE INDEX "lifecyclemodels_search_text_pgroonga" ON "public"."lifecyclemodels" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
