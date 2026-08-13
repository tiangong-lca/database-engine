CREATE INDEX "flowproperties_search_text_pgroonga" ON "public"."flowproperties" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
