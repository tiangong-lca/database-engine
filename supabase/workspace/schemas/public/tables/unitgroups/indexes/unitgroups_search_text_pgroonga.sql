CREATE INDEX "unitgroups_search_text_pgroonga" ON "public"."unitgroups" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
