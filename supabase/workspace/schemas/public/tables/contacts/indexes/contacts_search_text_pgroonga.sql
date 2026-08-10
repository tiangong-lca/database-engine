CREATE INDEX "contacts_search_text_pgroonga" ON "public"."contacts" USING "pgroonga" ("search_text") WITH ("tokenizer"='TokenBigram', "normalizer"='NormalizerAuto');
