CREATE INDEX "processes_json_exchange_gin_idx" ON "public"."processes" USING "gin" ((((("json" -> 'processDataSet'::"text") -> 'exchanges'::"text") -> 'exchange'::"text")));
