CREATE INDEX "lifecyclemodels_json_tg_idx" ON "public"."lifecyclemodels" USING "gin" ("json_tg");
