CREATE INDEX "flowproperties_json_pgroonga" ON "public"."flowproperties" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");
