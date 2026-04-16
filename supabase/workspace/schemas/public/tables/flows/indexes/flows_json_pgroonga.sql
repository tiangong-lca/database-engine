CREATE INDEX "flows_json_pgroonga" ON "public"."flows" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");
