CREATE INDEX "processes_json_pgroonga" ON "public"."processes" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");
