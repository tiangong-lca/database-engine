CREATE INDEX "sources_json_pgroonga" ON "public"."sources" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");
