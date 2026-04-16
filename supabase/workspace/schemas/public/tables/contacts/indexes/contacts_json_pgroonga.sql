CREATE INDEX "contacts_json_pgroonga" ON "public"."contacts" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");
