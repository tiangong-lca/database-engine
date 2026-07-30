CREATE OR REPLACE TRIGGER "sources_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "public"."sources_sync_jsonb_version"();
