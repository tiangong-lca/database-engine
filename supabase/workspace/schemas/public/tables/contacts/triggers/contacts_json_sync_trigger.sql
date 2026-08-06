CREATE OR REPLACE TRIGGER "contacts_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "private"."contacts_sync_jsonb_version"();
