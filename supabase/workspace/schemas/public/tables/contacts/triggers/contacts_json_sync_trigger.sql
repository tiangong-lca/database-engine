CREATE OR REPLACE TRIGGER "contacts_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."contacts_sync_jsonb_version"();
