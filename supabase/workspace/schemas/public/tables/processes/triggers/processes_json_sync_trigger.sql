CREATE OR REPLACE TRIGGER "processes_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "public"."processes_sync_jsonb_version"();
