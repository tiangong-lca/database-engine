CREATE OR REPLACE TRIGGER "flows_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "public"."flows_sync_jsonb_version"();
