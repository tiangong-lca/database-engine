CREATE OR REPLACE TRIGGER "lifecyclemodels_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "public"."lifecyclemodels_sync_jsonb_version"();
