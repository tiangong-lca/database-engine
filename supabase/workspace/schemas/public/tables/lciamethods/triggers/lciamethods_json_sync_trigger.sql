CREATE OR REPLACE TRIGGER "lciamethods_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "public"."lciamethods_sync_jsonb_version"();
