CREATE OR REPLACE TRIGGER "ilcd_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "public"."sync_json_to_jsonb"();
