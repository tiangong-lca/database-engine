CREATE OR REPLACE TRIGGER "flowproperties_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "public"."flowproperties_sync_jsonb_version"();
