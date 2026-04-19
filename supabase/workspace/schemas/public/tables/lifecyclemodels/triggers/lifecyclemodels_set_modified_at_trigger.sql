CREATE OR REPLACE TRIGGER "lifecyclemodels_set_modified_at_trigger" BEFORE UPDATE ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
