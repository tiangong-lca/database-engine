CREATE OR REPLACE TRIGGER "ilcd_set_modified_at_trigger" BEFORE UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
