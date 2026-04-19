CREATE OR REPLACE TRIGGER "flowproperties_set_modified_at_trigger" BEFORE UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
