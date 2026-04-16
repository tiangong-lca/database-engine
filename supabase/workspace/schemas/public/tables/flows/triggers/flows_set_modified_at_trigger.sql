CREATE OR REPLACE TRIGGER "flows_set_modified_at_trigger" BEFORE UPDATE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
