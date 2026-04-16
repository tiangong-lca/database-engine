CREATE OR REPLACE TRIGGER "processes_set_modified_at_trigger" BEFORE UPDATE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
