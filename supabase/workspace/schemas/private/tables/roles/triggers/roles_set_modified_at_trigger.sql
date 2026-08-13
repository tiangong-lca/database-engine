CREATE OR REPLACE TRIGGER "roles_set_modified_at_trigger" BEFORE UPDATE ON "private"."roles" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();
