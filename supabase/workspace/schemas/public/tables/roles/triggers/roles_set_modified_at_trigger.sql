CREATE OR REPLACE TRIGGER "roles_set_modified_at_trigger" BEFORE UPDATE ON "public"."roles" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
