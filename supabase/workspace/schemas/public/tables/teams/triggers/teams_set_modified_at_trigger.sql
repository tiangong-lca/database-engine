CREATE OR REPLACE TRIGGER "teams_set_modified_at_trigger" BEFORE UPDATE ON "public"."teams" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
