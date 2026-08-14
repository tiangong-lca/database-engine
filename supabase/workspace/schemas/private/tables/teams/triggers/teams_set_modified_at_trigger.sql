CREATE OR REPLACE TRIGGER "teams_set_modified_at_trigger" BEFORE UPDATE ON "private"."teams" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();
