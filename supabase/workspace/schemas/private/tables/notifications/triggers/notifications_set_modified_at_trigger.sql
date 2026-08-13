CREATE OR REPLACE TRIGGER "notifications_set_modified_at_trigger" BEFORE UPDATE ON "private"."notifications" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();
