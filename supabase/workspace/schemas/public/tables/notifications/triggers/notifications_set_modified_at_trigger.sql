CREATE OR REPLACE TRIGGER "notifications_set_modified_at_trigger" BEFORE UPDATE ON "public"."notifications" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
