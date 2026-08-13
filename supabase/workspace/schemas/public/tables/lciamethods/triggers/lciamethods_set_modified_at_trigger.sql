CREATE OR REPLACE TRIGGER "lciamethods_set_modified_at_trigger" BEFORE UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "private"."update_modified_at"();
