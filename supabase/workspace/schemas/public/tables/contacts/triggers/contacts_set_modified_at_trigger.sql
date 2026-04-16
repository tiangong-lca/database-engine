CREATE OR REPLACE TRIGGER "contacts_set_modified_at_trigger" BEFORE UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
