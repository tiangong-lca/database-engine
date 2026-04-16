CREATE OR REPLACE TRIGGER "sources_set_modified_at_trigger" BEFORE UPDATE ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
