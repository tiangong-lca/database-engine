CREATE OR REPLACE TRIGGER "unitgroups_set_modified_at_trigger" BEFORE UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();
