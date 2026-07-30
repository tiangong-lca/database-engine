CREATE OR REPLACE TRIGGER "unitgroups_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "public"."unitgroups_sync_jsonb_version"();
