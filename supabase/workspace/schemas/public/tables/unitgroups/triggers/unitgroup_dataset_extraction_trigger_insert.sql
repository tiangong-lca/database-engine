CREATE OR REPLACE TRIGGER "unitgroup_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();
