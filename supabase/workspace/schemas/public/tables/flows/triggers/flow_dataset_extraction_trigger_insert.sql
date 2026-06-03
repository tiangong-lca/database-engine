CREATE OR REPLACE TRIGGER "flow_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();
