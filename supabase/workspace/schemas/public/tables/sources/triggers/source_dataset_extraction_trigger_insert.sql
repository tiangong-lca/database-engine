CREATE OR REPLACE TRIGGER "source_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();
