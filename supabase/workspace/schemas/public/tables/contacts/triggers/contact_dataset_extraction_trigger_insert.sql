CREATE OR REPLACE TRIGGER "contact_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();
