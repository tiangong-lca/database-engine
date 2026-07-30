CREATE OR REPLACE TRIGGER "flowproperty_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();
