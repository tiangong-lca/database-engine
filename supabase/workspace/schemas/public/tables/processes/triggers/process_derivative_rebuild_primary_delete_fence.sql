CREATE OR REPLACE TRIGGER "process_derivative_rebuild_primary_delete_fence" BEFORE DELETE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"();
