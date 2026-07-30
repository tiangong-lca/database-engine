CREATE OR REPLACE TRIGGER "flow_derivative_rebuild_primary_delete_fence" BEFORE DELETE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"();
