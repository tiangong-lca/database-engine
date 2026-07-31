CREATE OR REPLACE TRIGGER "dataset_flow_identity_unitgroup_delete_active_fence" BEFORE DELETE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."dataset_flow_identity_active_fence"();
