CREATE OR REPLACE TRIGGER "review_dataset_content_guard_v1" BEFORE UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "private"."review_dataset_content_guard_v1"();
