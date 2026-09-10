CREATE OR REPLACE TRIGGER "protect_example_dataset_write" BEFORE DELETE OR UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "private"."protect_example_dataset_write"();
