CREATE OR REPLACE TRIGGER "reviews_scope_history_guard_v1" BEFORE DELETE OR UPDATE ON "private"."reviews" FOR EACH ROW EXECUTE FUNCTION "private"."review_scope_history_guard_v1"();
