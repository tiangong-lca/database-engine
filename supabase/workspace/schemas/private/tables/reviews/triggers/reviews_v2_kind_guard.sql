CREATE OR REPLACE TRIGGER "reviews_v2_kind_guard" BEFORE INSERT OR UPDATE ON "private"."reviews" FOR EACH ROW EXECUTE FUNCTION "private"."review_v2_kind_guard"();
