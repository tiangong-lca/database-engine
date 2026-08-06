CREATE OR REPLACE TRIGGER "comments_v2_kind_guard" BEFORE INSERT OR UPDATE ON "private"."comments" FOR EACH ROW EXECUTE FUNCTION "private"."review_v2_comment_guard"();
