CREATE OR REPLACE TRIGGER "lca_release_approvals_guard_update" BEFORE UPDATE ON "private"."lca_release_approvals" FOR EACH ROW EXECUTE FUNCTION "private"."lca_release_guard_approval_update"();
