CREATE OR REPLACE TRIGGER "lca_release_runs_guard_update" BEFORE UPDATE ON "private"."lca_release_runs" FOR EACH ROW EXECUTE FUNCTION "private"."lca_release_guard_run_update"();
