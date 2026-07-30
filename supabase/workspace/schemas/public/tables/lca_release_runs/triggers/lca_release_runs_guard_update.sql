CREATE OR REPLACE TRIGGER "lca_release_runs_guard_update" BEFORE UPDATE ON "public"."lca_release_runs" FOR EACH ROW EXECUTE FUNCTION "public"."lca_release_guard_run_update"();
