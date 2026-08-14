CREATE OR REPLACE TRIGGER "lca_release_artifacts_guard_update" BEFORE UPDATE ON "private"."lca_release_artifacts" FOR EACH ROW EXECUTE FUNCTION "private"."lca_release_guard_artifact_update"();
