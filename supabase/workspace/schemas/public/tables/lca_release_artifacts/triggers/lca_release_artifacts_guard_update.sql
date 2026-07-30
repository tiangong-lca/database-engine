CREATE OR REPLACE TRIGGER "lca_release_artifacts_guard_update" BEFORE UPDATE ON "public"."lca_release_artifacts" FOR EACH ROW EXECUTE FUNCTION "public"."lca_release_guard_artifact_update"();
