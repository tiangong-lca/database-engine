CREATE POLICY "lca_snapshot_artifacts_service_role_all" ON "public"."lca_snapshot_artifacts" TO "service_role" USING (true) WITH CHECK (true);
