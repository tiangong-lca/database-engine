CREATE POLICY "lca_active_snapshots_service_role_all" ON "public"."lca_active_snapshots" TO "service_role" USING (true) WITH CHECK (true);
