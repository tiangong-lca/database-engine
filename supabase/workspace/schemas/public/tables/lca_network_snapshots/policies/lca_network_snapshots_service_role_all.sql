CREATE POLICY "lca_network_snapshots_service_role_all" ON "public"."lca_network_snapshots" TO "service_role" USING (true) WITH CHECK (true);
