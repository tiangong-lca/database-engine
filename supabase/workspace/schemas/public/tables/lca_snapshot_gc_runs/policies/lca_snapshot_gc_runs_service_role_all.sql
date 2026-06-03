CREATE POLICY "lca_snapshot_gc_runs_service_role_all" ON "public"."lca_snapshot_gc_runs" TO "service_role" USING (true) WITH CHECK (true);
