CREATE POLICY "lca_snapshot_gc_run_items_service_role_all" ON "private"."lca_snapshot_gc_run_items" TO "service_role" USING (true) WITH CHECK (true);
