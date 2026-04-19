CREATE POLICY "lca_latest_all_unit_results_service_role_all" ON "public"."lca_latest_all_unit_results" TO "service_role" USING (true) WITH CHECK (true);
