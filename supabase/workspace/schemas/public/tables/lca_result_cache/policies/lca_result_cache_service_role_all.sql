CREATE POLICY "lca_result_cache_service_role_all" ON "public"."lca_result_cache" TO "service_role" USING (true) WITH CHECK (true);
