CREATE POLICY "lca_factorization_registry_service_role_all" ON "private"."lca_factorization_registry" TO "service_role" USING (true) WITH CHECK (true);
