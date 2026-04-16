CREATE POLICY "lca_package_request_cache_select_own" ON "public"."lca_package_request_cache" FOR SELECT TO "authenticated" USING (("requested_by" = "auth"."uid"()));
