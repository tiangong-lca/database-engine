CREATE POLICY "lca_package_request_cache_select_own" ON "private"."lca_package_request_cache" FOR SELECT TO "authenticated" USING (("requested_by" = ( SELECT "auth"."uid"() AS "uid")));
