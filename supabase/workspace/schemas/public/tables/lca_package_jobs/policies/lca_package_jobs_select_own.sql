CREATE POLICY "lca_package_jobs_select_own" ON "public"."lca_package_jobs" FOR SELECT TO "authenticated" USING (("requested_by" = "auth"."uid"()));
