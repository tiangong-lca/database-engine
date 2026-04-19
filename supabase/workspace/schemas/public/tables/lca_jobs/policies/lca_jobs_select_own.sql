CREATE POLICY "lca_jobs_select_own" ON "public"."lca_jobs" FOR SELECT TO "authenticated" USING (("requested_by" = ( SELECT "auth"."uid"() AS "uid")));
