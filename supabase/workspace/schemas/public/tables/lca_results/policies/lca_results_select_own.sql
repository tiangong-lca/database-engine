CREATE POLICY "lca_results_select_own" ON "public"."lca_results" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."lca_jobs" "j"
  WHERE (("j"."id" = "lca_results"."job_id") AND ("j"."requested_by" = ( SELECT "auth"."uid"() AS "uid"))))));
