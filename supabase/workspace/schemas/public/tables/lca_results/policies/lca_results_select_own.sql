CREATE POLICY "lca_results_select_own" ON "public"."lca_results" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."worker_jobs" "worker_job"
  WHERE (("worker_job"."id" = "lca_results"."worker_job_id") AND ("worker_job"."requested_by" = ( SELECT "auth"."uid"() AS "uid"))))));
