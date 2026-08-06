CREATE POLICY "lca_package_artifacts_select_own" ON "private"."lca_package_artifacts" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "private"."worker_jobs" "worker_job"
  WHERE (("worker_job"."id" = "lca_package_artifacts"."worker_job_id") AND ("worker_job"."requested_by" = ( SELECT "auth"."uid"() AS "uid"))))));
