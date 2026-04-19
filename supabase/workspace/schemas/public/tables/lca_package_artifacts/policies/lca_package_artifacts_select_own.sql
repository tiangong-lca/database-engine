CREATE POLICY "lca_package_artifacts_select_own" ON "public"."lca_package_artifacts" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."lca_package_jobs" "j"
  WHERE (("j"."id" = "lca_package_artifacts"."job_id") AND ("j"."requested_by" = "auth"."uid"())))));
