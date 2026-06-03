CREATE OR REPLACE VIEW "public"."worker_legacy_lifecycle_audit" WITH ("security_invoker"='true') AS
 SELECT 'worker_jobs'::"text" AS "legacy_source",
    "worker_jobs"."job_kind" AS "task_family",
    "worker_jobs"."status" AS "legacy_status",
    "count"(*) AS "row_count",
    "count"(*) FILTER (WHERE ("worker_jobs"."status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'stale'::"text", 'blocked'::"text"]))) AS "active_count",
    "min"("worker_jobs"."created_at") AS "oldest_created_at",
    "max"("worker_jobs"."created_at") AS "newest_created_at",
    "max"("worker_jobs"."updated_at") AS "latest_updated_at"
   FROM "public"."worker_jobs"
  WHERE (("worker_jobs"."worker_runtime" = 'calculator'::"text") AND ("worker_jobs"."job_kind" = ANY (ARRAY['lca.solve_one'::"text", 'lca.solve_batch'::"text", 'lca.solve_all_unit'::"text", 'lca.build_snapshot'::"text", 'lca.contribution_path'::"text", 'lca.factorization_prepare'::"text", 'lca.snapshot_gc'::"text", 'lca.result_gc'::"text", 'tidas.package_artifact_gc'::"text", 'tidas.export_package'::"text", 'tidas.import_package'::"text", 'review_submit.submit'::"text", 'review_submit.gate'::"text"])))
  GROUP BY "worker_jobs"."job_kind", "worker_jobs"."status"
UNION ALL
 SELECT 'dataset_review_submit_gate_runs'::"text" AS "legacy_source",
    'review_submit.gate'::"text" AS "task_family",
    "dataset_review_submit_gate_runs"."status" AS "legacy_status",
    "count"(*) AS "row_count",
    "count"(*) FILTER (WHERE ("dataset_review_submit_gate_runs"."status" = ANY (ARRAY['queued'::"text", 'running'::"text"]))) AS "active_count",
    "min"("dataset_review_submit_gate_runs"."created_at") AS "oldest_created_at",
    "max"("dataset_review_submit_gate_runs"."created_at") AS "newest_created_at",
    "max"("dataset_review_submit_gate_runs"."modified_at") AS "latest_updated_at"
   FROM "public"."dataset_review_submit_gate_runs"
  GROUP BY "dataset_review_submit_gate_runs"."status";

ALTER VIEW "public"."worker_legacy_lifecycle_audit" OWNER TO "postgres";
