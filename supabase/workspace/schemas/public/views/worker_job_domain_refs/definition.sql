CREATE OR REPLACE VIEW "public"."worker_job_domain_refs" WITH ("security_invoker"='true') AS
 SELECT "lca_results"."worker_job_id",
    'lca_results'::"text" AS "domain_source",
    "lca_results"."id" AS "domain_id",
    'lca_result_artifact'::"text" AS "domain_role",
    "lca_results"."job_id" AS "legacy_job_id",
    NULL::"text" AS "status",
    "lca_results"."created_at",
    "lca_results"."created_at" AS "updated_at"
   FROM "public"."lca_results"
  WHERE ("lca_results"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_result_cache"."worker_job_id",
    'lca_result_cache'::"text" AS "domain_source",
    "lca_result_cache"."id" AS "domain_id",
    'lca_result_cache'::"text" AS "domain_role",
    "lca_result_cache"."job_id" AS "legacy_job_id",
    "lca_result_cache"."status",
    "lca_result_cache"."created_at",
    "lca_result_cache"."updated_at"
   FROM "public"."lca_result_cache"
  WHERE ("lca_result_cache"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_latest_all_unit_results"."worker_job_id",
    'lca_latest_all_unit_results'::"text" AS "domain_source",
    "lca_latest_all_unit_results"."id" AS "domain_id",
    'lca_latest_all_unit_result'::"text" AS "domain_role",
    "lca_latest_all_unit_results"."job_id" AS "legacy_job_id",
    "lca_latest_all_unit_results"."status",
    "lca_latest_all_unit_results"."created_at",
    "lca_latest_all_unit_results"."updated_at"
   FROM "public"."lca_latest_all_unit_results"
  WHERE ("lca_latest_all_unit_results"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_factorization_registry"."prepared_worker_job_id" AS "worker_job_id",
    'lca_factorization_registry'::"text" AS "domain_source",
    "lca_factorization_registry"."id" AS "domain_id",
    'lca_factorization_artifact'::"text" AS "domain_role",
    "lca_factorization_registry"."prepared_job_id" AS "legacy_job_id",
    "lca_factorization_registry"."status",
    "lca_factorization_registry"."created_at",
    "lca_factorization_registry"."updated_at"
   FROM "public"."lca_factorization_registry"
  WHERE ("lca_factorization_registry"."prepared_worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_package_artifacts"."worker_job_id",
    'lca_package_artifacts'::"text" AS "domain_source",
    "lca_package_artifacts"."id" AS "domain_id",
    'package_artifact'::"text" AS "domain_role",
    "lca_package_artifacts"."job_id" AS "legacy_job_id",
    "lca_package_artifacts"."status",
    "lca_package_artifacts"."created_at",
    "lca_package_artifacts"."updated_at"
   FROM "public"."lca_package_artifacts"
  WHERE ("lca_package_artifacts"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_package_export_items"."worker_job_id",
    'lca_package_export_items'::"text" AS "domain_source",
    "lca_package_export_items"."id" AS "domain_id",
    'package_export_item'::"text" AS "domain_role",
    "lca_package_export_items"."job_id" AS "legacy_job_id",
    NULL::"text" AS "status",
    "lca_package_export_items"."created_at",
    "lca_package_export_items"."updated_at"
   FROM "public"."lca_package_export_items"
  WHERE ("lca_package_export_items"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_package_request_cache"."worker_job_id",
    'lca_package_request_cache'::"text" AS "domain_source",
    "lca_package_request_cache"."id" AS "domain_id",
    'package_request_cache'::"text" AS "domain_role",
    "lca_package_request_cache"."job_id" AS "legacy_job_id",
    "lca_package_request_cache"."status",
    "lca_package_request_cache"."created_at",
    "lca_package_request_cache"."updated_at"
   FROM "public"."lca_package_request_cache"
  WHERE ("lca_package_request_cache"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "dataset_review_submit_requests"."submit_worker_job_id" AS "worker_job_id",
    'dataset_review_submit_requests'::"text" AS "domain_source",
    "dataset_review_submit_requests"."id" AS "domain_id",
    'review_submit_coordinator'::"text" AS "domain_role",
    NULL::"uuid" AS "legacy_job_id",
    "dataset_review_submit_requests"."status",
    "dataset_review_submit_requests"."created_at",
    "dataset_review_submit_requests"."modified_at" AS "updated_at"
   FROM "public"."dataset_review_submit_requests"
  WHERE ("dataset_review_submit_requests"."submit_worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "dataset_review_submit_gate_runs"."worker_job_id",
    'dataset_review_submit_gate_runs'::"text" AS "domain_source",
    "dataset_review_submit_gate_runs"."id" AS "domain_id",
    'review_submit_gate_report'::"text" AS "domain_role",
    NULL::"uuid" AS "legacy_job_id",
    "dataset_review_submit_gate_runs"."status",
    "dataset_review_submit_gate_runs"."created_at",
    "dataset_review_submit_gate_runs"."modified_at" AS "updated_at"
   FROM "public"."dataset_review_submit_gate_runs"
  WHERE ("dataset_review_submit_gate_runs"."worker_job_id" IS NOT NULL);

ALTER VIEW "public"."worker_job_domain_refs" OWNER TO "postgres";
