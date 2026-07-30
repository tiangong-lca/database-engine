CREATE OR REPLACE VIEW "public"."worker_domain_traceability_violations" WITH ("security_invoker"='true') AS
 SELECT 'lca_results'::"text" AS "domain_source",
    "results"."id" AS "domain_id",
    'lca_result_artifact'::"text" AS "domain_role",
    "results"."created_at",
    "results"."created_at" AS "updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyJobId', "results"."job_id", 'snapshotId', "results"."snapshot_id") AS "details"
   FROM ("public"."lca_results" "results"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_results'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("results"."created_at" >= "cutoffs"."cutover_at") AND ("results"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'lca_result_cache'::"text" AS "domain_source",
    "cache"."id" AS "domain_id",
    'lca_result_cache'::"text" AS "domain_role",
    "cache"."created_at",
    "cache"."updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyJobId', "cache"."job_id", 'snapshotId', "cache"."snapshot_id", 'status', "cache"."status") AS "details"
   FROM ("public"."lca_result_cache" "cache"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_result_cache'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("cache"."created_at" >= "cutoffs"."cutover_at") AND ("cache"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'lca_latest_all_unit_results'::"text" AS "domain_source",
    "latest"."id" AS "domain_id",
    'lca_latest_all_unit_result'::"text" AS "domain_role",
    "latest"."created_at",
    "latest"."updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyJobId', "latest"."job_id", 'snapshotId', "latest"."snapshot_id", 'status', "latest"."status") AS "details"
   FROM ("public"."lca_latest_all_unit_results" "latest"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_latest_all_unit_results'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("latest"."created_at" >= "cutoffs"."cutover_at") AND ("latest"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'lca_factorization_registry'::"text" AS "domain_source",
    "registry"."id" AS "domain_id",
    'lca_factorization_artifact'::"text" AS "domain_role",
    "registry"."created_at",
    "registry"."updated_at",
    "cutoffs"."cutover_at",
    'missing_prepared_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyPreparedJobId', "registry"."prepared_job_id", 'snapshotId', "registry"."snapshot_id", 'status', "registry"."status") AS "details"
   FROM ("public"."lca_factorization_registry" "registry"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_factorization_registry'::"text")))
  WHERE ("cutoffs"."traceability_required" AND (COALESCE("registry"."prepared_at", "registry"."created_at") >= "cutoffs"."cutover_at") AND ("registry"."status" = ANY (ARRAY['ready'::"text", 'failed'::"text", 'stale'::"text"])) AND ("registry"."prepared_worker_job_id" IS NULL))
UNION ALL
 SELECT 'lca_package_artifacts'::"text" AS "domain_source",
    "artifacts"."id" AS "domain_id",
    'package_artifact'::"text" AS "domain_role",
    "artifacts"."created_at",
    "artifacts"."updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyJobId', "artifacts"."job_id", 'artifactKind', "artifacts"."artifact_kind", 'status', "artifacts"."status") AS "details"
   FROM ("public"."lca_package_artifacts" "artifacts"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_package_artifacts'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("artifacts"."created_at" >= "cutoffs"."cutover_at") AND ("artifacts"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'lca_package_export_items'::"text" AS "domain_source",
    "export_items"."id" AS "domain_id",
    'package_export_item'::"text" AS "domain_role",
    "export_items"."created_at",
    "export_items"."updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyJobId', "export_items"."job_id", 'tableName', "export_items"."table_name", 'datasetId', "export_items"."dataset_id") AS "details"
   FROM ("public"."lca_package_export_items" "export_items"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_package_export_items'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("export_items"."created_at" >= "cutoffs"."cutover_at") AND ("export_items"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'lca_package_request_cache'::"text" AS "domain_source",
    "request_cache"."id" AS "domain_id",
    'package_request_cache'::"text" AS "domain_role",
    "request_cache"."created_at",
    "request_cache"."updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('legacyJobId', "request_cache"."job_id", 'operation', "request_cache"."operation", 'status', "request_cache"."status") AS "details"
   FROM ("public"."lca_package_request_cache" "request_cache"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'lca_package_request_cache'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("request_cache"."created_at" >= "cutoffs"."cutover_at") AND ("request_cache"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'dataset_review_submit_requests'::"text" AS "domain_source",
    "requests"."id" AS "domain_id",
    'review_submit_coordinator'::"text" AS "domain_role",
    "requests"."created_at",
    "requests"."modified_at" AS "updated_at",
    "cutoffs"."cutover_at",
    "missing"."violation_code",
    "jsonb_build_object"('status', "requests"."status", 'gateRunId', "requests"."gate_run_id", 'submitWorkerJobId', "requests"."submit_worker_job_id", 'gateWorkerJobId', "requests"."gate_worker_job_id") AS "details"
   FROM (("public"."dataset_review_submit_requests" "requests"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'dataset_review_submit_requests'::"text")))
     CROSS JOIN LATERAL ( VALUES ('missing_submit_worker_job_id'::"text","requests"."submit_worker_job_id",true), ('missing_gate_worker_job_id'::"text","requests"."gate_worker_job_id",(("requests"."gate_run_id" IS NOT NULL) OR ("requests"."status" = ANY (ARRAY['waiting_gate'::"text", 'submitting'::"text", 'submitted'::"text", 'blocked'::"text"]))))) "missing"("violation_code", "worker_job_id", "is_required"))
  WHERE ("cutoffs"."traceability_required" AND ("requests"."created_at" >= "cutoffs"."cutover_at") AND ("requests"."status" <> 'cancelled'::"text") AND "missing"."is_required" AND ("missing"."worker_job_id" IS NULL))
UNION ALL
 SELECT 'dataset_review_submit_gate_runs'::"text" AS "domain_source",
    "gate_runs"."id" AS "domain_id",
    'review_submit_gate_report'::"text" AS "domain_role",
    "gate_runs"."created_at",
    "gate_runs"."modified_at" AS "updated_at",
    "cutoffs"."cutover_at",
    'missing_worker_job_id'::"text" AS "violation_code",
    "jsonb_build_object"('status', "gate_runs"."status", 'requestedBy', "gate_runs"."requested_by") AS "details"
   FROM ("public"."dataset_review_submit_gate_runs" "gate_runs"
     JOIN "public"."worker_domain_traceability_cutoffs" "cutoffs" ON (("cutoffs"."domain_source" = 'dataset_review_submit_gate_runs'::"text")))
  WHERE ("cutoffs"."traceability_required" AND ("gate_runs"."created_at" >= "cutoffs"."cutover_at") AND ("gate_runs"."worker_job_id" IS NULL));

ALTER VIEW "public"."worker_domain_traceability_violations" OWNER TO "postgres";
