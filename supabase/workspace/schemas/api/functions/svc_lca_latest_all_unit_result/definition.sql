CREATE OR REPLACE FUNCTION "api"."svc_lca_latest_all_unit_result"("p_snapshot_id" "uuid", "p_requested_by" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'ok', true,
    'data', (
      select jsonb_strip_nulls(jsonb_build_object(
        'id', result.id,
        'snapshotId', result.snapshot_id,
        'jobId', result.job_id,
        'workerJobId', result.worker_job_id,
        'resultId', result.result_id,
        'status', result.status,
        'queryArtifactUrl', result.query_artifact_url,
        'queryArtifactSha256', result.query_artifact_sha256,
        'queryArtifactByteSize', result.query_artifact_byte_size,
        'queryArtifactFormat', result.query_artifact_format,
        'computedAt', result.computed_at,
        'updatedAt', result.updated_at
      ))
      from private.lca_latest_all_unit_results as result
      join private.worker_jobs as worker on worker.id = result.worker_job_id
      where result.snapshot_id = p_snapshot_id
        and result.status = 'ready'
        and worker.job_kind = 'lca.solve_all_unit'
        and worker.requested_by = p_requested_by
      order by result.updated_at desc, result.id
      limit 1
    )
  )
$$;

ALTER FUNCTION "api"."svc_lca_latest_all_unit_result"("p_snapshot_id" "uuid", "p_requested_by" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_lca_latest_all_unit_result"("p_snapshot_id" "uuid", "p_requested_by" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_lca_latest_all_unit_result"("p_snapshot_id" "uuid", "p_requested_by" "uuid") TO "service_role";
