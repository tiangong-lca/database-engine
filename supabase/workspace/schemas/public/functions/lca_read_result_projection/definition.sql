CREATE OR REPLACE FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text" DEFAULT NULL::"text", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_result public.lca_results%rowtype;
  v_job public.worker_jobs%rowtype;
  v_required_format text := nullif(trim(p_required_artifact_format), '');
begin
  -- Authorization is enforced by EXECUTE grants below; runtime service clients can
  -- call this RPC even when request-header GUCs are not populated by PostgREST.

  if p_result_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_RESULT_LOOKUP',
      'status', 400,
      'message', 'p_result_id is required'
    );
  end if;

  select result_row.*
    into v_result
  from public.lca_results as result_row
  join public.worker_jobs as worker_job
    on worker_job.id = result_row.worker_job_id
  where result_row.id = p_result_id
    and worker_job.job_kind = any(array[
      'lca.solve_one',
      'lca.solve_batch',
      'lca.solve_all_unit',
      'lca.contribution_path'
    ])
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
  limit 1;

  if v_result.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_result.worker_job_id;

  if v_required_format is not null
    and coalesce(v_result.artifact_format, '') <> v_required_format then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_LCA_RESULT_ARTIFACT_FORMAT',
      'status', 409,
      'message', 'LCA result artifact format is not supported for this read path',
      'details', jsonb_build_object(
        'resultId', v_result.id,
        'expectedArtifactFormat', v_required_format,
        'actualArtifactFormat', v_result.artifact_format
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'result', jsonb_strip_nulls(
          jsonb_build_object(
            'resultId', v_result.id,
            'legacyJobId', v_result.job_id,
            'workerJobId', v_result.worker_job_id,
            'snapshotId', v_result.snapshot_id,
            'createdAt', v_result.created_at,
            'diagnostics', v_result.diagnostics,
            'artifact', jsonb_strip_nulls(
              jsonb_build_object(
                'artifactUrl', v_result.artifact_url,
                'artifactFormat', v_result.artifact_format,
                'artifactByteSize', v_result.artifact_byte_size,
                'artifactSha256', v_result.artifact_sha256
              )
            )
          )
        ),
        'job', jsonb_strip_nulls(
          jsonb_build_object(
            'workerJobId', v_job.id,
            'legacyJobId', v_result.job_id,
            'snapshotId', v_result.snapshot_id,
            'jobKind', v_job.job_kind,
            'jobType', public.lca_legacy_job_type(v_job.job_kind),
            'status', v_job.status,
            'phase', v_job.phase,
            'progress', v_job.progress,
            'timestamps', jsonb_strip_nulls(
              jsonb_build_object(
                'createdAt', v_job.created_at,
                'startedAt', v_job.started_at,
                'finishedAt', v_job.finished_at,
                'updatedAt', v_job.updated_at
              )
            )
          )
        ),
        'workerJob', public.worker_job_payload(v_job, p_include_internal)
      )
    )
  );
end;
$$;

ALTER FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) TO "service_role";
