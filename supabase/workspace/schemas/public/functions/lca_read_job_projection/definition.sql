CREATE OR REPLACE FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid" DEFAULT NULL::"uuid", "p_legacy_job_id" "uuid" DEFAULT NULL::"uuid", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_legacy_job_id uuid;
  v_snapshot_id text;
begin
  -- Authorization is enforced by EXECUTE grants below; runtime service clients can
  -- call this RPC even when request-header GUCs are not populated by PostgREST.

  if p_worker_job_id is null and p_legacy_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_JOB_LOOKUP',
      'status', 400,
      'message', 'p_worker_job_id or p_legacy_job_id is required'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs as worker_job
  where worker_job.job_kind = any(array[
      'lca.solve_one',
      'lca.solve_batch',
      'lca.solve_all_unit',
      'lca.build_snapshot',
      'lca.contribution_path',
      'lca.factorization_prepare'
    ])
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
    and (
      (p_worker_job_id is not null and worker_job.id = p_worker_job_id)
      or (
        p_legacy_job_id is not null
        and worker_job.subject_type = 'lca_job'
        and worker_job.subject_id = p_legacy_job_id
      )
      or (
        p_legacy_job_id is not null
        and worker_job.payload_json->>'job_id' = p_legacy_job_id::text
      )
      or (
        p_legacy_job_id is not null
        and worker_job.payload_json->>'lcaJobId' = p_legacy_job_id::text
      )
    )
  order by worker_job.updated_at desc, worker_job.created_at desc
  limit 1;

  if v_job.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  v_legacy_job_id := coalesce(
    p_legacy_job_id,
    case when v_job.subject_type = 'lca_job' then v_job.subject_id else null end,
    nullif(v_job.payload_json->>'job_id', '')::uuid,
    nullif(v_job.payload_json->>'lcaJobId', '')::uuid
  );

  select *
    into v_result
  from public.lca_results as result_row
  where result_row.worker_job_id = v_job.id
     or (v_legacy_job_id is not null and result_row.job_id = v_legacy_job_id)
  order by result_row.created_at desc
  limit 1;

  v_snapshot_id := coalesce(
    nullif(v_result.snapshot_id::text, ''),
    nullif(v_job.subject_version, ''),
    nullif(v_job.payload_json->>'snapshot_id', ''),
    nullif(v_job.payload_json->>'snapshotId', '')
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'job', jsonb_strip_nulls(
          jsonb_build_object(
            'workerJobId', v_job.id,
            'legacyJobId', v_legacy_job_id,
            'snapshotId', v_snapshot_id,
            'jobKind', v_job.job_kind,
            'jobType', public.lca_legacy_job_type(v_job.job_kind),
            'status', v_job.status,
            'phase', v_job.phase,
            'progress', v_job.progress,
            'payload', case when p_include_internal then v_job.payload_json else null end,
            'diagnostics', case when p_include_internal then v_job.diagnostics else null end,
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
        'workerJob', public.worker_job_payload(v_job, p_include_internal),
        'result', case
          when v_result.id is null then null
          else jsonb_strip_nulls(
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
          )
        end
      )
    )
  );
end;
$$;

ALTER FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) TO "service_role";
