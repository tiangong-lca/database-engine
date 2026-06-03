-- Add LCA-specific service projections so Edge Functions can stop reading
-- public.lca_jobs for production worker_jobs-backed result/status paths.

create or replace function public.lca_legacy_job_type(
  p_job_kind text
) returns text
language sql
immutable
set search_path = public, pg_temp
as $$
  select case lower(trim(coalesce(p_job_kind, '')))
    when 'lca.solve_one' then 'solve_one'
    when 'lca.solve_batch' then 'solve_batch'
    when 'lca.solve_all_unit' then 'solve_all_unit'
    when 'lca.build_snapshot' then 'build_snapshot'
    when 'lca.contribution_path' then 'analyze_contribution_path'
    when 'lca.factorization_prepare' then 'prepare_factorization'
    when 'lca.snapshot_gc' then 'snapshot_gc'
    when 'lca.result_gc' then 'result_gc'
    else null
  end
$$;

create or replace function public.lca_read_job_projection(
  p_requested_by uuid,
  p_worker_job_id uuid default null,
  p_legacy_job_id uuid default null,
  p_include_internal boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_legacy_job_id uuid;
  v_snapshot_id text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read LCA job projections'
    );
  end if;

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

create or replace function public.lca_read_result_projection(
  p_requested_by uuid,
  p_result_id uuid,
  p_required_artifact_format text default null,
  p_include_internal boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_result public.lca_results%rowtype;
  v_job public.worker_jobs%rowtype;
  v_required_format text := nullif(trim(p_required_artifact_format), '');
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read LCA result projections'
    );
  end if;

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

create or replace function public.lca_read_latest_single_solve_result(
  p_requested_by uuid,
  p_snapshot_id uuid,
  p_process_index integer
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_cache public.lca_result_cache%rowtype;
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_amount numeric;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read LCA solve projections'
    );
  end if;

  if p_snapshot_id is null or p_process_index is null or p_process_index < 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_SOLVE_LOOKUP',
      'status', 400,
      'message', 'p_snapshot_id and a non-negative p_process_index are required'
    );
  end if;

  select cache_row.*
    into v_cache
  from public.lca_result_cache as cache_row
  join public.worker_jobs as worker_job
    on worker_job.id = cache_row.worker_job_id
  join public.lca_results as result_row
    on result_row.id = cache_row.result_id
  where cache_row.snapshot_id = p_snapshot_id
    and cache_row.status = 'ready'
    and cache_row.result_id is not null
    and worker_job.job_kind = 'lca.solve_one'
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
    and cache_row.request_payload->>'demand_mode' = 'single'
    and cache_row.request_payload#>>'{demand,process_index}' ~ '^[0-9]+$'
    and (cache_row.request_payload#>>'{demand,process_index}')::integer = p_process_index
  order by cache_row.updated_at desc, cache_row.created_at desc
  limit 1;

  if v_cache.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_cache.worker_job_id;

  select *
    into v_result
  from public.lca_results
  where id = v_cache.result_id;

  v_amount := case
    when jsonb_typeof(v_cache.request_payload#>'{demand,amount}') = 'number'
      then (v_cache.request_payload#>>'{demand,amount}')::numeric
    else 1
  end;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'snapshotId', v_cache.snapshot_id,
        'processIndex', p_process_index,
        'amount', v_amount,
        'cache', jsonb_strip_nulls(
          jsonb_build_object(
            'cacheId', v_cache.id,
            'requestKey', v_cache.request_key,
            'status', v_cache.status,
            'createdAt', v_cache.created_at,
            'updatedAt', v_cache.updated_at
          )
        ),
        'result', jsonb_strip_nulls(
          jsonb_build_object(
            'resultId', v_result.id,
            'legacyJobId', v_result.job_id,
            'workerJobId', v_result.worker_job_id,
            'snapshotId', v_result.snapshot_id,
            'createdAt', v_result.created_at,
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
        'workerJob', public.worker_job_payload(v_job, false)
      )
    )
  );
end;
$$;

revoke all on function public.lca_legacy_job_type(text) from public, anon, authenticated;
revoke all on function public.lca_read_job_projection(uuid, uuid, uuid, boolean) from public, anon, authenticated;
revoke all on function public.lca_read_result_projection(uuid, uuid, text, boolean) from public, anon, authenticated;
revoke all on function public.lca_read_latest_single_solve_result(uuid, uuid, integer) from public, anon, authenticated;

grant execute on function public.lca_read_job_projection(uuid, uuid, uuid, boolean) to service_role;
grant execute on function public.lca_read_result_projection(uuid, uuid, text, boolean) to service_role;
grant execute on function public.lca_read_latest_single_solve_result(uuid, uuid, integer) to service_role;

comment on function public.lca_read_job_projection(uuid, uuid, uuid, boolean)
  is 'Service-role LCA job/status/result projection backed by worker_jobs and retained result metadata, not public.lca_jobs.';
comment on function public.lca_read_result_projection(uuid, uuid, text, boolean)
  is 'Service-role LCA result projection with worker_jobs ownership, replacing Edge reads through public.lca_jobs.';
comment on function public.lca_read_latest_single_solve_result(uuid, uuid, integer)
  is 'Service-role latest solve_one result projection from worker_jobs-backed lca_result_cache, replacing lca_jobs payload scans.';
