-- Issue #448 / workspace #566
--
-- Package artifacts are authorized by ownership of their request cache or
-- canonical worker job. Artifact metadata is worker output and must not be an
-- authorization source. Worker diagnostics describe only the current attempt;
-- the append-only worker_job_events table retains attempt history.

do $worker_attempt_diagnostics$
declare
  v_oid oid;
  v_definition text;
  v_rewritten text;
begin
  select routine.oid
    into strict v_oid
  from pg_proc as routine
  join pg_namespace as namespace on namespace.oid = routine.pronamespace
  where namespace.nspname = 'private'
    and routine.proname = 'worker_claim_jobs';

  v_definition := pg_get_functiondef(v_oid);
  v_rewritten := replace(
    v_definition,
    $old$          updated_at = now(),
          error_code = null,$old$,
    $new$          updated_at = now(),
          diagnostics = case
            when j.attempt_count > 0 then '{}'::jsonb
            else j.diagnostics
          end,
          error_code = null,$new$
  );
  if v_rewritten = v_definition then
    raise exception 'private.worker_claim_jobs diagnostics rewrite target not found';
  end if;
  execute v_rewritten;

  select routine.oid
    into strict v_oid
  from pg_proc as routine
  join pg_namespace as namespace on namespace.oid = routine.pronamespace
  where namespace.nspname = 'private'
    and routine.proname = 'worker_record_job_result';

  v_definition := pg_get_functiondef(v_oid);
  v_rewritten := replace(
    v_definition,
    $old$        diagnostics = diagnostics || coalesce(p_diagnostics, '{}'::jsonb),$old$,
    $new$        diagnostics = coalesce(p_diagnostics, '{}'::jsonb),$new$
  );
  if v_rewritten = v_definition then
    raise exception 'private.worker_record_job_result diagnostics rewrite target not found';
  end if;
  execute v_rewritten;

  select routine.oid
    into strict v_oid
  from pg_proc as routine
  join pg_namespace as namespace on namespace.oid = routine.pronamespace
  where namespace.nspname = 'private'
    and routine.proname = 'worker_retry_job';

  v_definition := pg_get_functiondef(v_oid);
  v_rewritten := replace(
    v_definition,
    $old$        lease_expires_at = null,
        error_code = null,$old$,
    $new$        lease_expires_at = null,
        diagnostics = '{}'::jsonb,
        error_code = null,$new$
  );
  if v_rewritten = v_definition then
    raise exception 'private.worker_retry_job diagnostics rewrite target not found';
  end if;
  execute v_rewritten;
end
$worker_attempt_diagnostics$;

create or replace function api.svc_tidas_package_read(
  p_requested_by uuid,
  p_lookup_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_cache private.lca_package_request_cache%rowtype;
  v_job private.worker_jobs%rowtype;
  v_effective_job_id uuid;
  v_artifacts jsonb := '[]'::jsonb;
begin
  select *
    into v_cache
  from private.lca_package_request_cache
  where requested_by = p_requested_by
    and (job_id = p_lookup_id or worker_job_id = p_lookup_id)
  order by updated_at desc, id
  limit 1;

  select *
    into v_job
  from private.worker_jobs
  where requested_by = p_requested_by
    and job_kind in ('tidas.export_package', 'tidas.import_package')
    and (
      id = p_lookup_id
      or subject_id = p_lookup_id
      or (v_cache.job_id is not null and subject_id = v_cache.job_id)
    )
  order by (id = p_lookup_id) desc, created_at desc, id
  limit 1;

  -- Authorization must complete before an artifact lookup. In particular, a
  -- caller who knows another user's logical or worker job UUID receives the
  -- same null result as an unknown UUID.
  if v_cache.id is null and v_job.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  v_effective_job_id := coalesce(v_cache.job_id, v_job.subject_id);

  if v_effective_job_id is not null then
    select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'id', artifact.id,
      'workerJobId', artifact.worker_job_id,
      'artifactKind', artifact.artifact_kind,
      'status', artifact.status,
      'artifactUrl', artifact.artifact_url,
      'artifactSha256', artifact.artifact_sha256,
      'artifactByteSize', artifact.artifact_byte_size,
      'artifactFormat', artifact.artifact_format,
      'contentType', artifact.content_type,
      'metadata', artifact.metadata - 'requested_by' - 'import_prepare_idempotency_key',
      'expiresAt', artifact.expires_at,
      'isPinned', artifact.is_pinned,
      'createdAt', artifact.created_at,
      'updatedAt', artifact.updated_at
    )) order by artifact.created_at, artifact.id), '[]'::jsonb)
      into v_artifacts
    from private.lca_package_artifacts as artifact
    where artifact.job_id = v_effective_job_id;
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_strip_nulls(jsonb_build_object(
    'jobId', v_effective_job_id,
    'workerJobId', coalesce(v_job.id, v_cache.worker_job_id),
    'status', coalesce(v_job.status, v_cache.status),
    'operation', coalesce(
      v_cache.operation,
      case v_job.job_kind
        when 'tidas.export_package' then 'export_package'
        when 'tidas.import_package' then 'import_package'
      end
    ),
    'scope', v_job.payload_json ->> 'scope',
    'rootCount', coalesce(jsonb_array_length(v_job.payload_json -> 'roots'), 0),
    'requestKey', coalesce(v_cache.request_key, v_job.request_hash),
    'payload', v_job.payload_json,
    'diagnostics', v_job.diagnostics,
    'startedAt', v_job.started_at,
    'finishedAt', v_job.finished_at,
    'artifacts', v_artifacts,
    'requestCache', case when v_cache.id is null then null else jsonb_strip_nulls(jsonb_build_object(
      'id', v_cache.id,
      'status', v_cache.status,
      'error_code', v_cache.error_code,
      'error_message', v_cache.error_message,
      'hit_count', v_cache.hit_count,
      'last_accessed_at', v_cache.last_accessed_at,
      'created_at', v_cache.created_at,
      'updated_at', v_cache.updated_at,
      'export_artifact_id', v_cache.export_artifact_id,
      'report_artifact_id', v_cache.report_artifact_id
    )) end,
    'createdAt', coalesce(v_job.created_at, v_cache.created_at),
    'updatedAt', coalesce(v_job.updated_at, v_cache.updated_at)
  )));
end
$function$;
