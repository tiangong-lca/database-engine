CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
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
$$;

ALTER FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") TO "service_role";
