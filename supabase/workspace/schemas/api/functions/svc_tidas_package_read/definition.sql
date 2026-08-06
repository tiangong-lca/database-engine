CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_cache private.lca_package_request_cache%rowtype;
  v_job private.worker_jobs%rowtype;
  v_effective_job_id uuid;
  v_artifacts jsonb;
begin
  select * into v_cache from private.lca_package_request_cache
  where requested_by = p_requested_by
    and (job_id = p_lookup_id or worker_job_id = p_lookup_id)
  order by updated_at desc, id limit 1;
  v_effective_job_id := coalesce(v_cache.job_id, p_lookup_id);

  select * into v_job from private.worker_jobs
  where requested_by = p_requested_by
    and job_kind in ('tidas.export_package', 'tidas.import_package')
    and (id = p_lookup_id or subject_id = v_effective_job_id)
  order by created_at desc, id limit 1;

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
  where artifact.job_id = v_effective_job_id
    and artifact.metadata ->> 'requested_by' = p_requested_by::text;

  if v_cache.id is null and v_job.id is null and jsonb_array_length(v_artifacts) = 0 then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_strip_nulls(jsonb_build_object(
    'jobId', v_effective_job_id,
    'workerJobId', coalesce(v_job.id, v_cache.worker_job_id),
    'status', coalesce(v_job.status, v_cache.status),
    'operation', v_cache.operation,
    'requestKey', v_cache.request_key,
    'artifacts', v_artifacts,
    'createdAt', coalesce(v_job.created_at, v_cache.created_at),
    'updatedAt', coalesce(v_job.updated_at, v_cache.updated_at)
  )));
end
$$;

ALTER FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_read"("p_requested_by" "uuid", "p_lookup_id" "uuid") TO "service_role";
