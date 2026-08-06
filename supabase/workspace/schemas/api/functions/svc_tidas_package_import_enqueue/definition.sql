CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_import_enqueue"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_artifact private.lca_package_artifacts%rowtype;
  v_worker private.worker_jobs%rowtype;
  v_enqueue jsonb;
  v_worker_id uuid;
begin
  select * into v_artifact from private.lca_package_artifacts
  where id = p_source_artifact_id and job_id = p_job_id
  for update;
  if v_artifact.id is null or v_artifact.metadata ->> 'requested_by' <> p_requested_by::text then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_JOB_NOT_FOUND', 'status', 404);
  end if;
  if v_artifact.artifact_kind <> 'import_source' or v_artifact.status in ('failed', 'deleted')
     or (v_artifact.expires_at is not null and v_artifact.expires_at <= now()) then
    return jsonb_build_object('ok', false, 'code', 'IMPORT_SOURCE_NOT_USABLE', 'status', 409);
  end if;
  if p_artifact_byte_size is null or p_artifact_byte_size < 0
     or nullif(btrim(coalesce(p_artifact_sha256, '')), '') is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_IMPORT_ARTIFACT', 'status', 400);
  end if;
  if v_artifact.worker_job_id is not null then
    select * into v_worker from private.worker_jobs
    where id = v_artifact.worker_job_id
      and requested_by = p_requested_by
      and job_kind = 'tidas.import_package';
    if v_worker.status = 'completed' then
      return jsonb_build_object('ok', true, 'mode', 'completed', 'job_id', p_job_id, 'worker_job_id', v_worker.id, 'source_artifact_id', v_artifact.id);
    elsif v_worker.status in ('queued', 'running', 'waiting', 'stale') then
      return jsonb_build_object('ok', true, 'mode', 'in_progress', 'job_id', p_job_id, 'worker_job_id', v_worker.id, 'source_artifact_id', v_artifact.id);
    end if;
  end if;

  v_enqueue := private.worker_enqueue_job(
    p_job_kind => 'tidas.import_package',
    p_payload_json => jsonb_build_object(
      'type', 'import_package', 'job_id', p_job_id,
      'requested_by', p_requested_by, 'source_artifact_id', p_source_artifact_id
    ),
    p_payload_schema_version => 'tidas.import_package.request.v1',
    p_subject_type => 'lca_package_job', p_subject_id => p_job_id,
    p_subject_version => p_source_artifact_id::text,
    p_requested_by => p_requested_by, p_requester_type => 'user',
    p_idempotency_key => p_requested_by::text || ':import_package:' || p_job_id::text,
    p_request_hash => p_artifact_sha256, p_queue_key => p_source_artifact_id::text,
    p_visibility => 'user'
  );
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then return v_enqueue; end if;
  v_worker_id := (v_enqueue #>> '{data,id}')::uuid;

  update private.lca_package_artifacts set
    status = 'ready', artifact_sha256 = p_artifact_sha256,
    artifact_byte_size = p_artifact_byte_size,
    content_type = coalesce(nullif(btrim(p_content_type), ''), content_type),
    worker_job_id = v_worker_id,
    metadata = metadata || jsonb_build_object(
      'filename', coalesce(nullif(btrim(p_filename), ''), metadata ->> 'filename', 'package.zip'),
      'original_filename', coalesce(nullif(btrim(p_filename), ''), metadata ->> 'original_filename', 'package.zip'),
      'upload_state', 'uploaded', 'phase', 'enqueue_import'
    ),
    updated_at = now()
  where id = v_artifact.id;

  return jsonb_build_object('ok', true, 'mode', 'queued', 'job_id', p_job_id, 'worker_job_id', v_worker_id, 'source_artifact_id', v_artifact.id);
end
$$;

ALTER FUNCTION "api"."svc_tidas_package_import_enqueue"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_import_enqueue"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_import_enqueue"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text") TO "service_role";
