CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_import_prepare"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_url" "text", "p_content_type" "text", "p_filename" "text", "p_idempotency_key" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_existing private.lca_package_artifacts%rowtype;
  v_idempotency_key text := nullif(btrim(coalesce(p_idempotency_key, '')), '');
begin
  if p_requested_by is null or p_job_id is null or p_source_artifact_id is null
     or nullif(btrim(p_artifact_url), '') is null or nullif(btrim(p_content_type), '') is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_IMPORT_PREPARE_REQUEST', 'status', 400);
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    p_requested_by::text || ':import_prepare:' || coalesce(v_idempotency_key, p_job_id::text), 0
  ));
  if v_idempotency_key is not null then
    select * into v_existing from private.lca_package_artifacts
    where artifact_kind = 'import_source'
      and metadata ->> 'requested_by' = p_requested_by::text
      and metadata ->> 'import_prepare_idempotency_key' = v_idempotency_key
    order by created_at desc, id limit 1;
    if found then
      return jsonb_build_object(
        'ok', true, 'mode', 'reused', 'job_id', v_existing.job_id,
        'source_artifact_id', v_existing.id, 'artifact_url', v_existing.artifact_url
      );
    end if;
  end if;

  insert into private.lca_package_artifacts (
    id, job_id, artifact_kind, status, artifact_url, artifact_format,
    content_type, metadata, created_at, updated_at
  ) values (
    p_source_artifact_id, p_job_id, 'import_source', 'pending', p_artifact_url,
    'tidas-package-zip:v1', p_content_type,
    jsonb_strip_nulls(jsonb_build_object(
      'filename', coalesce(nullif(btrim(p_filename), ''), 'package.zip'),
      'original_filename', coalesce(nullif(btrim(p_filename), ''), 'package.zip'),
      'upload_state', 'prepared',
      'requested_by', p_requested_by,
      'import_prepare_idempotency_key', v_idempotency_key
    )), now(), now()
  );
  return jsonb_build_object(
    'ok', true, 'mode', 'prepared', 'job_id', p_job_id,
    'source_artifact_id', p_source_artifact_id, 'artifact_url', p_artifact_url
  );
end
$$;

ALTER FUNCTION "api"."svc_tidas_package_import_prepare"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_url" "text", "p_content_type" "text", "p_filename" "text", "p_idempotency_key" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_import_prepare"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_url" "text", "p_content_type" "text", "p_filename" "text", "p_idempotency_key" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_import_prepare"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_url" "text", "p_content_type" "text", "p_filename" "text", "p_idempotency_key" "text") TO "service_role";
