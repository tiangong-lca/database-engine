CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_import_enqueue_v2"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare v_existing uuid; v_schema text; v_result jsonb; v_worker_id uuid;
begin
  select worker_job_id into v_existing from private.lca_package_artifacts
  where id = p_source_artifact_id and job_id = p_job_id
    and metadata ->> 'requested_by' = p_requested_by::text for update;
  if v_existing is not null then
    select payload_schema_version into v_schema from private.worker_jobs where id = v_existing;
    if v_schema is distinct from 'tidas.import_package.request.v2' then
      return jsonb_build_object('ok', false, 'code', 'IMPORT_POLICY_MISMATCH', 'status', 409);
    end if;
  end if;
  v_result := api.svc_tidas_package_import_enqueue(p_requested_by, p_job_id, p_source_artifact_id,
    p_artifact_sha256, p_artifact_byte_size, p_filename, p_content_type);
  if coalesce((v_result ->> 'ok')::boolean, false) then
    v_worker_id := (v_result ->> 'worker_job_id')::uuid;
    update private.worker_jobs set payload_schema_version = 'tidas.import_package.request.v2',
      payload_json = payload_json || jsonb_build_object('import_policy', 'root_closure_v2')
    where id = v_worker_id and status = 'queued' and attempt_count = 0;
  end if;
  return v_result;
end $$;

ALTER FUNCTION "api"."svc_tidas_package_import_enqueue_v2"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_import_enqueue_v2"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_import_enqueue_v2"("p_requested_by" "uuid", "p_job_id" "uuid", "p_source_artifact_id" "uuid", "p_artifact_sha256" "text", "p_artifact_byte_size" bigint, "p_filename" "text", "p_content_type" "text") TO "service_role";
