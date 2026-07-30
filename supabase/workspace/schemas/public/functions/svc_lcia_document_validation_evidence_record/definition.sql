CREATE OR REPLACE FUNCTION "public"."svc_lcia_document_validation_evidence_record"("p_records" "jsonb", "p_source_worker_job_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare v_record jsonb; v_inserted integer:=0;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_records,'null'::jsonb)) <> 'array' then return public.lcia_scope_closure_error('invalid_document_evidence_records',400,'Evidence records must be an array'); end if;
  for v_record in select value from jsonb_array_elements(p_records) loop
    insert into public.lcia_document_validation_evidence(dataset_type,dataset_id,dataset_version,canonical_content_hash,document_validator_version,document_validation_profile,validation_report_schema_version,validator_engine_fingerprint,tidas_schema_lock_sha256,status,summary,issue_artifact_ref,issue_artifact_hash,source_worker_job_id)
    values(nullif(v_record->>'datasetType',''),nullif(v_record->>'datasetId','')::uuid,nullif(v_record->>'datasetVersion',''),nullif(v_record->>'canonicalContentHash',''),nullif(v_record->>'documentValidatorVersion',''),nullif(v_record->>'documentValidationProfile',''),nullif(v_record->>'validationReportSchemaVersion',''),nullif(v_record->>'validatorEngineFingerprint',''),nullif(v_record->>'tidasSchemaLockSha256',''),nullif(v_record->>'status',''),coalesce(v_record->'summary','{}'::jsonb),coalesce(v_record->'issueArtifactRef','{}'::jsonb),nullif(v_record->>'issueArtifactHash',''),p_source_worker_job_id)
    on conflict (dataset_type,dataset_id,dataset_version,canonical_content_hash,document_validator_version,document_validation_profile,validation_report_schema_version,validator_engine_fingerprint,tidas_schema_lock_sha256) do nothing;
    if found then v_inserted:=v_inserted+1; end if;
  end loop;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('insertedCount',v_inserted));
exception when not_null_violation or invalid_text_representation or check_violation then return public.lcia_scope_closure_error('invalid_document_evidence_records',400,'Evidence record violates the cache contract');
end;
$$;

ALTER FUNCTION "public"."svc_lcia_document_validation_evidence_record"("p_records" "jsonb", "p_source_worker_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_document_validation_evidence_record"("p_records" "jsonb", "p_source_worker_job_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_document_validation_evidence_record"("p_records" "jsonb", "p_source_worker_job_id" "uuid") TO "service_role";
