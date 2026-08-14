CREATE OR REPLACE FUNCTION "private"."svc_lcia_document_validation_evidence_lookup"("p_cache_keys" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  if not coalesce(util.is_service_request(), false) then return api.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_cache_keys,'null'::jsonb)) <> 'array' then return api.lcia_scope_closure_error('invalid_document_evidence_keys',400,'Cache keys must be an array'); end if;
  return jsonb_build_object('ok',true,'data',coalesce((
    with requested as (
      select (value->>'datasetType') dataset_type, nullif(value->>'datasetId','')::uuid dataset_id,
        value->>'datasetVersion' dataset_version, value->>'canonicalContentHash' canonical_content_hash,
        value->>'documentValidatorVersion' document_validator_version,
        value->>'documentValidationProfile' document_validation_profile,
        value->>'validationReportSchemaVersion' validation_report_schema_version,
        value->>'validatorEngineFingerprint' validator_engine_fingerprint,
        value->>'tidasSchemaLockSha256' tidas_schema_lock_sha256
      from jsonb_array_elements(p_cache_keys)
    )
    select jsonb_agg(jsonb_build_object('datasetType',e.dataset_type,'datasetId',e.dataset_id,'datasetVersion',e.dataset_version,'canonicalContentHash',e.canonical_content_hash,'documentValidatorVersion',e.document_validator_version,'documentValidationProfile',e.document_validation_profile,'validationReportSchemaVersion',e.validation_report_schema_version,'validatorEngineFingerprint',e.validator_engine_fingerprint,'tidasSchemaLockSha256',e.tidas_schema_lock_sha256,'status',e.status,'summary',e.summary,'issueArtifactRef',e.issue_artifact_ref,'issueArtifactHash',e.issue_artifact_hash) order by e.dataset_type,e.dataset_id,e.dataset_version)
    from requested r join private.lcia_document_validation_evidence e on (e.dataset_type,e.dataset_id,e.dataset_version,e.canonical_content_hash,e.document_validator_version,e.document_validation_profile,e.validation_report_schema_version,e.validator_engine_fingerprint,e.tidas_schema_lock_sha256)=(r.dataset_type,r.dataset_id,r.dataset_version,r.canonical_content_hash,r.document_validator_version,r.document_validation_profile,r.validation_report_schema_version,r.validator_engine_fingerprint,r.tidas_schema_lock_sha256)
  ),'[]'::jsonb));
exception when invalid_text_representation then return api.lcia_scope_closure_error('invalid_document_evidence_keys',400,'Cache key contains invalid identity values');
end;
$$;

ALTER FUNCTION "private"."svc_lcia_document_validation_evidence_lookup"("p_cache_keys" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_document_validation_evidence_lookup"("p_cache_keys" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_document_validation_evidence_lookup"("p_cache_keys" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_document_validation_evidence_lookup"("p_cache_keys" "jsonb") TO "api_internal_executor";
