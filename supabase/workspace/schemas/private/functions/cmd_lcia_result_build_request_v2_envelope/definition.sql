CREATE OR REPLACE FUNCTION "private"."cmd_lcia_result_build_request_v2_envelope"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare v_actor uuid := auth.uid(); v_check private.lcia_scope_closure_checks%rowtype; v_result jsonb; v_effective jsonb; v_payload jsonb;
begin
  if v_actor is null then return api.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  select * into v_check from private.lcia_scope_closure_checks where id=p_closure_check_id and requested_by=v_actor;
  if v_check.id is null then return api.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_check.certificate_status='stale' then return api.lcia_scope_closure_error('closure_check_stale',409,'Closure certificate is stale'); end if;
  if v_check.certificate_status='revoked' then return api.lcia_scope_closure_error('closure_check_revoked',409,'Closure certificate is revoked'); end if;
  if v_check.status<>'passed' or v_check.scan_completeness<>'complete' or v_check.certificate_status<>'valid' or v_check.certificate_hash is null then return api.lcia_scope_closure_error('closure_check_not_usable',409,'A valid complete closure certificate is required'); end if;
  if v_check.requested_scope_hash<>trim(coalesce(p_requested_scope_hash,'')) then return api.lcia_scope_closure_error('closure_check_scope_mismatch',409,'Requested scope does not match the closure certificate'); end if;
  if v_check.policy_fingerprint<>trim(coalesce(p_policy_fingerprint,'')) then return api.lcia_scope_closure_error('closure_check_policy_mismatch',409,'Policy does not match the closure certificate'); end if;
  v_effective:=coalesce(v_check.effective_scope_manifest,v_check.requested_scope_manifest);
  v_result:=private.cmd_lcia_result_build_request_legacy(p_name,v_effective->'processes','subset',p_default_impact_category,v_effective->'lciaMethods',p_idempotency_key,coalesce(p_audit,'{}'::jsonb)||jsonb_build_object('closureCheckId',v_check.id,'requestedScopeHash',v_check.requested_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'certificateHash',v_check.certificate_hash));
  v_payload:=(v_result->'data'->'workerJob'->'payload') || jsonb_build_object('coverage_mode',v_effective->>'coverageMode','input_manifest',jsonb_build_object('predicateVersion',v_effective->>'eligibilityPredicateVersion','selectionMode','closure_certificate','processes',v_effective->'processes'),'input_manifest_hash',private.lcia_scope_closure_sha256(jsonb_build_object('processes',v_effective->'processes')),'lcia_method_set',v_effective->'lciaMethods','closure_check_id',v_check.id,'closure_certificate_hash',v_check.certificate_hash,'effective_scope_hash',v_check.effective_scope_hash,'data_snapshot_token',v_check.data_snapshot_token,'snapshot_id',v_check.snapshot_id,'snapshot_hash',v_check.snapshot_hash,'closure_bundle_hash',v_check.closure_bundle_hash,'report_artifact_manifest_hash',v_check.report_artifact_manifest_hash);
  return jsonb_set(v_result,'{data,workerJob,payload}',v_payload,true) || jsonb_build_object('closureCheckId',v_check.id,'effectiveScopeHash',v_check.effective_scope_hash,'certificateHash',v_check.certificate_hash);
end;
$$;

ALTER FUNCTION "private"."cmd_lcia_result_build_request_v2_envelope"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_lcia_result_build_request_v2_envelope"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_lcia_result_build_request_v2_envelope"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") TO "api_internal_executor";
