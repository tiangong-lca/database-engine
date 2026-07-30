CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2_legacy"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb" DEFAULT '{}'::"jsonb", "p_issues" "jsonb" DEFAULT '[]'::"jsonb", "p_blocker_codes" "text"[] DEFAULT '{}'::"text"[], "p_report_artifact_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare v_result jsonb; v_check public.lcia_scope_closure_checks%rowtype; v_execution public.lcia_scope_closure_scan_executions%rowtype; v_scan_key text; v_existing_execution uuid;
begin
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_check.scan_execution_id is not null then
    select * into v_execution from public.lcia_scope_closure_scan_executions where id=v_check.scan_execution_id for update;
    if v_execution.status<>'running' or v_execution.leased_by_job_id<>p_job_id or v_execution.lease_token is distinct from p_lease_token then return public.lcia_scope_closure_error('scan_execution_lease_invalid',409,'Scan execution is not held by this worker job'); end if;
  end if;
  v_result:=public.svc_lcia_scope_closure_check_record_result_v2_untracked(p_closure_check_id,p_job_id,p_lease_token,p_status,p_scan_completeness,p_effective_scope_manifest,p_evidence,p_result_summary,p_issues,p_blocker_codes,p_report_artifact_id);
  if coalesce((v_result->>'ok')::boolean,false) is not true then return v_result; end if;
  if v_execution.id is not null then
    select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
    v_scan_key:=public.lcia_scope_closure_sha256(jsonb_build_object('effectiveScopeHash',v_check.effective_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'validatorScannerFingerprint',v_check.expected_validator_scanner_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token));
    select id into v_existing_execution from public.lcia_scope_closure_scan_executions where scan_key=v_scan_key and id<>v_execution.id limit 1;
    if v_existing_execution is not null then v_scan_key:=null; end if;
    update public.lcia_scope_closure_scan_executions set scan_key=v_scan_key,status=case when lower(trim(p_status))='passed' or (lower(trim(p_status))='blocked' and p_scan_completeness='complete') then 'completed' else 'failed' end,lease_token=null,leased_by_job_id=null,lease_expires_at=null,completed_check_id=case when lower(trim(p_status))='passed' or (lower(trim(p_status))='blocked' and p_scan_completeness='complete') then p_closure_check_id else null end,source_fingerprint=nullif(p_evidence->>'sourceFingerprint',''),evidence_hash=nullif(p_evidence->>'evidenceHash',''),updated_at=now(),completed_at=now() where id=v_execution.id;
  end if;
  return v_result;
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2_legacy"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2_legacy"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") FROM PUBLIC;
