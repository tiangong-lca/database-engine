CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_check_record_result"("p_closure_check_id" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_hash" "text", "p_certificate_hash" "text" DEFAULT NULL::"text", "p_result_summary" "jsonb" DEFAULT '{}'::"jsonb", "p_blocker_codes" "text"[] DEFAULT '{}'::"text"[], "p_report_artifact_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare v_check private.lcia_scope_closure_checks%rowtype; v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if p_status not in ('passed', 'blocked', 'failed', 'cancelled')
    or p_scan_completeness not in ('complete', 'incomplete')
    or jsonb_typeof(coalesce(p_result_summary, '{}'::jsonb)) <> 'object' then
    return api.lcia_scope_closure_error('invalid_closure_result', 400, 'Invalid closure result payload');
  end if;
  select * into v_check from private.lcia_scope_closure_checks where id = p_closure_check_id for update;
  if v_check.id is null then return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found'); end if;
  if p_report_artifact_id is not null and not exists (
    select 1 from private.worker_job_artifacts a where a.id = p_report_artifact_id and a.job_id = v_check.worker_job_id
  ) then return api.lcia_scope_closure_error('closure_report_unavailable', 409, 'Report artifact does not belong to the closure job'); end if;
  if p_status = 'blocked' and p_report_artifact_id is null then
    return api.lcia_scope_closure_error('closure_report_unavailable', 409, 'Blocked closure checks require a report artifact');
  end if;
  if p_status = 'passed' and p_scan_completeness <> 'complete' then
    return api.lcia_scope_closure_error('closure_check_incomplete', 409, 'A passed closure check must be complete');
  end if;
  if p_status = 'passed' and (coalesce(nullif(trim(p_effective_scope_hash), ''), '') = '' or coalesce(nullif(trim(p_certificate_hash), ''), '') = '') then
    return api.lcia_scope_closure_error('closure_evidence_unavailable', 409, 'Passed closure checks require effective scope and certificate evidence');
  end if;
  if p_status = 'passed' and coalesce(p_result_summary->>'evidenceHash', '') <> coalesce(nullif(trim(p_certificate_hash), ''), '') then
    return api.lcia_scope_closure_error('closure_evidence_hash_mismatch', 409, 'Certificate hash must match the worker evidence hash');
  end if;
  update private.lcia_scope_closure_checks
  set status = p_status, scan_completeness = p_scan_completeness,
      effective_scope_hash = nullif(trim(p_effective_scope_hash), ''), certificate_status = case when p_status = 'passed' then 'valid' else 'unavailable' end,
      certificate_hash = nullif(trim(p_certificate_hash), ''), result_summary = coalesce(p_result_summary, '{}'::jsonb),
      blocker_codes = coalesce(p_blocker_codes, '{}'::text[]), report_artifact_id = p_report_artifact_id,
      updated_at = now(), finished_at = now()
  where id = v_check.id returning * into v_check;
  update private.worker_jobs set status = case when p_status = 'passed' then 'completed' else p_status end,
      result_json = jsonb_build_object('closureCheckId', v_check.id, 'status', p_status,
        'scanCompleteness', p_scan_completeness, 'certificateStatus', case when p_status = 'passed' then 'valid' else 'unavailable' end,
        'effectiveScopeHash', v_check.effective_scope_hash, 'certificateHash', v_check.certificate_hash),
      blocker_codes = case when p_status = 'blocked' then coalesce(p_blocker_codes, '{}'::text[]) else blocker_codes end,
      resolution_scope = case when p_status = 'blocked' then 'operator' else resolution_scope end,
      finished_at = now(), updated_at = now()
  where id = v_check.worker_job_id returning * into v_job;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object('closureCheckId', v_check.id, 'workerJob', private.worker_job_payload(v_job, true)));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_check_record_result"("p_closure_check_id" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_hash" "text", "p_certificate_hash" "text", "p_result_summary" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result"("p_closure_check_id" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_hash" "text", "p_certificate_hash" "text", "p_result_summary" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result"("p_closure_check_id" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_hash" "text", "p_certificate_hash" "text", "p_result_summary" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") TO "api_internal_executor";
