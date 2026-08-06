CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_claim_scan_execution"("p_scan_execution_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare v_execution private.lcia_scope_closure_scan_executions%rowtype; v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then return api.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_job from private.worker_jobs where id=p_worker_job_id;
  if v_job.id is null or v_job.status<>'running' or v_job.lease_token is distinct from p_lease_token or v_job.lease_expires_at<now() then return api.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into v_execution from private.lcia_scope_closure_scan_executions where id=p_scan_execution_id for update;
  if v_execution.id is null or not exists (select 1 from private.lcia_scope_closure_checks c where c.scan_execution_id=v_execution.id and c.worker_job_id=v_job.id and c.request_fingerprint=v_execution.request_fingerprint) then return api.lcia_scope_closure_error('scan_execution_not_found',404,'Scan execution not found'); end if;
  if v_execution.status='completed' then return jsonb_build_object('ok',true,'data',jsonb_build_object('acquired',false,'completed',true,'completedCheckId',v_execution.completed_check_id)); end if;
  if v_execution.status='running' and v_execution.lease_expires_at>=now() and v_execution.leased_by_job_id<>v_job.id then return jsonb_build_object('ok',true,'data',jsonb_build_object('acquired',false,'completed',false)); end if;
  update private.lcia_scope_closure_scan_executions set status='running',lease_token=p_lease_token,leased_by_job_id=v_job.id,lease_expires_at=v_job.lease_expires_at,updated_at=now() where id=v_execution.id;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('acquired',true,'completed',false));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_claim_scan_execution"("p_scan_execution_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_claim_scan_execution"("p_scan_execution_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_claim_scan_execution"("p_scan_execution_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_claim_scan_execution"("p_scan_execution_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid") TO "api_internal_executor";
