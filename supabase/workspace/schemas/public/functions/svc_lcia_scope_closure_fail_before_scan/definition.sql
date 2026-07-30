CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_fail_before_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_error_code" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare c public.lcia_scope_closure_checks%rowtype; j public.worker_jobs%rowtype; e public.lcia_scope_closure_scan_executions%rowtype; wr jsonb;
begin
  if not coalesce(util.is_service_request(),false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into c from public.lcia_scope_closure_checks where id=p_closure_check_id for update; select * into j from public.worker_jobs where id=p_worker_job_id for update;
  if c.id is null or c.worker_job_id<>j.id or c.status not in ('queued','running') or j.status<>'running' or j.lease_token is distinct from p_lease_token or j.lease_expires_at<now() then return public.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into e from public.lcia_scope_closure_scan_executions where id=c.scan_execution_id for update;
  update public.lcia_scope_closure_checks set status='failed',scan_completeness='unknown',certificate_status='unavailable',result_summary=jsonb_build_object('errorCode',coalesce(nullif(trim(p_error_code),''),'closure_check_failed')),updated_at=now(),finished_at=now() where id=c.id;
  if e.id is not null and e.status='running' and e.leased_by_job_id=j.id and e.lease_token is not distinct from p_lease_token then update public.lcia_scope_closure_scan_executions set status='failed',lease_token=null,leased_by_job_id=null,lease_expires_at=null,updated_at=now(),completed_at=now() where id=e.id; end if;
  select public.worker_record_job_result(j.id,p_lease_token,'failed',jsonb_build_object('closureCheckId',c.id,'status','failed'),'lcia.scope_closure_check.result.v1',null,null,coalesce(nullif(trim(p_error_code),''),'closure_check_failed'),'Scope closure check failed',null,null,null,true) into wr;
  if coalesce((wr->>'ok')::boolean,false) is not true then raise exception using errcode='P0001',message='worker_job_result_rejected'; end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',c.id,'workerJobId',j.id,'status','failed'));
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_fail_before_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_error_code" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_fail_before_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_error_code" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_fail_before_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_error_code" "text") TO "service_role";
