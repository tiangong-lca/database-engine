CREATE OR REPLACE FUNCTION "public"."svc_lcia_result_build_bind_closure"("p_worker_job_id" "uuid", "p_closure_check_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare v_job public.worker_jobs%rowtype; v_check public.lcia_scope_closure_checks%rowtype; v_binding jsonb;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_job from public.worker_jobs where id=p_worker_job_id for update;
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  if v_job.id is null or v_job.job_kind<>'lcia_result.package_build' or v_check.id is null then return public.lcia_scope_closure_error('build_binding_not_found',404,'Build job or closure certificate not found'); end if;
  if v_check.status<>'passed' or v_check.scan_completeness<>'complete' or v_check.certificate_status<>'valid' then return public.lcia_scope_closure_error('closure_check_not_usable',409,'Closure certificate is not usable'); end if;
  v_binding:=jsonb_strip_nulls(jsonb_build_object('closure_check_id',v_check.id,'closure_certificate_hash',v_check.certificate_hash,'effective_scope_hash',v_check.effective_scope_hash,'data_snapshot_token',v_check.data_snapshot_token,'snapshot_id',v_check.snapshot_id,'snapshot_hash',v_check.snapshot_hash,'closure_bundle_hash',v_check.closure_bundle_hash,'report_artifact_manifest_hash',v_check.report_artifact_manifest_hash));
  update public.worker_jobs set payload_json=payload_json||v_binding,payload_ref=coalesce(payload_ref,'{}'::jsonb)||jsonb_build_object('closureCertificate',v_binding),updated_at=now() where id=v_job.id returning * into v_job;
  insert into public.worker_job_events(job_id,event_type,status,details) values(v_job.id,'closure_certificate_bound',v_job.status,jsonb_build_object('closureCheckId',v_check.id,'certificateHash',v_check.certificate_hash));
  return jsonb_build_object('ok',true,'data',jsonb_build_object('workerJobId',v_job.id,'closureCheckId',v_check.id));
end;
$$;

ALTER FUNCTION "public"."svc_lcia_result_build_bind_closure"("p_worker_job_id" "uuid", "p_closure_check_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_result_build_bind_closure"("p_worker_job_id" "uuid", "p_closure_check_id" "uuid") FROM PUBLIC;
