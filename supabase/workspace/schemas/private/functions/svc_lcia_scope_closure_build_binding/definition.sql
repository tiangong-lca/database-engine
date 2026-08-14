CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_build_binding"("build_worker_job_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_check private.lcia_scope_closure_checks%rowtype;
  v_check_id uuid;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select * into v_job
  from private.worker_jobs
  where id = build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build' then
    return api.lcia_scope_closure_error(
      'build_binding_not_found', 404, 'Build job not found'
    );
  end if;
  begin
    v_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    return api.lcia_scope_closure_error(
      'build_binding_invalid', 409, 'Build payload contains an invalid closure check identity'
    );
  end;
  select * into v_check
  from private.lcia_scope_closure_checks
  where id = v_check_id
    and requested_by = v_job.requested_by;
  if v_check.id is null then
    return api.lcia_scope_closure_error(
      'closure_check_not_usable', 409, 'Closure certificate is not valid for this build owner'
    );
  end if;
  if v_check.valid_until <= now()
     or not private.lcia_scope_closure_evidence_usable(v_check) then
    return api.lcia_scope_closure_error(
      'closure_certificate_expired', 410, 'Closure certificate evidence has expired'
    );
  end if;
  return private.svc_lcia_scope_closure_build_binding_without_expiry(
    build_worker_job_id
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_build_binding"("build_worker_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_build_binding"("build_worker_job_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_build_binding"("build_worker_job_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_build_binding"("build_worker_job_id" "uuid") TO "api_internal_executor";
