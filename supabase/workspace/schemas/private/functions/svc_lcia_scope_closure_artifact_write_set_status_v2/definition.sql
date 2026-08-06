CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_status_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
  v_lease_sha256 text;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_closure_check_id is null
     or p_worker_job_id is null
     or p_worker_lease_token is null
     or p_request_id is null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact write-set status request'
    );
  end if;

  select * into v_job
  from private.worker_jobs
  where id = p_worker_job_id;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_worker_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now() then
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is no longer valid'
    );
  end if;

  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where closure_check_id = p_closure_check_id
    and worker_job_id = p_worker_job_id
    and request_id = p_request_id
    and contract_version =
      'lcia.scope-closure-artifact-write-set.v2';
  if v_write_set.id is null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;

  v_lease_sha256 :=
    private.lcia_scope_closure_artifact_v2_lease_sha256(
      p_worker_lease_token
    );
  if v_write_set.worker_lease_token_sha256 is distinct from v_lease_sha256 then
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is not bound to the artifact write-set'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_status_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_status_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_status_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_status_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid") TO "api_internal_executor";
