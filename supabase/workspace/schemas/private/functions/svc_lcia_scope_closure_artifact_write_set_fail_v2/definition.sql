CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_fail_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_error" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_failure_invalid', 400, 'Failure reason is required'
    );
  end if;

  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null
     or v_write_set.contract_version is distinct from
       'lcia.scope-closure-artifact-write-set.v2' then
    return api.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_write_set.write_token is distinct from p_write_token then
    return api.lcia_scope_closure_error(
      'artifact_write_set_token_invalid',
      409,
      'Artifact write-set token is not current'
    );
  end if;
  if v_write_set.worker_job_id is distinct from p_worker_job_id then
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job is not bound to the artifact write-set'
    );
  end if;

  select * into v_job
  from private.worker_jobs
  where id = p_worker_job_id;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_worker_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now()
     or v_write_set.worker_lease_token_sha256 is distinct from
       private.lcia_scope_closure_artifact_v2_lease_sha256(
         p_worker_lease_token
       ) then
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is no longer valid'
    );
  end if;

  if v_write_set.status = 'cleanup_pending' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data',
        private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
    );
  end if;
  if v_write_set.status not in ('registration_open', 'staging') then
    return api.lcia_scope_closure_error(
      'artifact_write_set_not_finalizable',
      409,
      'Artifact write-set cannot be failed from its current state'
    );
  end if;

  update private.lcia_scope_closure_artifact_write_sets
  set status = 'cleanup_pending',
      failure_reason = left(trim(p_error), 1000),
      updated_at = now()
  where id = v_write_set.id
  returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_fail_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_error" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_fail_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_error" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_fail_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_error" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_fail_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_error" "text") TO "api_internal_executor";
