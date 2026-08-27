CREATE OR REPLACE FUNCTION "api"."cmd_lcia_result_build_request_v3"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_idempotency_key text;
  v_request jsonb;
  v_result jsonb;
  v_job private.worker_jobs%rowtype;
  v_job_id uuid;
  v_actual_idempotency_key text;
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if private.portal_lcia_safe_audit_v1(p_audit) is not true
     or nullif(btrim(coalesce(p_idempotency_key, '')), '') is null
     or length(btrim(p_idempotency_key)) > 220 then
    return api.lcia_result_error(
      'invalid_projection_request', 400, 'Invalid Portal LCIA V3 request'
    );
  end if;
  v_idempotency_key := 'portal-lcia-v3:' || btrim(p_idempotency_key);
  v_request := jsonb_build_object(
    'name', p_name,
    'processes', p_processes,
    'coverageMode', p_coverage_mode,
    'defaultImpactCategory', p_default_impact_category,
    'lciaMethodSet', p_lcia_method_set,
    'closureCheckId', p_closure_check_id,
    'requestedScopeHash', p_requested_scope_hash,
    'policyFingerprint', p_policy_fingerprint
  );

  select job.* into v_job
  from private.worker_jobs as job
  where job.job_kind = 'lcia_result.package_build'
    and job.requested_by = v_actor
    and job.payload_schema_version = 'lcia_result.package_build.request.v3'
    and job.payload_json ->> 'portalProjectionIdempotencyKey'
          = v_idempotency_key
  order by job.created_at desc, job.id
  limit 1
  for update;
  if v_job.id is not null then
    if v_job.payload_json -> 'portalProjectionRequest' is distinct from v_request
       or v_job.payload_json ->> 'portalProjectionContractVersion'
            <> 'portal.lcia-projection.v1' then
      return api.lcia_result_error(
        'build_enqueue_conflict', 409,
        'Existing V3 build is bound to different content'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', jsonb_build_object(
        'buildId', v_job.subject_id,
        'workerJobId', v_job.id,
        'workerJob', private.worker_job_payload(v_job, false),
        'projectionContractVersion', 'portal.lcia-projection.v1'
      )
    );
  end if;

  v_result := api.cmd_lcia_result_build_request_v2(
    p_name,
    p_processes,
    p_coverage_mode,
    p_default_impact_category,
    p_lcia_method_set,
    v_idempotency_key,
    p_closure_check_id,
    p_requested_scope_hash,
    p_policy_fingerprint,
    p_audit
  );
  if coalesce((v_result ->> 'ok')::boolean, false) is not true then
    return v_result;
  end if;
  begin
    v_job_id := nullif(v_result -> 'data' ->> 'workerJobId', '')::uuid;
    v_actual_idempotency_key := nullif(
      v_result -> 'data' -> 'workerJob' ->> 'idempotencyKey', ''
    );
  exception when invalid_text_representation then
    return api.lcia_result_error(
      'build_enqueue_unavailable', 503,
      'V2 admission did not return a valid Worker job identity'
    );
  end;

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_job_id
  for update;
  if v_job.payload_schema_version = 'lcia_result.package_build.request.v3' then
    if v_job.requested_by = v_actor
       and v_job.idempotency_key is not distinct from v_actual_idempotency_key
       and v_job.payload_json ->> 'portalProjectionIdempotencyKey'
            = v_idempotency_key
       and v_job.payload_json -> 'portalProjectionRequest' = v_request
       and v_job.payload_json ->> 'portalProjectionContractVersion'
            = 'portal.lcia-projection.v1' then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'buildId', v_job.subject_id,
          'workerJobId', v_job.id,
          'workerJob', private.worker_job_payload(v_job, false),
          'projectionContractVersion', 'portal.lcia-projection.v1'
        )
      );
    end if;
    return api.lcia_result_error(
      'build_enqueue_conflict', 409,
      'Existing V3 build is bound to different content'
    );
  end if;
  if v_job.id is null
     or v_job.requested_by <> v_actor
     or v_actual_idempotency_key is null
     or v_job.idempotency_key is distinct from v_actual_idempotency_key
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
     or v_job.status not in ('queued', 'running', 'waiting', 'stale', 'blocked') then
    return api.lcia_result_error(
      'build_enqueue_conflict', 409,
      'V2 admission did not create the reserved convertible Worker job'
    );
  end if;

  update private.worker_jobs
  set payload_schema_version = 'lcia_result.package_build.request.v3',
      payload_json = payload_json || jsonb_build_object(
        'portalProjectionContractVersion', 'portal.lcia-projection.v1',
        'portalProjectionHashContractVersion',
          'portal.lcia-projection.int32be-frame-sha256.v1',
        'portalProjectionIdempotencyKey', v_idempotency_key,
        'portalProjectionRequest', v_request
      ),
      updated_at = clock_timestamp()
  where id = v_job.id
  returning * into v_job;

  insert into private.worker_job_events (
    job_id, event_type, status, details
  ) values (
    v_job.id,
    'portal_projection_v3_admitted',
    v_job.status,
    jsonb_build_object(
      'projectionContractVersion', 'portal.lcia-projection.v1',
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1'
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'buildId', v_job.subject_id,
      'workerJobId', v_job.id,
      'workerJob', private.worker_job_payload(v_job, false),
      'projectionContractVersion', 'portal.lcia-projection.v1'
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error(
      'build_enqueue_conflict', 409,
      'A conflicting Portal LCIA V3 build already exists'
    );
end
$$;

ALTER FUNCTION "api"."cmd_lcia_result_build_request_v3"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lcia_result_build_request_v3"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_build_request_v3"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") TO "authenticated";
