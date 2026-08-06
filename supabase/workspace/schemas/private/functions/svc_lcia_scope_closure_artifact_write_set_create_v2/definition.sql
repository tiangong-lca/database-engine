CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid", "p_contract_version" "text", "p_expected_descriptor_count" integer, "p_descriptor_set_sha256" "text", "p_required_primary_roles" "jsonb", "p_staging_seconds" integer DEFAULT 900, "p_reused_from_check_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_check private.lcia_scope_closure_checks%rowtype;
  v_source_check private.lcia_scope_closure_checks%rowtype;
  v_job private.worker_jobs%rowtype;
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
  v_publication_mode text := case
    when p_reused_from_check_id is null then 'fresh'
    else 'reused'
  end;
  v_required_roles jsonb;
  v_lease_sha256 text;
  v_request_sha256 text;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;

  v_required_roles :=
    private.lcia_scope_closure_artifact_v2_required_roles(v_publication_mode);
  if p_closure_check_id is null
     or p_worker_job_id is null
     or p_worker_lease_token is null
     or p_request_id is null
     or p_contract_version is distinct from
       'lcia.scope-closure-artifact-write-set.v2'
     or p_expected_descriptor_count is null
     or p_expected_descriptor_count not between 1 and 100000
     or (
       v_publication_mode = 'fresh'
       and p_expected_descriptor_count < 3
     )
     or (
       v_publication_mode = 'reused'
       and p_expected_descriptor_count <> 1
     )
     or coalesce(p_descriptor_set_sha256, '') !~ '^[a-f0-9]{64}$'
     or p_required_primary_roles is distinct from v_required_roles
     or p_staging_seconds is null
     or p_staging_seconds not between 1 and 86400 then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact write-set header'
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

  select * into v_check
  from private.lcia_scope_closure_checks
  where id = p_closure_check_id
  for update;
  if v_check.id is null then
    return api.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if v_check.worker_job_id is distinct from v_job.id then
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is not bound to the closure check'
    );
  end if;
  if v_check.status in ('failed', 'cancelled') then
    return api.lcia_scope_closure_error(
      'artifact_write_set_unavailable',
      409,
      'Closure check cannot publish artifacts'
    );
  end if;

  if v_publication_mode = 'reused' then
    select * into v_source_check
    from private.lcia_scope_closure_checks
    where id = p_reused_from_check_id;
    if v_source_check.id is null
       or v_source_check.status not in ('passed', 'blocked')
       or v_source_check.scan_completeness <> 'complete'
       or v_source_check.requested_scope_hash <> v_check.requested_scope_hash
       or v_source_check.policy_fingerprint <> v_check.policy_fingerprint
       or v_source_check.data_snapshot_token <> v_check.data_snapshot_token
       or v_source_check.complete_machine_result_artifact_id is null
       or v_source_check.closure_bundle_artifact_id is null then
      return api.lcia_scope_closure_error(
        'artifact_write_set_reuse_invalid',
        409,
        'Reusable source evidence does not match the closure check'
      );
    end if;
  end if;

  v_lease_sha256 :=
    private.lcia_scope_closure_artifact_v2_lease_sha256(
      p_worker_lease_token
    );
  v_request_sha256 :=
    private.lcia_scope_closure_worker_canonical_sha256(jsonb_build_object(
      'closureCheckId', p_closure_check_id,
      'workerJobId', p_worker_job_id,
      'workerLeaseTokenSha256', v_lease_sha256,
      'requestId', p_request_id,
      'contractVersion', p_contract_version,
      'expectedDescriptorCount', p_expected_descriptor_count,
      'descriptorSetSha256', p_descriptor_set_sha256,
      'requiredPrimaryRoles', p_required_primary_roles,
      'stagingSeconds', p_staging_seconds,
      'publicationMode', v_publication_mode,
      'reusedFromCheckId', p_reused_from_check_id
    ));

  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where closure_check_id = p_closure_check_id
    and request_id = p_request_id;
  if v_write_set.id is not null then
    if v_write_set.request_sha256 is distinct from v_request_sha256
       or v_write_set.worker_job_id is distinct from p_worker_job_id
       or v_write_set.worker_lease_token_sha256 is distinct from
         v_lease_sha256 then
      return api.lcia_scope_closure_error(
        'artifact_write_set_v2_request_conflict',
        409,
        'Staged artifact write-set request identity conflicts'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data',
        private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
    );
  end if;

  if exists (
    select 1
    from private.lcia_scope_closure_artifact_write_sets active
    where active.closure_check_id = p_closure_check_id
      and active.status in ('registration_open', 'staging', 'ready')
  ) then
    return api.lcia_scope_closure_error(
      'artifact_write_set_unavailable',
      409,
      'Closure check already has an active artifact write-set'
    );
  end if;

  if v_publication_mode = 'reused' then
    update private.lcia_scope_closure_checks
    set reused_from_check_id = v_source_check.id,
        complete_machine_result_artifact_id =
          v_source_check.complete_machine_result_artifact_id,
        closure_bundle_artifact_id =
          v_source_check.closure_bundle_artifact_id,
        updated_at = now()
    where id = v_check.id;
  end if;

  insert into private.lcia_scope_closure_artifact_write_sets (
    closure_check_id,
    worker_job_id,
    requested_by,
    publication_mode,
    reused_from_check_id,
    idempotency_key,
    request_sha256,
    status,
    staging_expires_at,
    contract_version,
    request_id,
    expected_descriptor_count,
    descriptor_set_sha256,
    required_primary_roles,
    worker_lease_token_sha256
  ) values (
    v_check.id,
    v_job.id,
    v_check.requested_by,
    v_publication_mode,
    p_reused_from_check_id,
    'v2:' || p_request_id::text,
    v_request_sha256,
    'registration_open',
    least(
      now() + make_interval(secs => p_staging_seconds),
      v_job.lease_expires_at
    ),
    p_contract_version,
    p_request_id,
    p_expected_descriptor_count,
    p_descriptor_set_sha256,
    p_required_primary_roles,
    v_lease_sha256
  ) returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$_$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid", "p_contract_version" "text", "p_expected_descriptor_count" integer, "p_descriptor_set_sha256" "text", "p_required_primary_roles" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid", "p_contract_version" "text", "p_expected_descriptor_count" integer, "p_descriptor_set_sha256" "text", "p_required_primary_roles" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid", "p_contract_version" "text", "p_expected_descriptor_count" integer, "p_descriptor_set_sha256" "text", "p_required_primary_roles" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create_v2"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_request_id" "uuid", "p_contract_version" "text", "p_expected_descriptor_count" integer, "p_descriptor_set_sha256" "text", "p_required_primary_roles" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") TO "api_internal_executor";
