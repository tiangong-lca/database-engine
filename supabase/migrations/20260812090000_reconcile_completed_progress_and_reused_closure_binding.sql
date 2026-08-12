set check_function_bodies = off;

-- Issue #474: apply the production hotfix semantics to the post-cutover
-- private schema without changing either already-executed migration body.

CREATE OR REPLACE FUNCTION private.lcia_scope_closure_bundle_binding_matches(
  p_check private.lcia_scope_closure_checks,
  p_bundle private.worker_job_artifacts
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
AS $function$
  SELECT coalesce((
    (p_bundle).id = (p_check).closure_bundle_artifact_id
    AND (p_bundle).artifact_role = 'closure_bundle'
    AND (p_bundle).artifact_type = 'closure_bundle'
    AND (p_bundle).checksum_sha256 = (p_check).closure_bundle_hash
    AND (
      (
        (p_check).reused_from_check_id IS NULL
        AND (p_bundle).job_id = (p_check).worker_job_id
        AND coalesce((p_bundle).metadata->>'closureCheckId', '') = (p_check).id::text
      )
      OR EXISTS (
        SELECT 1
        FROM private.lcia_scope_closure_checks source
        WHERE source.id = (p_check).reused_from_check_id
          AND source.reused_from_check_id IS NULL
          AND source.status = 'passed'
          AND source.scan_completeness = 'complete'
          AND source.worker_job_id = (p_bundle).job_id
          AND source.closure_bundle_artifact_id = (p_bundle).id
          AND source.closure_bundle_hash = (p_check).closure_bundle_hash
          AND source.snapshot_id = (p_check).snapshot_id
          AND source.snapshot_hash = (p_check).snapshot_hash
          AND source.snapshot_artifact_id = (p_check).snapshot_artifact_id
          AND source.snapshot_index_sha256 = (p_check).snapshot_index_sha256
          AND source.snapshot_build_contract_hash = (p_check).snapshot_build_contract_hash
          AND source.effective_scope_hash = (p_check).effective_scope_hash
          AND source.requested_scope_hash = (p_check).requested_scope_hash
          AND source.policy_fingerprint = (p_check).policy_fingerprint
          AND source.data_snapshot_token = (p_check).data_snapshot_token
          AND coalesce((p_bundle).metadata->>'closureCheckId', '') = source.id::text
      )
    )
  ), false)
$function$;

ALTER FUNCTION private.lcia_scope_closure_bundle_binding_matches(
  private.lcia_scope_closure_checks,
  private.worker_job_artifacts
) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.lcia_scope_closure_bundle_binding_matches(
  private.lcia_scope_closure_checks,
  private.worker_job_artifacts
) FROM PUBLIC;

CREATE OR REPLACE FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_check private.lcia_scope_closure_checks%rowtype;
  v_snapshot private.lca_network_snapshots%rowtype;
  v_artifact private.lca_snapshot_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
  v_report private.worker_job_artifacts%rowtype;
  v_result jsonb;
  v_kind private.worker_job_kinds%rowtype;
  v_job private.worker_jobs%rowtype;
  v_build_id uuid;
  v_payload jsonb;
  v_input_manifest jsonb;
  v_report_manifest_hash text;
  v_idempotency_key text;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  select * into v_check
  from private.lcia_scope_closure_checks
  where id = p_closure_check_id and requested_by = v_actor
  for share;
  if v_check.id is null then
    return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_check.certificate_status = 'stale' then
    return api.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate is stale');
  end if;
  if v_check.certificate_status = 'revoked' then
    return api.lcia_scope_closure_error('closure_check_revoked', 409, 'Closure certificate is revoked');
  end if;
  if v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.certificate_hash is null then
    return api.lcia_scope_closure_error('closure_check_not_usable', 409, 'A valid complete numerical snapshot certificate is required');
  end if;
  if v_check.requested_scope_hash <> trim(coalesce(p_requested_scope_hash, '')) then
    return api.lcia_scope_closure_error('closure_check_scope_mismatch', 409, 'Requested scope does not match the closure certificate');
  end if;
  if v_check.policy_fingerprint <> trim(coalesce(p_policy_fingerprint, '')) then
    return api.lcia_scope_closure_error('closure_check_policy_mismatch', 409, 'Policy does not match the closure certificate');
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not private.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    return api.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate was created against an earlier public release');
  end if;

  select * into v_snapshot
  from private.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from private.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from private.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from private.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_report_manifest_hash := private.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or not private.lcia_scope_closure_bundle_binding_matches(v_check, v_bundle)
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash then
    return api.lcia_scope_closure_error('closure_snapshot_binding_invalid', 409, 'Numerical snapshot certificate binding is not ready');
  end if;

  v_result := private.cmd_lcia_result_build_request_v2_envelope(
    p_name, p_processes, p_coverage_mode, p_default_impact_category,
    p_lcia_method_set, p_idempotency_key, p_closure_check_id,
    p_requested_scope_hash, p_policy_fingerprint, p_audit
  );
  if coalesce((v_result->>'ok')::boolean, false) is not true then return v_result; end if;
  v_build_id := nullif(v_result->'data'->>'buildId', '')::uuid;
  select * into v_kind
  from private.worker_job_kinds
  where job_kind = 'lcia_result.package_build';
  if v_build_id is null or v_kind.job_kind is null then
    return api.lcia_scope_closure_error('build_enqueue_unavailable', 503, 'Build queue configuration is unavailable');
  end if;

  v_input_manifest := jsonb_build_object(
    'predicateVersion', v_check.effective_scope_manifest->>'eligibilityPredicateVersion',
    'selectionMode', 'closure_certificate',
    'processes', v_check.effective_scope_manifest->'processes'
  );
  v_payload := coalesce(v_result->'data'->'workerJob'->'payload', '{}'::jsonb)
    || jsonb_build_object(
      'coverage_mode', v_check.effective_scope_manifest->>'coverageMode',
      'input_manifest', v_input_manifest,
      'input_manifest_hash', private.lcia_scope_closure_sha256(v_input_manifest),
      'lcia_method_set', v_check.effective_scope_manifest->'lciaMethods',
      'closure_check_id', v_check.id,
      'closure_certificate_hash', v_check.certificate_hash,
      'requested_scope_hash', v_check.requested_scope_hash,
      'policy_fingerprint', v_check.policy_fingerprint,
      'effective_scope_hash', v_check.effective_scope_hash,
      'data_snapshot_token', v_check.data_snapshot_token,
      'snapshot_id', v_check.snapshot_id,
      'snapshot_hash', v_check.snapshot_hash,
      'closure_bundle_artifact_id', v_check.closure_bundle_artifact_id,
      'closure_bundle_hash', v_check.closure_bundle_hash,
      'report_artifact_manifest_hash', v_check.report_artifact_manifest_hash,
      'snapshot_artifact_id', v_check.snapshot_artifact_id,
      'snapshot_index_sha256', v_check.snapshot_index_sha256,
      'snapshot_build_contract_hash', v_check.snapshot_build_contract_hash,
      'effective_scope', v_check.effective_scope_manifest
    );
  v_idempotency_key := nullif(v_result->'data'->'workerJob'->>'idempotencyKey', '');
  select * into v_job
  from private.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and job_kind = v_kind.job_kind
    and requested_by = v_actor
    and idempotency_key is not distinct from v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc limit 1
  for update;
  if v_job.id is null then
    insert into private.worker_jobs(
      job_kind, worker_runtime, worker_queue, priority, queue_key,
      subject_type, subject_id, requester_type, requested_by,
      idempotency_key, request_hash, concurrency_key, visibility,
      max_attempts, payload_schema_version, payload_json, payload_ref,
      result_schema_version
    ) values (
      v_kind.job_kind, v_kind.worker_runtime, v_kind.worker_queue,
      v_kind.default_priority, nullif(v_result->'data'->'workerJob'->>'queueKey', ''),
      'lcia_result_build', v_build_id, 'operator', v_actor,
      v_idempotency_key, nullif(v_result->'data'->'workerJob'->>'requestHash', ''),
      nullif(v_result->'data'->'workerJob'->>'queueKey', ''), 'operator',
      v_kind.default_max_attempts, 'lcia_result.package_build.request.v2',
      v_payload,
      jsonb_build_object('closureCertificate', jsonb_build_object(
        'closureCheckId', v_check.id,
        'certificateHash', v_check.certificate_hash,
        'requestedScopeHash', v_check.requested_scope_hash,
        'policyFingerprint', v_check.policy_fingerprint,
        'effectiveScopeHash', v_check.effective_scope_hash,
        'dataSnapshotToken', v_check.data_snapshot_token,
        'snapshotId', v_check.snapshot_id,
        'snapshotHash', v_check.snapshot_hash,
        'closureBundleArtifactId', v_check.closure_bundle_artifact_id,
        'closureBundleHash', v_check.closure_bundle_hash,
        'reportArtifactManifestHash', v_check.report_artifact_manifest_hash,
        'snapshotArtifactId', v_check.snapshot_artifact_id,
        'snapshotIndexSha256', v_check.snapshot_index_sha256,
        'snapshotBuildContractHash', v_check.snapshot_build_contract_hash,
        'effectiveScope', v_check.effective_scope_manifest
      )),
      v_kind.result_schema_version
    ) returning * into v_job;
    insert into private.worker_job_events(job_id, event_type, status, details)
    values(v_job.id, 'enqueued', v_job.status, jsonb_build_object(
      'jobKind', v_job.job_kind,
      'closureCheckId', v_check.id,
      'certificateHash', v_check.certificate_hash,
      'snapshotId', v_check.snapshot_id
    ));
  elsif v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
        or v_job.payload_json <> v_payload then
    return api.lcia_scope_closure_error('build_enqueue_conflict', 409, 'Existing active build does not match the certificate-bound V2 payload');
  end if;
  return jsonb_set(
    jsonb_set(v_result, '{data,workerJob}', private.worker_job_payload(v_job, false), true),
    '{data,workerJobId}', to_jsonb(v_job.id), true
  );
exception
  when unique_violation then
    return api.lcia_scope_closure_error('build_enqueue_conflict', 409, 'A conflicting certificate-bound build is already active');
end;
$$;

ALTER FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."lcia_result_package_bind_closure_certificate"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_check private.lcia_scope_closure_checks%rowtype;
  v_snapshot private.lca_network_snapshots%rowtype;
  v_artifact private.lca_snapshot_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
  v_report private.worker_job_artifacts%rowtype;
  v_closure_check_id uuid;
  v_input_manifest jsonb;
  v_input_manifest_hash text;
  v_report_manifest_hash text;
begin
  select * into v_job
  from private.worker_jobs
  where id = new.build_worker_job_id
  for share;
  if v_job.payload_schema_version <> 'lcia_result.package_build.request.v2' then
    return new;
  end if;
  v_closure_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  select * into v_check
  from private.lcia_scope_closure_checks
  where id = v_closure_check_id
  for share;
  select * into v_snapshot
  from private.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from private.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from private.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from private.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_input_manifest := jsonb_build_object(
    'predicateVersion', v_check.effective_scope_manifest->>'eligibilityPredicateVersion',
    'selectionMode', 'closure_certificate',
    'processes', v_check.effective_scope_manifest->'processes'
  );
  v_input_manifest_hash := private.lcia_scope_closure_sha256(v_input_manifest);
  v_report_manifest_hash := private.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_check.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.status <> 'running'
     or v_job.lease_token is null
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now()
     or v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.requested_by <> v_job.requested_by
     or v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or not private.lcia_scope_closure_bundle_binding_matches(v_check, v_bundle)
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash
     or new.build_id <> v_job.subject_id
     or new.created_by <> v_job.requested_by
     or new.snapshot_id <> v_check.snapshot_id
     or new.coverage_mode <> v_check.effective_scope_manifest->>'coverageMode'
     or new.lcia_method_set <> coalesce(v_check.effective_scope_manifest->'lciaMethods', 'null'::jsonb)
     or new.input_manifest <> v_input_manifest
     or new.input_manifest_hash <> v_input_manifest_hash
     or coalesce(v_job.payload_json->>'closure_check_id', '') <> v_check.id::text
     or coalesce(v_job.payload_json->>'closure_certificate_hash', '') <> v_check.certificate_hash
     or coalesce(v_job.payload_json->>'requested_scope_hash', '') <> v_check.requested_scope_hash
     or coalesce(v_job.payload_json->>'policy_fingerprint', '') <> v_check.policy_fingerprint
     or coalesce(v_job.payload_json->>'effective_scope_hash', '') <> v_check.effective_scope_hash
     or coalesce(v_job.payload_json->>'data_snapshot_token', '') <> v_check.data_snapshot_token
     or coalesce(v_job.payload_json->>'snapshot_id', '') <> v_check.snapshot_id::text
     or coalesce(v_job.payload_json->>'snapshot_hash', '') <> v_check.snapshot_hash
     or coalesce(v_job.payload_json->>'closure_bundle_artifact_id', '') <> v_check.closure_bundle_artifact_id::text
     or coalesce(v_job.payload_json->>'closure_bundle_hash', '') <> v_check.closure_bundle_hash
     or coalesce(v_job.payload_json->>'report_artifact_manifest_hash', '') <> v_check.report_artifact_manifest_hash
     or coalesce(v_job.payload_json->>'snapshot_artifact_id', '') <> v_check.snapshot_artifact_id::text
     or coalesce(v_job.payload_json->>'snapshot_index_sha256', '') <> v_check.snapshot_index_sha256
     or coalesce(v_job.payload_json->>'snapshot_build_contract_hash', '') <> v_check.snapshot_build_contract_hash
     or coalesce(v_job.payload_json->'effective_scope', 'null'::jsonb) <> v_check.effective_scope_manifest
     or coalesce(v_job.payload_json->>'coverage_mode', '') <> v_check.effective_scope_manifest->>'coverageMode'
     or coalesce(v_job.payload_json->'lcia_method_set', 'null'::jsonb)
          <> coalesce(v_check.effective_scope_manifest->'lciaMethods', 'null'::jsonb)
     or coalesce(v_job.payload_json->'input_manifest', 'null'::jsonb) <> v_input_manifest
     or coalesce(v_job.payload_json->>'input_manifest_hash', '') <> v_input_manifest_hash then
    raise exception 'closure_certificate_binding_mismatch' using errcode = '23514';
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not private.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    raise exception 'closure_certificate_stale' using errcode = '23514';
  end if;
  new.closure_check_id := v_check.id;
  new.closure_certificate_hash := v_check.certificate_hash;
  new.closure_snapshot_hash := v_check.snapshot_hash;
  return new;
end;
$$;

ALTER FUNCTION "private"."lcia_result_package_bind_closure_certificate"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_result_package_bind_closure_certificate"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_result_package_bind_closure_certificate"() TO "service_role";

GRANT ALL ON FUNCTION "private"."lcia_result_package_bind_closure_certificate"() TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_report private.worker_job_artifacts%rowtype;
  v_machine_result private.worker_job_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
begin
  if new.certificate_status <> 'valid' then
    return new;
  end if;
  select * into v_report
  from private.worker_job_artifacts where id = new.report_artifact_id;
  select * into v_bundle
  from private.worker_job_artifacts where id = new.closure_bundle_artifact_id;
  select * into v_machine_result
  from private.worker_job_artifacts
  where id = coalesce(
    new.complete_machine_result_artifact_id,
    case
      when coalesce(
        v_bundle.metadata->>'completeMachineResultArtifactId', ''
      ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      then (v_bundle.metadata->>'completeMachineResultArtifactId')::uuid
    end
  );
  if v_report.id is null
     or v_report.job_id <> new.worker_job_id
     or v_report.artifact_role <> 'closure_report'
     or v_report.lifecycle_state <> 'ready'
     or v_machine_result.id is null
     or (
       v_machine_result.job_id <> new.worker_job_id
       and not exists (
         select 1
         from private.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_machine_result.job_id
       )
     )
     or v_machine_result.artifact_role <> 'complete_machine_result'
     or v_machine_result.lifecycle_state <> 'ready'
     or not private.lcia_scope_closure_bundle_binding_matches(new, v_bundle)
     or v_bundle.lifecycle_state <> 'ready'
     or v_report.expires_at is null
     or v_machine_result.expires_at is null
     or v_bundle.expires_at is null then
    raise exception 'closure_certificate_evidence_lifecycle_invalid'
      using errcode = '23514';
  end if;
  new.complete_machine_result_artifact_id := v_machine_result.id;
  new.valid_until := least(
    v_report.expires_at,
    v_machine_result.expires_at,
    v_bundle.expires_at
  );
  if new.valid_until <= coalesce(new.finished_at, now())
     or new.valid_until <= now() then
    raise exception 'closure_certificate_evidence_already_expired'
      using errcode = '23514';
  end if;
  return new;
end;
$_$;

ALTER FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_certificate_validity_guard"() TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select
    (p_check).status = 'passed'
    and (p_check).scan_completeness = 'complete'
    and (p_check).certificate_status = 'valid'
    and (p_check).valid_until > now()
    and exists (
      select 1
      from private.worker_job_artifacts report
      cross join private.worker_job_artifacts machine_result
      cross join private.worker_job_artifacts bundle
      where report.id = (p_check).report_artifact_id
        and report.job_id = (p_check).worker_job_id
        and report.artifact_role = 'closure_report'
        and report.lifecycle_state = 'ready'
        and report.expires_at > now()
        and report.storage_bucket is not null
        and report.storage_path is not null
        and report.checksum_sha256 is not null
        and machine_result.id =
          (p_check).complete_machine_result_artifact_id
        and (
          machine_result.job_id = (p_check).worker_job_id
          or exists (
            select 1
            from private.lcia_scope_closure_checks source
            where source.id = (p_check).reused_from_check_id
              and source.worker_job_id = machine_result.job_id
          )
        )
        and machine_result.artifact_role = 'complete_machine_result'
        and machine_result.lifecycle_state = 'ready'
        and machine_result.expires_at > now()
        and machine_result.storage_bucket is not null
        and machine_result.storage_path is not null
        and machine_result.checksum_sha256 is not null
        and private.lcia_scope_closure_bundle_binding_matches(p_check, bundle)
        and bundle.lifecycle_state = 'ready'
        and bundle.expires_at > now()
        and bundle.storage_bucket is not null
        and bundle.storage_path is not null
        and bundle.checksum_sha256 is not null
    )
$$;

ALTER FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_check private.lcia_scope_closure_checks%rowtype;
  v_snapshot private.lca_network_snapshots%rowtype;
  v_artifact private.lca_snapshot_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
  v_report private.worker_job_artifacts%rowtype;
  v_closure_check_id uuid;
  v_input_manifest jsonb;
  v_input_manifest_hash text;
  v_report_manifest_hash text;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  select * into v_job
  from private.worker_jobs
  where id = build_worker_job_id
  for update;
  if v_job.id is null or v_job.job_kind <> 'lcia_result.package_build' then
    return api.lcia_scope_closure_error('build_binding_not_found', 404, 'Build job not found');
  end if;
  if v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
     or v_job.status <> 'running'
     or v_job.lease_token is null
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now() then
    return api.lcia_scope_closure_error('build_worker_job_lease_invalid', 409, 'Build job does not hold a current running lease');
  end if;
  begin
    v_closure_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    return api.lcia_scope_closure_error('build_binding_invalid', 409, 'Build payload contains an invalid closure check identity');
  end;
  select * into v_check
  from private.lcia_scope_closure_checks
  where id = v_closure_check_id
  for share;
  if v_check.id is null
     or v_check.requested_by <> v_job.requested_by
     or v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.certificate_hash is null then
    return api.lcia_scope_closure_error('closure_check_not_usable', 409, 'Closure certificate is not valid for this build owner');
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not private.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    return api.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate was created against an earlier public release');
  end if;

  select * into v_snapshot
  from private.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from private.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from private.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from private.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_input_manifest := jsonb_build_object(
    'predicateVersion', v_check.effective_scope_manifest->>'eligibilityPredicateVersion',
    'selectionMode', 'closure_certificate',
    'processes', v_check.effective_scope_manifest->'processes'
  );
  v_input_manifest_hash := private.lcia_scope_closure_sha256(v_input_manifest);
  v_report_manifest_hash := private.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or not private.lcia_scope_closure_bundle_binding_matches(v_check, v_bundle)
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash then
    return api.lcia_scope_closure_error('closure_snapshot_binding_invalid', 409, 'Persisted closure snapshot evidence is no longer usable');
  end if;

  if coalesce(v_job.payload_json->>'closure_check_id', '') <> v_check.id::text
     or coalesce(v_job.payload_json->>'closure_certificate_hash', '') <> v_check.certificate_hash
     or coalesce(v_job.payload_json->>'requested_scope_hash', '') <> v_check.requested_scope_hash
     or coalesce(v_job.payload_json->>'policy_fingerprint', '') <> v_check.policy_fingerprint
     or coalesce(v_job.payload_json->>'effective_scope_hash', '') <> v_check.effective_scope_hash
     or coalesce(v_job.payload_json->>'data_snapshot_token', '') <> v_check.data_snapshot_token
     or coalesce(v_job.payload_json->>'snapshot_id', '') <> v_check.snapshot_id::text
     or coalesce(v_job.payload_json->>'snapshot_hash', '') <> v_check.snapshot_hash
     or coalesce(v_job.payload_json->>'closure_bundle_artifact_id', '') <> v_check.closure_bundle_artifact_id::text
     or coalesce(v_job.payload_json->>'closure_bundle_hash', '') <> v_check.closure_bundle_hash
     or coalesce(v_job.payload_json->>'report_artifact_manifest_hash', '') <> v_check.report_artifact_manifest_hash
     or coalesce(v_job.payload_json->>'snapshot_artifact_id', '') <> v_check.snapshot_artifact_id::text
     or coalesce(v_job.payload_json->>'snapshot_index_sha256', '') <> v_check.snapshot_index_sha256
     or coalesce(v_job.payload_json->>'snapshot_build_contract_hash', '') <> v_check.snapshot_build_contract_hash
     or coalesce(v_job.payload_json->'effective_scope', 'null'::jsonb) <> v_check.effective_scope_manifest
     or coalesce(v_job.payload_json->>'coverage_mode', '') <> v_check.effective_scope_manifest->>'coverageMode'
     or coalesce(v_job.payload_json->'lcia_method_set', 'null'::jsonb)
          <> coalesce(v_check.effective_scope_manifest->'lciaMethods', 'null'::jsonb)
     or coalesce(v_job.payload_json->'input_manifest', 'null'::jsonb) <> v_input_manifest
     or coalesce(v_job.payload_json->>'input_manifest_hash', '') <> v_input_manifest_hash then
    return api.lcia_scope_closure_error('build_binding_mismatch', 409, 'Build payload does not exactly match the database-owned certificate');
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id,
    'certificateHash', v_check.certificate_hash,
    'requestedScopeHash', v_check.requested_scope_hash,
    'policyFingerprint', v_check.policy_fingerprint,
    'effectiveScopeHash', v_check.effective_scope_hash,
    'dataSnapshotToken', v_check.data_snapshot_token,
    'snapshotId', v_check.snapshot_id,
    'snapshotHash', v_check.snapshot_hash,
    'closureBundleArtifactId', v_check.closure_bundle_artifact_id,
    'closureBundleHash', v_check.closure_bundle_hash,
    'snapshotArtifactId', v_check.snapshot_artifact_id,
    'snapshotIndexSha256', v_check.snapshot_index_sha256,
    'snapshotBuildContractHash', v_check.snapshot_build_contract_hash,
    'coverageMode', v_check.effective_scope_manifest->>'coverageMode',
    'lciaMethodSet', v_check.effective_scope_manifest->'lciaMethods',
    'inputManifest', v_input_manifest,
    'inputManifestHash', v_input_manifest_hash,
    'effectiveScope', v_check.effective_scope_manifest
  ));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") TO "api_internal_executor";

CREATE OR REPLACE FUNCTION "private"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb" DEFAULT NULL::"jsonb", "p_result_schema_version" "text" DEFAULT NULL::"text", "p_result_ref" "jsonb" DEFAULT NULL::"jsonb", "p_diagnostics" "jsonb" DEFAULT NULL::"jsonb", "p_error_code" "text" DEFAULT NULL::"text", "p_error_message" "text" DEFAULT NULL::"text", "p_error_details" "jsonb" DEFAULT NULL::"jsonb", "p_blocker_codes" "text"[] DEFAULT NULL::"text"[], "p_resolution_scope" "text" DEFAULT NULL::"text", "p_retryable" boolean DEFAULT NULL::boolean) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_resolution_scope text := lower(trim(coalesce(p_resolution_scope, '')));
  v_blocker_codes text[] := coalesce(p_blocker_codes, '{}'::text[]);
  v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to record worker job results'
    );
  end if;

  if v_status not in ('completed', 'blocked', 'failed', 'waiting') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT_STATUS',
      'status', 400,
      'message', 'status must be completed, blocked, failed, or waiting'
    );
  end if;

  if p_result_json is not null and jsonb_typeof(p_result_json) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT',
      'status', 400,
      'message', 'result must be a JSON object'
    );
  end if;

  if p_result_ref is not null and jsonb_typeof(p_result_ref) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT_REF',
      'status', 400,
      'message', 'resultRef must be a JSON object'
    );
  end if;

  if p_diagnostics is not null and jsonb_typeof(p_diagnostics) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_DIAGNOSTICS',
      'status', 400,
      'message', 'diagnostics must be a JSON object'
    );
  end if;

  if p_error_details is not null and jsonb_typeof(p_error_details) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_ERROR_DETAILS',
      'status', 400,
      'message', 'error details must be a JSON object'
    );
  end if;

  if v_status = 'blocked' and (cardinality(v_blocker_codes) = 0 or v_resolution_scope = '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_BLOCKER_DETAILS_REQUIRED',
      'status', 400,
      'message', 'blocked worker jobs require blockerCodes and resolutionScope'
    );
  end if;

  if v_status = 'blocked' and v_resolution_scope not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESOLUTION_SCOPE',
      'status', 400,
      'message', 'resolutionScope must be user, operator, or system'
    );
  end if;

  if v_status = 'failed' and nullif(trim(p_error_code), '') is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_ERROR_CODE_REQUIRED',
      'status', 400,
      'message', 'failed worker jobs require an errorCode'
    );
  end if;

  select *
    into v_job
  from private.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status <> 'running' then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RUNNING',
      'status', 409,
      'message', 'Worker job is not running'
    );
  end if;

  if v_job.lease_token is distinct from p_lease_token then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_TOKEN_MISMATCH',
      'status', 409,
      'message', 'Worker job lease token does not match'
    );
  end if;

  if v_job.lease_expires_at < now() then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_EXPIRED',
      'status', 409,
      'message', 'Worker job lease has expired'
    );
  end if;

  update private.worker_jobs
    set status = v_status,
        progress = case
          when v_status = 'completed' then 1
          else progress
        end,
        result_schema_version = coalesce(nullif(trim(p_result_schema_version), ''), result_schema_version),
        result_json = p_result_json,
        result_ref = p_result_ref,
        diagnostics = coalesce(p_diagnostics, '{}'::jsonb),
        error_code = nullif(trim(p_error_code), ''),
        error_message = nullif(trim(p_error_message), ''),
        error_details = p_error_details,
        blocker_codes = case
          when v_status = 'blocked' then v_blocker_codes
          else '{}'::text[]
        end,
        resolution_scope = case
          when v_status = 'blocked' then v_resolution_scope
          else null
        end,
        retryable = p_retryable,
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        heartbeat_at = coalesce(heartbeat_at, now()),
        updated_at = now(),
        finished_at = case
          when v_status in ('completed', 'blocked', 'failed') then now()
          else null
        end
  where id = v_job.id
  returning *
    into v_job;

  insert into private.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    worker_id,
    lease_token,
    message,
    details
  ) values (
    v_job.id,
    v_status,
    v_job.status,
    v_job.phase,
    v_job.progress,
    null,
    p_lease_token,
    coalesce(p_error_message, null),
    jsonb_strip_nulls(
      jsonb_build_object(
        'errorCode', v_job.error_code,
        'blockerCodes', to_jsonb(v_job.blocker_codes),
        'resolutionScope', v_job.resolution_scope,
        'retryable', v_job.retryable
      )
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', private.worker_job_payload(v_job, true)
  );
end;
$$;

ALTER FUNCTION "private"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) TO "service_role";

GRANT ALL ON FUNCTION "private"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) TO "api_internal_executor";

-- Preserve terminal timestamps and historical events; normalize only the
-- canonical job row to the completed-progress invariant.
UPDATE private.worker_jobs
SET progress = 1
WHERE status = 'completed'
  AND progress IS DISTINCT FROM 1;
