CREATE OR REPLACE FUNCTION "public"."cmd_lcia_scope_closure_check_request"("p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_request_idempotency_token" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_request_key text; v_current_manifest jsonb; v_data_snapshot_token text; v_expected_validator_scanner_fingerprint text;
  v_request_fingerprint text;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if coalesce(nullif(trim(p_requested_scope_hash), ''), '') = ''
    or coalesce(nullif(trim(p_policy_fingerprint), ''), '') = ''
    or coalesce(nullif(trim(p_request_idempotency_token), ''), '') = '' then
    return public.lcia_scope_closure_error('invalid_closure_request', 400, 'Closure scope, policy, and idempotency token are required');
  end if;
  select expected_validator_scanner_fingerprint into v_expected_validator_scanner_fingerprint
  from public.lcia_scope_closure_config where singleton;
  if v_expected_validator_scanner_fingerprint is null then
    return public.lcia_scope_closure_error('closure_evidence_unavailable', 503, 'Closure validator configuration is unavailable');
  end if;
  v_current_manifest := public.lcia_result_current_eligible_manifest();
  v_data_snapshot_token := v_current_manifest->>'inputManifestHash';

  v_request_fingerprint := encode(digest(
    trim(p_requested_scope_hash) || '|' || trim(p_policy_fingerprint) || '|' ||
    v_expected_validator_scanner_fingerprint || '|' || v_data_snapshot_token, 'sha256'), 'hex');
  v_request_key := encode(digest(v_actor::text || '|' || trim(p_request_idempotency_token) || '|' || v_request_fingerprint, 'sha256'), 'hex');

  select * into v_check
  from public.lcia_scope_closure_checks
  where requested_by = v_actor and request_key = v_request_key
  for update;
  if v_check.id is not null then
    select * into v_job from public.worker_jobs where id = v_check.worker_job_id;
    return jsonb_build_object('ok', true, 'data', jsonb_build_object(
      'closureCheckId', v_check.id, 'workerJob', public.worker_job_payload(v_job, false),
      'requestFingerprint', v_request_fingerprint, 'reused', true));
  end if;

  insert into public.worker_jobs (
    job_kind, worker_runtime, worker_queue, priority, subject_type, requester_type,
    requested_by, idempotency_key, request_hash, concurrency_key, visibility,
    max_attempts, payload_schema_version, payload_json, result_schema_version
  ) values (
    'lcia.scope_closure_check', 'calculator', 'solver', 10, 'lcia_scope_closure_check', 'user',
    v_actor, trim(p_request_idempotency_token), v_request_fingerprint, v_request_key, 'operator',
    3, 'lcia.scope_closure_check.request.v1', jsonb_build_object(
      'closure_check_id', null, 'requested_by', v_actor, 'requested_scope_hash', trim(p_requested_scope_hash),
      'policy_fingerprint', trim(p_policy_fingerprint), 'data_snapshot_token', v_data_snapshot_token,
      'expected_validator_scanner_fingerprint', v_expected_validator_scanner_fingerprint,
      'request_fingerprint', v_request_fingerprint, 'request_key', v_request_key),
    'lcia.scope_closure_check.result.v1'
  ) returning * into v_job;

  insert into public.lcia_scope_closure_checks (
    worker_job_id, requested_by, request_idempotency_token, request_key, request_fingerprint,
    requested_scope_hash, policy_fingerprint, data_snapshot_token, expected_validator_scanner_fingerprint
  ) values (
    v_job.id, v_actor, trim(p_request_idempotency_token), v_request_key, v_request_fingerprint,
    trim(p_requested_scope_hash), trim(p_policy_fingerprint), v_data_snapshot_token, v_expected_validator_scanner_fingerprint
  ) returning * into v_check;

  update public.worker_jobs
  set subject_id = v_check.id,
      payload_json = payload_json || jsonb_build_object('closure_check_id', v_check.id),
      updated_at = now()
  where id = v_job.id
  returning * into v_job;

  insert into public.worker_job_events (job_id, event_type, status, details)
  values (v_job.id, 'enqueued', 'queued', jsonb_build_object('closureCheckId', v_check.id, 'requestFingerprint', v_request_fingerprint));

  insert into public.command_audit_log (command, actor_user_id, target_table, target_id, payload)
  values ('cmd_lcia_scope_closure_check_request', v_actor, 'lcia_scope_closure_checks', v_check.id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('requestFingerprint', v_request_fingerprint));

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id, 'workerJob', public.worker_job_payload(v_job, false),
    'requestFingerprint', v_request_fingerprint, 'reused', false));
end;
$$;

ALTER FUNCTION "public"."cmd_lcia_scope_closure_check_request"("p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_request_idempotency_token" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_lcia_scope_closure_check_request"("p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_request_idempotency_token" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_lcia_scope_closure_check_request"("p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_request_idempotency_token" "text", "p_audit" "jsonb") TO "service_role";
