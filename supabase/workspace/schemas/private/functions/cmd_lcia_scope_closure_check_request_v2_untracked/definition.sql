CREATE OR REPLACE FUNCTION "private"."cmd_lcia_scope_closure_check_request_v2_untracked"("p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid(); v_scope jsonb; v_policy jsonb; v_requested_scope_hash text;
  v_policy_fingerprint text; v_data_snapshot_token text; v_expected_validator_scanner_fingerprint text;
  v_request_fingerprint text; v_request_key text; v_check private.lcia_scope_closure_checks%rowtype; v_job private.worker_jobs%rowtype;
begin
  if v_actor is null then return api.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  if not api.lcia_scope_closure_is_manager() then return api.lcia_scope_closure_error('not_data_product_manager', 403, 'Data product manager role is required'); end if;
  if coalesce(nullif(trim(p_request_idempotency_token), ''), '') = '' then return api.lcia_scope_closure_error('invalid_closure_request', 400, 'Idempotency token is required'); end if;
  v_scope := private.lcia_scope_closure_normalize_request(p_requested_scope);
  v_requested_scope_hash := private.lcia_scope_closure_sha256(v_scope);
  v_policy := jsonb_build_object('scopePolicy', v_scope - 'processes' - 'lciaMethods' - 'processManifestHash', 'visibilityScope', 'data_product_manager.v1');
  v_policy_fingerprint := private.lcia_scope_closure_sha256(v_policy);
  v_data_snapshot_token := private.lcia_scope_closure_sha256(jsonb_build_object('processes', v_scope->'processes', 'lciaMethods', v_scope->'lciaMethods'));
  select expected_validator_scanner_fingerprint into v_expected_validator_scanner_fingerprint from private.lcia_scope_closure_config where singleton;
  if v_expected_validator_scanner_fingerprint is null then return api.lcia_scope_closure_error('closure_evidence_unavailable', 503, 'Closure validator configuration is unavailable'); end if;
  v_request_fingerprint := encode(extensions.digest(v_requested_scope_hash || '|' || v_policy_fingerprint || '|' || v_expected_validator_scanner_fingerprint || '|' || v_data_snapshot_token, 'sha256'), 'hex');
  v_request_key := encode(extensions.digest(v_actor::text || '|' || trim(p_request_idempotency_token) || '|' || v_request_fingerprint, 'sha256'), 'hex');
  select * into v_check from private.lcia_scope_closure_checks where requested_by = v_actor and request_key = v_request_key for update;
  if v_check.id is not null then
    select * into v_job from private.worker_jobs where id = v_check.worker_job_id;
    return jsonb_build_object('ok', true, 'data', jsonb_build_object('closureCheckId', v_check.id, 'requestedScopeHash', v_check.requested_scope_hash, 'policyFingerprint', v_check.policy_fingerprint, 'workerJob', private.worker_job_payload(v_job, false), 'reused', true));
  end if;
  insert into private.worker_jobs (job_kind, worker_runtime, worker_queue, priority, subject_type, requester_type, requested_by, idempotency_key, request_hash, concurrency_key, visibility, max_attempts, payload_schema_version, payload_json, result_schema_version)
  values ('lcia.scope_closure_check','calculator','solver',10,'lcia_scope_closure_check','user',v_actor,trim(p_request_idempotency_token),v_request_fingerprint,v_request_key,'operator',3,'lcia.scope_closure_check.request.v1',jsonb_build_object('closure_check_id', null, 'request_fingerprint', v_request_fingerprint),'lcia.scope_closure_check.result.v1') returning * into v_job;
  insert into private.lcia_scope_closure_checks (worker_job_id,requested_by,request_idempotency_token,request_key,request_fingerprint,requested_scope_hash,requested_scope_manifest,policy_fingerprint,data_snapshot_token,expected_validator_scanner_fingerprint)
  values (v_job.id,v_actor,trim(p_request_idempotency_token),v_request_key,v_request_fingerprint,v_requested_scope_hash,v_scope,v_policy_fingerprint,v_data_snapshot_token,v_expected_validator_scanner_fingerprint) returning * into v_check;
  update private.worker_jobs set subject_id = v_check.id, payload_json = payload_json || jsonb_build_object('closure_check_id', v_check.id), updated_at = now() where id = v_job.id returning * into v_job;
  insert into private.worker_job_events(job_id,event_type,status,details) values(v_job.id,'enqueued','queued',jsonb_build_object('closureCheckId',v_check.id,'requestFingerprint',v_request_fingerprint));
  insert into private.command_audit_log(command,actor_user_id,target_table,target_id,payload) values ('cmd_lcia_scope_closure_check_request_v2',v_actor,'lcia_scope_closure_checks',v_check.id,coalesce(p_audit,'{}'::jsonb)||jsonb_build_object('requestFingerprint',v_request_fingerprint,'requestedScopeHash',v_requested_scope_hash,'policyFingerprint',v_policy_fingerprint));
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'requestedScopeHash',v_requested_scope_hash,'policyFingerprint',v_policy_fingerprint,'workerJob',private.worker_job_payload(v_job,false),'reused',false));
exception when sqlstate '22023' then
  return api.lcia_scope_closure_error('invalid_closure_scope',400,sqlerrm);
end;
$$;

ALTER FUNCTION "private"."cmd_lcia_scope_closure_check_request_v2_untracked"("p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_lcia_scope_closure_check_request_v2_untracked"("p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_lcia_scope_closure_check_request_v2_untracked"("p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") TO "api_internal_executor";
