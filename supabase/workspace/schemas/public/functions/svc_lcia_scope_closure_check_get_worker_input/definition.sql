CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_check_get_worker_input"("p_closure_check_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_check public.lcia_scope_closure_checks%rowtype;
  v_data_snapshot public.lcia_scope_closure_data_snapshots%rowtype;
  v_execution public.lcia_scope_closure_scan_executions%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id;
  select * into v_data_snapshot
  from public.lcia_scope_closure_data_snapshots
  where data_snapshot_token = v_check.data_snapshot_token;
  select * into v_execution
  from public.lcia_scope_closure_scan_executions
  where id = v_check.scan_execution_id;
  if v_check.id is null
     or v_check.requested_scope_manifest is null
     or v_data_snapshot.data_snapshot_token is null
     or v_execution.id is null
     or v_execution.numerical_snapshot_id is null then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check or preallocated numerical snapshot not found');
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id,
    'scanExecutionId', v_check.scan_execution_id,
    'numericalSnapshotId', v_execution.numerical_snapshot_id,
    'requestedScope', v_check.requested_scope_manifest,
    'requestedScopeHash', v_check.requested_scope_hash,
    'policyFingerprint', v_check.policy_fingerprint,
    'dataSnapshotToken', v_check.data_snapshot_token,
    'dataSnapshotManifest', v_data_snapshot.root_manifest,
    'dataSnapshotManifestHash', v_data_snapshot.root_manifest_hash,
    'publicationEpoch', v_data_snapshot.publication_epoch,
    'expectedValidatorScannerFingerprint', v_check.expected_validator_scanner_fingerprint,
    'requestFingerprint', v_check.request_fingerprint
  ));
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_check_get_worker_input"("p_closure_check_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_check_get_worker_input"("p_closure_check_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_check_get_worker_input"("p_closure_check_id" "uuid") TO "service_role";
