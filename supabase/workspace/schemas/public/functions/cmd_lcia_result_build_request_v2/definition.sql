CREATE OR REPLACE FUNCTION "public"."cmd_lcia_result_build_request_v2"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if v_check.valid_until <= now()
     or exists (
       select 1
       from public.worker_job_artifacts artifact
       where artifact.id in (
         v_check.report_artifact_id,
         v_check.complete_machine_result_artifact_id,
         v_check.closure_bundle_artifact_id
       )
         and (
           artifact.lifecycle_state in ('expired', 'deleted')
           or artifact.expires_at <= now()
         )
     ) then
    return public.lcia_scope_closure_error(
      'closure_certificate_expired', 410, 'Closure certificate evidence has expired'
    );
  end if;
  if not public.lcia_scope_closure_evidence_usable(v_check) then
    return public.lcia_scope_closure_error(
      'closure_check_not_usable', 409, 'Closure certificate evidence is not usable'
    );
  end if;

  return public.cmd_lcia_result_build_request_v2_without_expiry(
    p_name,
    p_processes,
    p_coverage_mode,
    p_default_impact_category,
    p_lcia_method_set,
    p_idempotency_key,
    p_closure_check_id,
    p_requested_scope_hash,
    p_policy_fingerprint,
    p_audit
  );
end;
$$;

ALTER FUNCTION "public"."cmd_lcia_result_build_request_v2"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_lcia_result_build_request_v2"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_lcia_result_build_request_v2"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") TO "authenticated";
