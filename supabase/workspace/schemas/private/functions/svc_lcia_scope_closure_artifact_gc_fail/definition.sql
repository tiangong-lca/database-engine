CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_fail"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_error" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_artifact private.worker_job_artifacts%rowtype;
  v_gc_phase text;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return api.lcia_scope_closure_error(
      'invalid_gc_failure', 400, 'GC failure reason is required'
    );
  end if;

  select * into v_artifact
  from private.worker_job_artifacts
  where id = p_artifact_id
  for update;
  if v_artifact.id is null then
    return api.lcia_scope_closure_error(
      'artifact_not_found', 404, 'Artifact not found'
    );
  end if;
  if v_artifact.lifecycle_state = 'deleted'
     and v_artifact.gc_cleanup_state = 'complete'
     and v_artifact.gc_claim_token = p_claim_token then
    return jsonb_build_object('ok', true, 'reused', true);
  end if;
  if v_artifact.gc_claim_token is distinct from p_claim_token
     or v_artifact.gc_claim_expires_at is null
     or v_artifact.gc_claim_expires_at < now()
     or not (
       v_artifact.lifecycle_state = 'expired'
       or (
         v_artifact.lifecycle_state = 'deleted'
         and v_artifact.gc_cleanup_state = 'pending'
       )
     ) then
    return api.lcia_scope_closure_error(
      'gc_claim_invalid', 409, 'GC claim is not current'
    );
  end if;

  v_gc_phase := case
    when v_artifact.lifecycle_state = 'deleted' then 'detail_cleanup'
    else 'object_delete'
  end;

  update private.worker_job_artifacts
  set gc_claim_token = null,
      gc_claimed_at = null,
      gc_claim_expires_at = null,
      gc_failure_count = gc_failure_count + 1,
      gc_last_error = left(trim(p_error), 1000)
  where id = v_artifact.id
  returning * into v_artifact;

  return jsonb_build_object('ok', true, 'reused', false, 'data',
    jsonb_build_object(
      'artifactId', v_artifact.id,
      'state', v_artifact.lifecycle_state,
      'lifecycleState', v_artifact.lifecycle_state,
      'gcPhase', v_gc_phase,
      'objectDeleteRequired', v_gc_phase = 'object_delete',
      'failureCount', v_artifact.gc_failure_count
    )
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_fail"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_error" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_fail"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_error" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_fail"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_error" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_fail"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_error" "text") TO "api_internal_executor";
