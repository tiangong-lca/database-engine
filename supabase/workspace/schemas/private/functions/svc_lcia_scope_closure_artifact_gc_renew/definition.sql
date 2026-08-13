CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_renew"("p_claim_token" "uuid", "p_lease_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
  v_artifact_ids jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_claim_token is null
     or p_lease_seconds is null
     or p_lease_seconds not between 1 and 3600 then
    return api.lcia_scope_closure_error(
      'invalid_gc_renewal', 400, 'Invalid GC renewal request'
    );
  end if;
  with renewed as (
    update private.worker_job_artifacts artifact
    set gc_claim_expires_at = v_lease_expires_at
    where artifact.gc_claim_token = p_claim_token
      and artifact.gc_claim_expires_at >= now()
      and (
        artifact.lifecycle_state = 'expired'
        or (
          artifact.lifecycle_state = 'deleted'
          and artifact.gc_cleanup_state = 'pending'
        )
      )
    returning artifact.id
  )
  select jsonb_agg(id order by id)
  into v_artifact_ids
  from renewed;
  if v_artifact_ids is null then
    return api.lcia_scope_closure_error(
      'gc_claim_invalid', 409, 'GC claim is not current'
    );
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'claimToken', p_claim_token,
    'leaseExpiresAt', v_lease_expires_at,
    'artifactIds', v_artifact_ids
  ));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_renew"("p_claim_token" "uuid", "p_lease_seconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_renew"("p_claim_token" "uuid", "p_lease_seconds" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_renew"("p_claim_token" "uuid", "p_lease_seconds" integer) TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_renew"("p_claim_token" "uuid", "p_lease_seconds" integer) TO "api_internal_executor";
