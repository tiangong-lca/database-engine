CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_claim"("p_limit" integer DEFAULT 100, "p_lease_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_claim_token uuid := gen_random_uuid();
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit is null
     or p_limit not between 1 and 500
     or p_lease_seconds is null
     or p_lease_seconds not between 1 and 3600 then
    return api.lcia_scope_closure_error(
      'invalid_gc_claim', 400, 'Invalid GC claim bounds'
    );
  end if;
  with candidates as (
    select id
    from private.worker_job_artifacts
    where artifact_role is not null
      and (
        (
          lifecycle_state in ('ready', 'expired')
          and expires_at <= now()
        )
        or (
          lifecycle_state = 'deleted'
          and gc_cleanup_state = 'pending'
        )
      )
      and (
        gc_claim_token is null
        or gc_claim_expires_at is null
        or gc_claim_expires_at <= now()
      )
    order by
      case
        when lifecycle_state = 'deleted'
             and gc_cleanup_state = 'pending' then 0
        else 1
      end,
      expires_at,
      created_at,
      id
    for update skip locked
    limit p_limit
  ), claimed as (
    update private.worker_job_artifacts artifact
    set lifecycle_state = case
          when artifact.lifecycle_state = 'deleted' then 'deleted'
          else 'expired'
        end,
        gc_claim_token = v_claim_token,
        gc_claimed_at = now(),
        gc_claim_expires_at = v_lease_expires_at,
        gc_last_error = null
    from candidates
    where artifact.id = candidates.id
    returning artifact.*
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'artifactId', id,
    'artifactRole', artifact_role,
    'lifecycleState', lifecycle_state,
    'gcPhase', case
      when lifecycle_state = 'deleted' then 'detail_cleanup'
      else 'object_delete'
    end,
    'objectDeleteRequired', lifecycle_state <> 'deleted',
    'bucket', storage_bucket,
    'objectPath', storage_path,
    'checksumSha256', checksum_sha256,
    'artifactExpiresAt', expires_at
  ) order by
    case when lifecycle_state = 'deleted' then 0 else 1 end,
    expires_at,
    created_at,
    id
  ), '[]'::jsonb)
  into v_items
  from claimed;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'claimToken', v_claim_token,
    'leaseExpiresAt', v_lease_expires_at,
    'items', v_items
  ));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_claim"("p_limit" integer, "p_lease_seconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_claim"("p_limit" integer, "p_lease_seconds" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_claim"("p_limit" integer, "p_lease_seconds" integer) TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_claim"("p_limit" integer, "p_lease_seconds" integer) TO "api_internal_executor";
