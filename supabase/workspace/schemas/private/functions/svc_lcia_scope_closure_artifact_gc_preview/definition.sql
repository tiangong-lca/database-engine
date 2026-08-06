CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_preview"("p_limit" integer DEFAULT 100) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit is null or p_limit not between 1 and 500 then
    return api.lcia_scope_closure_error(
      'invalid_gc_preview', 400, 'Invalid GC preview bound'
    );
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'artifactId', artifact.id,
    'artifactRole', artifact.artifact_role,
    'lifecycleState', artifact.lifecycle_state,
    'gcPhase', case
      when artifact.lifecycle_state = 'deleted' then 'detail_cleanup'
      else 'object_delete'
    end,
    'objectDeleteRequired', artifact.lifecycle_state <> 'deleted',
    'bucket', artifact.storage_bucket,
    'objectPath', artifact.storage_path,
    'checksumSha256', artifact.checksum_sha256,
    'artifactExpiresAt', artifact.expires_at
  ) order by
    case when artifact.lifecycle_state = 'deleted' then 0 else 1 end,
    artifact.expires_at,
    artifact.created_at,
    artifact.id
  ), '[]'::jsonb)
  into v_items
  from (
    select *
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
    limit p_limit
  ) artifact;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'items', v_items
  ));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_preview"("p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_preview"("p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_preview"("p_limit" integer) TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_gc_preview"("p_limit" integer) TO "api_internal_executor";
