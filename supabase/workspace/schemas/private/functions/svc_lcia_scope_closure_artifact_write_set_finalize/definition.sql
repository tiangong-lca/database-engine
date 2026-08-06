CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_finalize"("p_write_set_id" "uuid", "p_write_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
  v_report_id uuid;
  v_manifest_id uuid;
  v_bundle_id uuid;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_write_set.write_token is distinct from p_write_token then
    return api.lcia_scope_closure_error(
      'artifact_write_set_token_invalid',
      409,
      'Artifact write-set token is not current'
    );
  end if;
  if v_write_set.contract_version is not null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_fence_required',
      409,
      'Use the worker-lease-fenced v2 finalize RPC'
    );
  end if;
  if v_write_set.status = 'ready' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', private.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
    );
  end if;
  if v_write_set.status <> 'staging'
     or v_write_set.staging_expires_at <= now() then
    return api.lcia_scope_closure_error(
      'artifact_write_set_not_finalizable',
      409,
      'Artifact write-set is not finalizable'
    );
  end if;

  select id into v_report_id
  from private.lcia_scope_closure_artifact_write_set_items
  where write_set_id = v_write_set.id
    and artifact_role = 'closure_report';
  if v_write_set.publication_mode = 'fresh' then
    select id into v_manifest_id
    from private.lcia_scope_closure_artifact_write_set_items
    where write_set_id = v_write_set.id
      and artifact_role = 'complete_machine_result'
      and content_type =
        'application/vnd.tiangong.scope-closure-manifest+json';
    select id into v_bundle_id
    from private.lcia_scope_closure_artifact_write_set_items
    where write_set_id = v_write_set.id
      and artifact_role = 'closure_bundle';
  end if;

  insert into private.worker_job_artifacts (
    id,
    job_id,
    artifact_type,
    artifact_role,
    lifecycle_state,
    storage_bucket,
    storage_path,
    content_type,
    byte_size,
    checksum_sha256,
    metadata,
    visibility,
    created_at,
    expires_at
  )
  select
    item.id,
    v_write_set.worker_job_id,
    item.artifact_type,
    item.artifact_role,
    'ready',
    item.storage_bucket,
    item.storage_path,
    item.content_type,
    item.byte_size,
    item.checksum_sha256,
    case
      when item.artifact_role = 'closure_bundle' then
        (item.metadata - 'completeMachineResultClientKey')
        || jsonb_build_object(
          'completeMachineResultArtifactId', v_manifest_id
        )
      else item.metadata
    end || jsonb_build_object(
      'writeSetId', v_write_set.id,
      'closureCheckId', v_write_set.closure_check_id,
      'clientKey', item.client_key
    ),
    'operator',
    now(),
    now() + interval '7 days'
  from private.lcia_scope_closure_artifact_write_set_items item
  where item.write_set_id = v_write_set.id
  order by item.ordinal;

  if v_write_set.publication_mode = 'fresh' then
    update private.lcia_scope_closure_checks
    set report_artifact_id = v_report_id,
        complete_machine_result_artifact_id = v_manifest_id,
        closure_bundle_artifact_id = v_bundle_id,
        updated_at = now()
    where id = v_write_set.closure_check_id
      and worker_job_id = v_write_set.worker_job_id
      and reused_from_check_id is null;
  else
    update private.lcia_scope_closure_checks
    set report_artifact_id = v_report_id,
        updated_at = now()
    where id = v_write_set.closure_check_id
      and worker_job_id = v_write_set.worker_job_id
      and reused_from_check_id = v_write_set.reused_from_check_id
      and complete_machine_result_artifact_id is not null
      and closure_bundle_artifact_id is not null;
  end if;
  if not found then
    raise exception 'artifact_write_set_closure_binding_changed'
      using errcode = '23514';
  end if;

  update private.lcia_scope_closure_artifact_write_sets
  set status = 'ready',
      finalized_at = now(),
      updated_at = now(),
      failure_reason = null
  where id = v_write_set.id
  returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_finalize"("p_write_set_id" "uuid", "p_write_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_finalize"("p_write_set_id" "uuid", "p_write_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_finalize"("p_write_set_id" "uuid", "p_write_token" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_finalize"("p_write_set_id" "uuid", "p_write_token" "uuid") TO "api_internal_executor";
