CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_finalize_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_report_id uuid;
  v_manifest_id uuid;
  v_bundle_id uuid;
  v_actual_sha256 text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_write_set_id is null
     or p_write_token is null
     or p_worker_job_id is null
     or p_worker_lease_token is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact write-set finalize request'
    );
  end if;

  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null
     or v_write_set.contract_version is distinct from
       'lcia.scope-closure-artifact-write-set.v2' then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_write_set.write_token is distinct from p_write_token then
    return public.lcia_scope_closure_error(
      'artifact_write_set_token_invalid',
      409,
      'Artifact write-set token is not current'
    );
  end if;
  if v_write_set.worker_job_id is distinct from p_worker_job_id then
    return public.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job is not bound to the artifact write-set'
    );
  end if;

  select * into v_job
  from public.worker_jobs
  where id = p_worker_job_id;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_worker_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now()
     or v_write_set.worker_lease_token_sha256 is distinct from
       private.lcia_scope_closure_artifact_v2_lease_sha256(
         p_worker_lease_token
       ) then
    return public.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is no longer valid'
    );
  end if;

  if v_write_set.status = 'ready' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data',
        private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
    );
  end if;
  if v_write_set.status <> 'staging'
     or v_write_set.sealed_at is null
     or v_write_set.staging_expires_at <= now() then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_finalizable',
      409,
      'Artifact write-set is not finalizable'
    );
  end if;

  v_actual_sha256 :=
    private.lcia_scope_closure_worker_canonical_sha256(
      private.lcia_scope_closure_artifact_v2_descriptor_set(v_write_set.id)
    );
  if v_actual_sha256 is distinct from v_write_set.descriptor_set_sha256 then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_digest_mismatch',
      409,
      'Sealed artifact descriptor-set digest changed'
    );
  end if;

  select id into v_report_id
  from public.lcia_scope_closure_artifact_write_set_items
  where write_set_id = v_write_set.id
    and artifact_role = 'closure_report';
  if v_write_set.publication_mode = 'fresh' then
    select id into v_manifest_id
    from public.lcia_scope_closure_artifact_write_set_items
    where write_set_id = v_write_set.id
      and artifact_role = 'complete_machine_result'
      and content_type =
        'application/vnd.tiangong.scope-closure-manifest+json';
    select id into v_bundle_id
    from public.lcia_scope_closure_artifact_write_set_items
    where write_set_id = v_write_set.id
      and artifact_role = 'closure_bundle';
  end if;

  insert into public.worker_job_artifacts (
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
  from public.lcia_scope_closure_artifact_write_set_items item
  where item.write_set_id = v_write_set.id
  order by item.ordinal;

  if v_write_set.publication_mode = 'fresh' then
    update public.lcia_scope_closure_checks
    set report_artifact_id = v_report_id,
        complete_machine_result_artifact_id = v_manifest_id,
        closure_bundle_artifact_id = v_bundle_id,
        updated_at = now()
    where id = v_write_set.closure_check_id
      and worker_job_id = v_write_set.worker_job_id
      and reused_from_check_id is null;
  else
    update public.lcia_scope_closure_checks
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

  update public.lcia_scope_closure_artifact_write_sets
  set status = 'ready',
      finalized_at = now(),
      updated_at = now(),
      failure_reason = null
  where id = v_write_set.id
  returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_finalize_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_finalize_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_finalize_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") TO "service_role";
