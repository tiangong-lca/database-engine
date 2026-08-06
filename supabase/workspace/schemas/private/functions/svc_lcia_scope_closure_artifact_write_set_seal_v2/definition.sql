CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_seal_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_job private.worker_jobs%rowtype;
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
  v_item_count integer;
  v_min_ordinal integer;
  v_max_ordinal integer;
  v_actual_sha256 text;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_write_set_id is null
     or p_write_token is null
     or p_worker_job_id is null
     or p_worker_lease_token is null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact write-set seal request'
    );
  end if;

  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null
     or v_write_set.contract_version is distinct from
       'lcia.scope-closure-artifact-write-set.v2' then
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
  if v_write_set.worker_job_id is distinct from p_worker_job_id then
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job is not bound to the artifact write-set'
    );
  end if;

  select * into v_job
  from private.worker_jobs
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
    return api.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is no longer valid'
    );
  end if;

  if v_write_set.status in ('staging', 'ready')
     and v_write_set.sealed_at is not null then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data',
        private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
    );
  end if;
  if v_write_set.status <> 'registration_open'
     or v_write_set.staging_expires_at <= now() then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_registration_closed',
      409,
      'Artifact descriptor registration is closed'
    );
  end if;

  select count(*), min(ordinal), max(ordinal)
  into v_item_count, v_min_ordinal, v_max_ordinal
  from private.lcia_scope_closure_artifact_write_set_items
  where write_set_id = v_write_set.id;
  if v_item_count <> v_write_set.expected_descriptor_count
     or v_min_ordinal is distinct from 1
     or v_max_ordinal is distinct from
       v_write_set.expected_descriptor_count then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_incomplete',
      409,
      'Artifact descriptor set is incomplete'
    );
  end if;

  v_actual_sha256 :=
    private.lcia_scope_closure_worker_canonical_sha256(
      private.lcia_scope_closure_artifact_v2_descriptor_set(v_write_set.id)
    );
  if v_actual_sha256 is distinct from v_write_set.descriptor_set_sha256 then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_digest_mismatch',
      409,
      'Artifact descriptor-set digest does not match'
    );
  end if;

  if v_write_set.required_primary_roles is distinct from
       private.lcia_scope_closure_artifact_v2_required_roles(
         v_write_set.publication_mode
       )
     or exists (
       select 1
       from private.lcia_scope_closure_artifact_write_set_items item
       where item.write_set_id = v_write_set.id
         and (
           item.metadata->>'schemaVersion' is distinct from
             'lcia.scope-closure-artifact.v2'
           or item.metadata->>'closureCheckId' is distinct from
             v_write_set.closure_check_id::text
           or item.metadata->>'fileName' is distinct from item.client_key
           or item.metadata->>'artifactRole' is distinct from
             item.artifact_role
           or item.metadata->>'retentionSeconds' is distinct from '604800'
           or coalesce(
             item.metadata->>'contentArtifactManifestHash',
             ''
           ) !~ '^[a-f0-9]{64}$'
           or position(
             'scope-closure/' || v_write_set.closure_check_id::text || '/'
             in item.storage_path
           ) = 0
           or right(
             item.storage_path,
             length(item.client_key) + 1
           ) <> '/' || item.client_key
         )
     ) then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_primary_roles_invalid',
      409,
      'Artifact descriptor invariants are invalid'
    );
  end if;

  if v_write_set.publication_mode = 'fresh' then
    if (
      select count(*)
      from private.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = v_write_set.id
        and item.artifact_role = 'closure_report'
        and item.artifact_type = 'closure_report_xlsx'
        and item.content_type =
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ) <> 1
       or (
         select count(*)
         from private.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'closure_report'
       ) <> 1
       or (
         select count(*)
         from private.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'closure_bundle'
           and item.artifact_type = 'closure_bundle'
           and item.content_type = 'application/json'
       ) <> 1
       or (
         select count(*)
         from private.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'closure_bundle'
       ) <> 1
       or (
         select count(*)
         from private.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'complete_machine_result'
           and item.artifact_type = 'closure_complete_machine_result'
           and item.content_type =
             'application/vnd.tiangong.scope-closure-manifest+json'
       ) <> 1
       or not exists (
         select 1
         from private.lcia_scope_closure_artifact_write_set_items bundle
         join private.lcia_scope_closure_artifact_write_set_items manifest
           on manifest.write_set_id = bundle.write_set_id
          and manifest.client_key =
            bundle.metadata->>'completeMachineResultClientKey'
         where bundle.write_set_id = v_write_set.id
           and bundle.artifact_role = 'closure_bundle'
           and manifest.artifact_role = 'complete_machine_result'
           and manifest.artifact_type =
             'closure_complete_machine_result'
           and manifest.content_type =
             'application/vnd.tiangong.scope-closure-manifest+json'
       ) then
      return api.lcia_scope_closure_error(
        'artifact_write_set_v2_primary_roles_invalid',
        409,
        'Fresh publication primary artifact roles are invalid'
      );
    end if;
  elsif v_item_count <> 1
     or not exists (
       select 1
       from private.lcia_scope_closure_artifact_write_set_items item
       where item.write_set_id = v_write_set.id
         and item.artifact_role = 'closure_report'
         and item.artifact_type = 'closure_report_xlsx'
         and item.content_type =
           'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
     ) then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_primary_roles_invalid',
      409,
      'Reused publication primary artifact role is invalid'
    );
  end if;

  update private.lcia_scope_closure_artifact_write_sets
  set status = 'staging',
      sealed_at = now(),
      updated_at = now()
  where id = v_write_set.id
  returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$_$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_seal_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_seal_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_seal_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_seal_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid") TO "api_internal_executor";
