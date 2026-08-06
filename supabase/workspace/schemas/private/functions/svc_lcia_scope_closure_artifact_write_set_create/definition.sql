CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create"("p_closure_check_id" "uuid", "p_idempotency_key" "text", "p_items" "jsonb", "p_staging_seconds" integer DEFAULT 900, "p_reused_from_check_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_check private.lcia_scope_closure_checks%rowtype;
  v_source_check private.lcia_scope_closure_checks%rowtype;
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
  v_request_sha256 text;
  v_item_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_idempotency_key, '')), '') is null
     or length(trim(p_idempotency_key)) > 200
     or jsonb_typeof(p_items) <> 'array'
     or jsonb_array_length(p_items) not between 1 and 500
     or p_staging_seconds is null
     or p_staging_seconds not between 1 and 86400 then
    return api.lcia_scope_closure_error(
      'artifact_write_set_invalid', 400, 'Invalid artifact write-set request'
    );
  end if;

  select * into v_check
  from private.lcia_scope_closure_checks
  where id = p_closure_check_id
  for update;
  if v_check.id is null then
    return api.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if v_check.status in ('failed', 'cancelled') then
    return api.lcia_scope_closure_error(
      'artifact_write_set_unavailable', 409, 'Closure check cannot publish artifacts'
    );
  end if;

  v_request_sha256 := private.lcia_scope_closure_sha256(
    jsonb_build_object(
      'items', p_items,
      'publicationMode',
        case when p_reused_from_check_id is null then 'fresh' else 'reused' end,
      'reusedFromCheckId', p_reused_from_check_id
    )
  );
  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where closure_check_id = p_closure_check_id
    and idempotency_key = trim(p_idempotency_key);
  if v_write_set.id is not null then
    if v_write_set.request_sha256 <> v_request_sha256 then
      return api.lcia_scope_closure_error(
        'artifact_write_set_idempotency_conflict',
        409,
        'Artifact write-set idempotency key conflicts'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', private.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_items) item
    where jsonb_typeof(item) <> 'object'
       or nullif(trim(item->>'clientKey'), '') is null
       or nullif(trim(item->>'artifactType'), '') is null
       or nullif(trim(item->>'artifactRole'), '') is null
       or nullif(trim(item->>'bucket'), '') is null
       or nullif(trim(item->>'objectPath'), '') is null
       or nullif(trim(item->>'mediaType'), '') is null
       or coalesce(item->>'checksumSha256', '') !~ '^[a-f0-9]{64}$'
       or coalesce(item->>'size', '') !~ '^[0-9]+$'
       or coalesce(item->'metadata', '{}'::jsonb) is null
       or jsonb_typeof(coalesce(item->'metadata', '{}'::jsonb)) <> 'object'
       or private.lcia_scope_closure_artifact_role(
            item->>'artifactType'
          ) is distinct from item->>'artifactRole'
  ) then
    return api.lcia_scope_closure_error(
      'artifact_write_set_invalid', 400, 'Artifact write-set item is invalid'
    );
  end if;

  select count(*) into v_item_count
  from (
    select item->>'clientKey'
    from jsonb_array_elements(p_items) item
    group by item->>'clientKey'
  ) distinct_client_keys;
  if v_item_count <> jsonb_array_length(p_items)
     or (
       select count(*)
       from (
         select item->>'bucket', item->>'objectPath'
         from jsonb_array_elements(p_items) item
         group by item->>'bucket', item->>'objectPath'
       ) distinct_locators
     ) <> jsonb_array_length(p_items)
  then
    return api.lcia_scope_closure_error(
      'artifact_write_set_invalid', 400, 'Artifact write-set keys or locators are invalid'
    );
  end if;

  if p_reused_from_check_id is null then
    if (
      select count(*)
      from jsonb_array_elements(p_items) item
      where item->>'artifactRole' = 'closure_report'
    ) <> 1
       or (
         select count(*)
         from jsonb_array_elements(p_items) item
         where item->>'artifactRole' = 'closure_bundle'
       ) <> 1
       or (
         select count(*)
         from jsonb_array_elements(p_items) item
         where item->>'artifactRole' = 'complete_machine_result'
           and item->>'mediaType' =
             'application/vnd.tiangong.scope-closure-manifest+json'
       ) <> 1
       or not exists (
         select 1
         from jsonb_array_elements(p_items) bundle
         join jsonb_array_elements(p_items) manifest
           on manifest->>'clientKey' =
             bundle #>> '{metadata,completeMachineResultClientKey}'
         where bundle->>'artifactRole' = 'closure_bundle'
           and manifest->>'artifactRole' = 'complete_machine_result'
           and manifest->>'mediaType' =
             'application/vnd.tiangong.scope-closure-manifest+json'
       ) then
      return api.lcia_scope_closure_error(
        'artifact_write_set_invalid',
        400,
        'Fresh publication requires report, manifest, and bundle roles'
      );
    end if;
  else
    if jsonb_array_length(p_items) <> 1
       or p_items #>> '{0,artifactRole}' <> 'closure_report'
       or p_items #>> '{0,artifactType}' <> 'closure_report_xlsx' then
      return api.lcia_scope_closure_error(
        'artifact_write_set_invalid',
        400,
        'Reused publication accepts exactly one XLSX report'
      );
    end if;
    select * into v_source_check
    from private.lcia_scope_closure_checks
    where id = p_reused_from_check_id;
    if v_source_check.id is null
       or v_source_check.status not in ('passed', 'blocked')
       or v_source_check.scan_completeness <> 'complete'
       or v_source_check.requested_scope_hash <> v_check.requested_scope_hash
       or v_source_check.policy_fingerprint <> v_check.policy_fingerprint
       or v_source_check.data_snapshot_token <> v_check.data_snapshot_token
       or v_source_check.complete_machine_result_artifact_id is null
       or v_source_check.closure_bundle_artifact_id is null then
      return api.lcia_scope_closure_error(
        'artifact_write_set_reuse_invalid',
        409,
        'Reusable source evidence does not match the closure check'
      );
    end if;
    update private.lcia_scope_closure_checks
    set reused_from_check_id = v_source_check.id,
        complete_machine_result_artifact_id =
          v_source_check.complete_machine_result_artifact_id,
        closure_bundle_artifact_id = v_source_check.closure_bundle_artifact_id,
        updated_at = now()
    where id = v_check.id;
  end if;

  insert into private.lcia_scope_closure_artifact_write_sets (
    closure_check_id,
    worker_job_id,
    requested_by,
    publication_mode,
    reused_from_check_id,
    idempotency_key,
    request_sha256,
    staging_expires_at
  ) values (
    v_check.id,
    v_check.worker_job_id,
    v_check.requested_by,
    case when p_reused_from_check_id is null then 'fresh' else 'reused' end,
    p_reused_from_check_id,
    trim(p_idempotency_key),
    v_request_sha256,
    now() + make_interval(secs => p_staging_seconds)
  ) returning * into v_write_set;

  insert into private.lcia_scope_closure_artifact_write_set_items (
    write_set_id,
    ordinal,
    client_key,
    artifact_type,
    artifact_role,
    storage_bucket,
    storage_path,
    content_type,
    byte_size,
    checksum_sha256,
    metadata
  )
  select
    v_write_set.id,
    item.ordinality::integer,
    trim(item.value->>'clientKey'),
    item.value->>'artifactType',
    item.value->>'artifactRole',
    trim(item.value->>'bucket'),
    trim(item.value->>'objectPath'),
    item.value->>'mediaType',
    (item.value->>'size')::bigint,
    item.value->>'checksumSha256',
    coalesce(item.value->'metadata', '{}'::jsonb)
  from jsonb_array_elements(p_items) with ordinality item(value, ordinality);

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$_$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create"("p_closure_check_id" "uuid", "p_idempotency_key" "text", "p_items" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create"("p_closure_check_id" "uuid", "p_idempotency_key" "text", "p_items" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create"("p_closure_check_id" "uuid", "p_idempotency_key" "text", "p_items" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_create"("p_closure_check_id" "uuid", "p_idempotency_key" "text", "p_items" "jsonb", "p_staging_seconds" integer, "p_reused_from_check_id" "uuid") TO "api_internal_executor";
