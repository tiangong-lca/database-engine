CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_register_batch_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_batch_id" "uuid", "p_items" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_job private.worker_jobs%rowtype;
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
  v_batch private.lcia_scope_closure_artifact_write_set_batches%rowtype;
  v_items jsonb;
  v_request_sha256 text;
  v_first_ordinal integer;
  v_last_ordinal integer;
  v_item_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_write_set_id is null
     or p_write_token is null
     or p_worker_job_id is null
     or p_worker_lease_token is null
     or p_batch_id is null
     or jsonb_typeof(p_items) <> 'array'
     or jsonb_array_length(p_items) not between 1 and 500 then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact descriptor batch'
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_items) item
    where jsonb_typeof(item) <> 'object'
       or (
         select count(*)
         from jsonb_object_keys(item) key
       ) <> 10
       or exists (
         select 1
         from jsonb_object_keys(item) key
         where key not in (
           'ordinal',
           'clientKey',
           'artifactType',
           'artifactRole',
           'bucket',
           'objectPath',
           'mediaType',
           'size',
           'checksumSha256',
           'metadata'
         )
       )
       or jsonb_typeof(item->'ordinal') <> 'number'
       or coalesce(item->>'ordinal', '') !~ '^[1-9][0-9]*$'
       or length(item->>'ordinal') > 6
       or nullif(item->>'clientKey', '') is null
       or item->>'clientKey' <> trim(item->>'clientKey')
       or length(item->>'clientKey') > 500
       or nullif(item->>'artifactType', '') is null
       or item->>'artifactType' <> trim(item->>'artifactType')
       or length(item->>'artifactType') > 200
       or nullif(item->>'artifactRole', '') is null
       or item->>'artifactRole' <> trim(item->>'artifactRole')
       or length(item->>'artifactRole') > 200
       or nullif(item->>'bucket', '') is null
       or item->>'bucket' <> trim(item->>'bucket')
       or length(item->>'bucket') > 255
       or nullif(item->>'objectPath', '') is null
       or item->>'objectPath' <> trim(item->>'objectPath')
       or length(item->>'objectPath') > 2048
       or nullif(item->>'mediaType', '') is null
       or item->>'mediaType' <> trim(item->>'mediaType')
       or length(item->>'mediaType') > 255
       or jsonb_typeof(item->'size') <> 'number'
       or coalesce(item->>'size', '') !~ '^(0|[1-9][0-9]*)$'
       or length(item->>'size') > 19
       or (
         length(item->>'size') = 19
         and item->>'size' > '9223372036854775807'
       )
       or coalesce(item->>'checksumSha256', '') !~ '^[a-f0-9]{64}$'
       or jsonb_typeof(item->'metadata') <> 'object'
       or octet_length((item->'metadata')::text) > 65536
       or private.lcia_scope_closure_artifact_role(
            item->>'artifactType'
          ) is distinct from item->>'artifactRole'
       or item->>'clientKey' like '/%'
       or item->>'clientKey' like '%//%'
       or item->>'clientKey' ~ '(^|/)\.\.?(/|$)'
       or item->>'clientKey' ~ '[[:cntrl:]\\]'
       or item->>'objectPath' like '/%'
       or item->>'objectPath' like '%//%'
       or item->>'objectPath' ~ '(^|/)\.\.?(/|$)'
       or item->>'objectPath' ~ '[[:cntrl:]\\]'
       or item->>'bucket' ~ '[[:cntrl:]/\\]'
  ) then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Staged artifact descriptor is invalid'
    );
  end if;

  if exists (
    select 1
    from (
      select
        (item.value->>'ordinal')::integer as ordinal,
        item.ordinality,
        lag((item.value->>'ordinal')::integer)
          over (order by item.ordinality) as previous_ordinal
      from jsonb_array_elements(p_items)
        with ordinality item(value, ordinality)
    ) ordered
    where (
      ordered.ordinality > 1
      and ordered.ordinal <> ordered.previous_ordinal + 1
    )
  ) then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Batch ordinals must be strictly ascending and contiguous'
    );
  end if;

  select jsonb_agg(jsonb_build_object(
    'ordinal', (item.value->>'ordinal')::integer,
    'clientKey', item.value->>'clientKey',
    'artifactType', item.value->>'artifactType',
    'artifactRole', item.value->>'artifactRole',
    'bucket', item.value->>'bucket',
    'objectPath', item.value->>'objectPath',
    'mediaType', item.value->>'mediaType',
    'size', (item.value->>'size')::bigint,
    'checksumSha256', item.value->>'checksumSha256',
    'metadata', item.value->'metadata'
  ) order by item.ordinality)
  into v_items
  from jsonb_array_elements(p_items)
    with ordinality item(value, ordinality);

  v_item_count := jsonb_array_length(v_items);
  v_first_ordinal := (v_items #>> '{0,ordinal}')::integer;
  v_last_ordinal :=
    (v_items #>> array[(v_item_count - 1)::text, 'ordinal'])::integer;
  v_request_sha256 :=
    private.lcia_scope_closure_worker_canonical_sha256(jsonb_build_object(
      'contractVersion',
        'lcia.scope-closure-artifact-write-set.v2',
      'batchId', p_batch_id,
      'items', v_items
    ));

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

  select * into v_batch
  from private.lcia_scope_closure_artifact_write_set_batches
  where write_set_id = v_write_set.id
    and batch_id = p_batch_id;
  if v_batch.write_set_id is not null then
    if v_batch.request_sha256 is distinct from v_request_sha256 then
      return api.lcia_scope_closure_error(
        'artifact_write_set_v2_batch_conflict',
        409,
        'Artifact descriptor batch identity conflicts'
      );
    end if;
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
  if v_first_ordinal < 1
     or v_last_ordinal > v_write_set.expected_descriptor_count then
    return api.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Artifact descriptor ordinal is outside the expected range'
    );
  end if;

  begin
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
      (item->>'ordinal')::integer,
      item->>'clientKey',
      item->>'artifactType',
      item->>'artifactRole',
      item->>'bucket',
      item->>'objectPath',
      item->>'mediaType',
      (item->>'size')::bigint,
      item->>'checksumSha256',
      item->'metadata'
    from jsonb_array_elements(v_items) item
    order by (item->>'ordinal')::integer;

    insert into private.lcia_scope_closure_artifact_write_set_batches (
      write_set_id,
      batch_id,
      request_sha256,
      item_count,
      first_ordinal,
      last_ordinal
    ) values (
      v_write_set.id,
      p_batch_id,
      v_request_sha256,
      v_item_count,
      v_first_ordinal,
      v_last_ordinal
    );
  exception
    when unique_violation then
      return api.lcia_scope_closure_error(
        'artifact_write_set_v2_descriptor_conflict',
        409,
        'Artifact descriptor ordinal, client key, or locator conflicts'
      );
  end;

  update private.lcia_scope_closure_artifact_write_sets
  set updated_at = now()
  where id = v_write_set.id;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$_$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_register_batch_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_batch_id" "uuid", "p_items" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_register_batch_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_batch_id" "uuid", "p_items" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_register_batch_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_batch_id" "uuid", "p_items" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_register_batch_v2"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_worker_job_id" "uuid", "p_worker_lease_token" "uuid", "p_batch_id" "uuid", "p_items" "jsonb") TO "api_internal_executor";
