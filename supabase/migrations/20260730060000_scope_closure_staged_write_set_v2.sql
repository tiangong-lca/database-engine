-- Issue #316 expands scope-closure artifact publication from the bounded
-- one-shot v1 adapter to a lease-fenced, batched v2 registration protocol.
-- The v1 overloads remain available during the migrate window.  A v2 header
-- stays upload-ineligible until one atomic seal verifies the complete,
-- canonical descriptor set.

alter table public.lcia_scope_closure_artifact_write_sets
  add column contract_version text,
  add column request_id uuid,
  add column expected_descriptor_count integer,
  add column descriptor_set_sha256 text,
  add column required_primary_roles jsonb,
  add column worker_lease_token_sha256 text,
  add column sealed_at timestamptz;

alter table public.lcia_scope_closure_artifact_write_sets
  drop constraint lcia_scope_closure_artifact_write_sets_status_check;

alter table public.lcia_scope_closure_artifact_write_sets
  add constraint lcia_scope_closure_artifact_write_sets_status_check
    check (
      status in (
        'registration_open',
        'staging',
        'ready',
        'cleanup_pending',
        'cleaned'
      )
      and (status <> 'registration_open' or contract_version is not null)
    ),
  add constraint lcia_scope_closure_artifact_write_sets_v2_shape_check
    check (
      (
        contract_version is null
        and request_id is null
        and expected_descriptor_count is null
        and descriptor_set_sha256 is null
        and required_primary_roles is null
        and worker_lease_token_sha256 is null
        and sealed_at is null
      )
      or
      (
        contract_version =
          'lcia.scope-closure-artifact-write-set.v2'
        and request_id is not null
        and expected_descriptor_count between 1 and 100000
        and descriptor_set_sha256 ~ '^[a-f0-9]{64}$'
        and jsonb_typeof(required_primary_roles) = 'array'
        and worker_lease_token_sha256 ~ '^[a-f0-9]{64}$'
      )
    ),
  add constraint lcia_scope_closure_artifact_write_sets_v2_seal_check
    check (
      contract_version is null
      or status in ('registration_open', 'cleanup_pending', 'cleaned')
      or sealed_at is not null
    ),
  add constraint lcia_scope_closure_artifact_write_sets_request_uidx
    unique (closure_check_id, request_id);

drop index public.lcia_scope_closure_artifact_write_sets_active_uidx;
create unique index lcia_scope_closure_artifact_write_sets_active_uidx
  on public.lcia_scope_closure_artifact_write_sets (closure_check_id)
  where status in ('registration_open', 'staging', 'ready');

create index lcia_scope_closure_artifact_write_sets_worker_job_idx
  on public.lcia_scope_closure_artifact_write_sets (
    worker_job_id,
    status,
    created_at,
    id
  );

create table public.lcia_scope_closure_artifact_write_set_batches (
  write_set_id uuid not null
    references public.lcia_scope_closure_artifact_write_sets(id)
    on delete restrict,
  batch_id uuid not null,
  request_sha256 text not null,
  item_count integer not null,
  first_ordinal integer not null,
  last_ordinal integer not null,
  created_at timestamptz not null default now(),
  constraint lcia_scope_closure_artifact_write_set_batches_pkey
    primary key (write_set_id, batch_id),
  constraint lcia_scope_closure_artifact_write_set_batches_hash_check
    check (request_sha256 ~ '^[a-f0-9]{64}$'),
  constraint lcia_scope_closure_artifact_write_set_batches_count_check
    check (item_count between 1 and 500),
  constraint lcia_scope_closure_artifact_write_set_batches_range_check
    check (
      first_ordinal > 0
      and last_ordinal >= first_ordinal
      and last_ordinal - first_ordinal + 1 = item_count
    )
);

create index lcia_scope_closure_artifact_write_set_batches_range_idx
  on public.lcia_scope_closure_artifact_write_set_batches (
    write_set_id,
    first_ordinal,
    last_ordinal,
    batch_id
  );

alter table public.lcia_scope_closure_artifact_write_set_batches
  enable row level security;
revoke all on public.lcia_scope_closure_artifact_write_set_batches
  from public, anon, authenticated, service_role;

create or replace function private.lcia_scope_closure_artifact_v2_lease_sha256(
  p_lease_token uuid
) returns text
language sql
immutable
strict
parallel safe
set search_path = ''
as $$
  select encode(
    extensions.digest(convert_to(p_lease_token::text, 'UTF8'), 'sha256'),
    'hex'
  )
$$;

create or replace function private.lcia_scope_closure_artifact_v2_required_roles(
  p_publication_mode text
) returns jsonb
language sql
immutable
strict
parallel safe
set search_path = ''
as $$
  select case p_publication_mode
    when 'fresh' then jsonb_build_array(
      jsonb_build_object(
        'artifactRole', 'closure_report',
        'artifactType', 'closure_report_xlsx',
        'mediaType',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'exactCount', 1
      ),
      jsonb_build_object(
        'artifactRole', 'complete_machine_result',
        'artifactType', 'closure_complete_machine_result',
        'mediaType',
          'application/vnd.tiangong.scope-closure-manifest+json',
        'exactCount', 1
      ),
      jsonb_build_object(
        'artifactRole', 'closure_bundle',
        'artifactType', 'closure_bundle',
        'mediaType', 'application/json',
        'exactCount', 1
      )
    )
    when 'reused' then jsonb_build_array(
      jsonb_build_object(
        'artifactRole', 'closure_report',
        'artifactType', 'closure_report_xlsx',
        'mediaType',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'exactCount', 1
      )
    )
    else null
  end
$$;

create or replace function private.lcia_scope_closure_artifact_v2_descriptor_set(
  p_write_set_id uuid
) returns jsonb
language sql
stable
set search_path = ''
as $$
  select jsonb_build_object(
    'contractVersion', write_set.contract_version,
    'descriptors', coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'ordinal', item.ordinal,
          'clientKey', item.client_key,
          'artifactType', item.artifact_type,
          'artifactRole', item.artifact_role,
          'bucket', item.storage_bucket,
          'objectPath', item.storage_path,
          'mediaType', item.content_type,
          'size', item.byte_size,
          'checksumSha256', item.checksum_sha256,
          'metadata', item.metadata
        )
        order by item.ordinal
      )
      from public.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ), '[]'::jsonb)
  )
  from public.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = p_write_set_id
$$;

create or replace function private.lcia_scope_closure_artifact_v2_status_json(
  p_write_set_id uuid
) returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  select jsonb_build_object(
    'contractVersion', write_set.contract_version,
    'writeSetId', write_set.id,
    'closureCheckId', write_set.closure_check_id,
    'workerJobId', write_set.worker_job_id,
    'requestId', write_set.request_id,
    'publicationMode', write_set.publication_mode,
    'reusedFromCheckId', write_set.reused_from_check_id,
    'status', write_set.status,
    'uploadEligible', write_set.status = 'staging',
    'writeToken', write_set.write_token,
    'expectedDescriptorCount', write_set.expected_descriptor_count,
    'registeredDescriptorCount', (
      select count(*)
      from public.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ),
    'registeredBatchCount', (
      select count(*)
      from public.lcia_scope_closure_artifact_write_set_batches batch
      where batch.write_set_id = write_set.id
    ),
    'descriptorSetSha256', write_set.descriptor_set_sha256,
    'requiredPrimaryRoles', write_set.required_primary_roles,
    'stagingExpiresAt', write_set.staging_expires_at,
    'sealedAt', write_set.sealed_at,
    'finalizedAt', write_set.finalized_at,
    'cleanedAt', write_set.cleaned_at,
    'artifactMap', case
      when write_set.sealed_at is null then '{}'::jsonb
      else coalesce((
        select jsonb_object_agg(item.client_key, item.id order by item.client_key)
        from public.lcia_scope_closure_artifact_write_set_items item
        where item.write_set_id = write_set.id
      ), '{}'::jsonb)
    end,
    'batches', coalesce((
      select jsonb_agg(jsonb_build_object(
        'batchId', batch.batch_id,
        'itemCount', batch.item_count,
        'firstOrdinal', batch.first_ordinal,
        'lastOrdinal', batch.last_ordinal
      ) order by batch.first_ordinal, batch.batch_id)
      from public.lcia_scope_closure_artifact_write_set_batches batch
      where batch.write_set_id = write_set.id
    ), '[]'::jsonb)
  )
  from public.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = p_write_set_id
$$;

create or replace function private.lcia_scope_closure_artifact_v2_item_guard()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_contract_version text;
  v_status text;
  v_write_set_id uuid := case
    when tg_op = 'DELETE' then old.write_set_id
    else new.write_set_id
  end;
begin
  select write_set.contract_version, write_set.status
  into v_contract_version, v_status
  from public.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = v_write_set_id;

  if v_contract_version is null then
    return case when tg_op = 'DELETE' then old else new end;
  end if;
  if tg_op = 'INSERT' and v_status = 'registration_open' then
    return new;
  end if;
  raise exception 'artifact_write_set_v2_items_are_immutable'
    using errcode = '23514';
end;
$$;

create trigger lcia_scope_closure_artifact_write_set_items_v2_guard
before insert or update or delete
on public.lcia_scope_closure_artifact_write_set_items
for each row execute function private.lcia_scope_closure_artifact_v2_item_guard();

create or replace function public.svc_lcia_scope_closure_artifact_write_set_create_v2(
  p_closure_check_id uuid,
  p_worker_job_id uuid,
  p_worker_lease_token uuid,
  p_request_id uuid,
  p_contract_version text,
  p_expected_descriptor_count integer,
  p_descriptor_set_sha256 text,
  p_required_primary_roles jsonb,
  p_staging_seconds integer default 900,
  p_reused_from_check_id uuid default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_check public.lcia_scope_closure_checks%rowtype;
  v_source_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_publication_mode text := case
    when p_reused_from_check_id is null then 'fresh'
    else 'reused'
  end;
  v_required_roles jsonb;
  v_lease_sha256 text;
  v_request_sha256 text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;

  v_required_roles :=
    private.lcia_scope_closure_artifact_v2_required_roles(v_publication_mode);
  if p_closure_check_id is null
     or p_worker_job_id is null
     or p_worker_lease_token is null
     or p_request_id is null
     or p_contract_version is distinct from
       'lcia.scope-closure-artifact-write-set.v2'
     or p_expected_descriptor_count is null
     or p_expected_descriptor_count not between 1 and 100000
     or (
       v_publication_mode = 'fresh'
       and p_expected_descriptor_count < 3
     )
     or (
       v_publication_mode = 'reused'
       and p_expected_descriptor_count <> 1
     )
     or coalesce(p_descriptor_set_sha256, '') !~ '^[a-f0-9]{64}$'
     or p_required_primary_roles is distinct from v_required_roles
     or p_staging_seconds is null
     or p_staging_seconds not between 1 and 86400 then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact write-set header'
    );
  end if;

  select * into v_job
  from public.worker_jobs
  where id = p_worker_job_id;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_worker_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now() then
    return public.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is no longer valid'
    );
  end if;

  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
  for update;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if v_check.worker_job_id is distinct from v_job.id then
    return public.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is not bound to the closure check'
    );
  end if;
  if v_check.status in ('failed', 'cancelled') then
    return public.lcia_scope_closure_error(
      'artifact_write_set_unavailable',
      409,
      'Closure check cannot publish artifacts'
    );
  end if;

  if v_publication_mode = 'reused' then
    select * into v_source_check
    from public.lcia_scope_closure_checks
    where id = p_reused_from_check_id;
    if v_source_check.id is null
       or v_source_check.status not in ('passed', 'blocked')
       or v_source_check.scan_completeness <> 'complete'
       or v_source_check.requested_scope_hash <> v_check.requested_scope_hash
       or v_source_check.policy_fingerprint <> v_check.policy_fingerprint
       or v_source_check.data_snapshot_token <> v_check.data_snapshot_token
       or v_source_check.complete_machine_result_artifact_id is null
       or v_source_check.closure_bundle_artifact_id is null then
      return public.lcia_scope_closure_error(
        'artifact_write_set_reuse_invalid',
        409,
        'Reusable source evidence does not match the closure check'
      );
    end if;
  end if;

  v_lease_sha256 :=
    private.lcia_scope_closure_artifact_v2_lease_sha256(
      p_worker_lease_token
    );
  v_request_sha256 :=
    private.lcia_scope_closure_worker_canonical_sha256(jsonb_build_object(
      'closureCheckId', p_closure_check_id,
      'workerJobId', p_worker_job_id,
      'workerLeaseTokenSha256', v_lease_sha256,
      'requestId', p_request_id,
      'contractVersion', p_contract_version,
      'expectedDescriptorCount', p_expected_descriptor_count,
      'descriptorSetSha256', p_descriptor_set_sha256,
      'requiredPrimaryRoles', p_required_primary_roles,
      'stagingSeconds', p_staging_seconds,
      'publicationMode', v_publication_mode,
      'reusedFromCheckId', p_reused_from_check_id
    ));

  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where closure_check_id = p_closure_check_id
    and request_id = p_request_id;
  if v_write_set.id is not null then
    if v_write_set.request_sha256 is distinct from v_request_sha256
       or v_write_set.worker_job_id is distinct from p_worker_job_id
       or v_write_set.worker_lease_token_sha256 is distinct from
         v_lease_sha256 then
      return public.lcia_scope_closure_error(
        'artifact_write_set_v2_request_conflict',
        409,
        'Staged artifact write-set request identity conflicts'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data',
        private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
    );
  end if;

  if exists (
    select 1
    from public.lcia_scope_closure_artifact_write_sets active
    where active.closure_check_id = p_closure_check_id
      and active.status in ('registration_open', 'staging', 'ready')
  ) then
    return public.lcia_scope_closure_error(
      'artifact_write_set_unavailable',
      409,
      'Closure check already has an active artifact write-set'
    );
  end if;

  if v_publication_mode = 'reused' then
    update public.lcia_scope_closure_checks
    set reused_from_check_id = v_source_check.id,
        complete_machine_result_artifact_id =
          v_source_check.complete_machine_result_artifact_id,
        closure_bundle_artifact_id =
          v_source_check.closure_bundle_artifact_id,
        updated_at = now()
    where id = v_check.id;
  end if;

  insert into public.lcia_scope_closure_artifact_write_sets (
    closure_check_id,
    worker_job_id,
    requested_by,
    publication_mode,
    reused_from_check_id,
    idempotency_key,
    request_sha256,
    status,
    staging_expires_at,
    contract_version,
    request_id,
    expected_descriptor_count,
    descriptor_set_sha256,
    required_primary_roles,
    worker_lease_token_sha256
  ) values (
    v_check.id,
    v_job.id,
    v_check.requested_by,
    v_publication_mode,
    p_reused_from_check_id,
    'v2:' || p_request_id::text,
    v_request_sha256,
    'registration_open',
    least(
      now() + make_interval(secs => p_staging_seconds),
      v_job.lease_expires_at
    ),
    p_contract_version,
    p_request_id,
    p_expected_descriptor_count,
    p_descriptor_set_sha256,
    p_required_primary_roles,
    v_lease_sha256
  ) returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_write_set_status_v2(
  p_closure_check_id uuid,
  p_worker_job_id uuid,
  p_worker_lease_token uuid,
  p_request_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_lease_sha256 text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_closure_check_id is null
     or p_worker_job_id is null
     or p_worker_lease_token is null
     or p_request_id is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Invalid staged artifact write-set status request'
    );
  end if;

  select * into v_job
  from public.worker_jobs
  where id = p_worker_job_id;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_worker_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now() then
    return public.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is no longer valid'
    );
  end if;

  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where closure_check_id = p_closure_check_id
    and worker_job_id = p_worker_job_id
    and request_id = p_request_id
    and contract_version =
      'lcia.scope-closure-artifact-write-set.v2';
  if v_write_set.id is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;

  v_lease_sha256 :=
    private.lcia_scope_closure_artifact_v2_lease_sha256(
      p_worker_lease_token
    );
  if v_write_set.worker_lease_token_sha256 is distinct from v_lease_sha256 then
    return public.lcia_scope_closure_error(
      'worker_job_lease_invalid',
      409,
      'Worker job lease is not bound to the artifact write-set'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
  p_write_set_id uuid,
  p_write_token uuid,
  p_worker_job_id uuid,
  p_worker_lease_token uuid,
  p_batch_id uuid,
  p_items jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_batch public.lcia_scope_closure_artifact_write_set_batches%rowtype;
  v_items jsonb;
  v_request_sha256 text;
  v_first_ordinal integer;
  v_last_ordinal integer;
  v_item_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
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
    return public.lcia_scope_closure_error(
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
       or public.lcia_scope_closure_artifact_role(
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
    return public.lcia_scope_closure_error(
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
    return public.lcia_scope_closure_error(
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

  select * into v_batch
  from public.lcia_scope_closure_artifact_write_set_batches
  where write_set_id = v_write_set.id
    and batch_id = p_batch_id;
  if v_batch.write_set_id is not null then
    if v_batch.request_sha256 is distinct from v_request_sha256 then
      return public.lcia_scope_closure_error(
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
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_registration_closed',
      409,
      'Artifact descriptor registration is closed'
    );
  end if;
  if v_first_ordinal < 1
     or v_last_ordinal > v_write_set.expected_descriptor_count then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_invalid',
      400,
      'Artifact descriptor ordinal is outside the expected range'
    );
  end if;

  begin
    insert into public.lcia_scope_closure_artifact_write_set_items (
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

    insert into public.lcia_scope_closure_artifact_write_set_batches (
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
      return public.lcia_scope_closure_error(
        'artifact_write_set_v2_descriptor_conflict',
        409,
        'Artifact descriptor ordinal, client key, or locator conflicts'
      );
  end;

  update public.lcia_scope_closure_artifact_write_sets
  set updated_at = now()
  where id = v_write_set.id;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_write_set_seal_v2(
  p_write_set_id uuid,
  p_write_token uuid,
  p_worker_job_id uuid,
  p_worker_lease_token uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_item_count integer;
  v_min_ordinal integer;
  v_max_ordinal integer;
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
      'Invalid staged artifact write-set seal request'
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
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_registration_closed',
      409,
      'Artifact descriptor registration is closed'
    );
  end if;

  select count(*), min(ordinal), max(ordinal)
  into v_item_count, v_min_ordinal, v_max_ordinal
  from public.lcia_scope_closure_artifact_write_set_items
  where write_set_id = v_write_set.id;
  if v_item_count <> v_write_set.expected_descriptor_count
     or v_min_ordinal is distinct from 1
     or v_max_ordinal is distinct from
       v_write_set.expected_descriptor_count then
    return public.lcia_scope_closure_error(
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
    return public.lcia_scope_closure_error(
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
       from public.lcia_scope_closure_artifact_write_set_items item
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
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_primary_roles_invalid',
      409,
      'Artifact descriptor invariants are invalid'
    );
  end if;

  if v_write_set.publication_mode = 'fresh' then
    if (
      select count(*)
      from public.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = v_write_set.id
        and item.artifact_role = 'closure_report'
        and item.artifact_type = 'closure_report_xlsx'
        and item.content_type =
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ) <> 1
       or (
         select count(*)
         from public.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'closure_report'
       ) <> 1
       or (
         select count(*)
         from public.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'closure_bundle'
           and item.artifact_type = 'closure_bundle'
           and item.content_type = 'application/json'
       ) <> 1
       or (
         select count(*)
         from public.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'closure_bundle'
       ) <> 1
       or (
         select count(*)
         from public.lcia_scope_closure_artifact_write_set_items item
         where item.write_set_id = v_write_set.id
           and item.artifact_role = 'complete_machine_result'
           and item.artifact_type = 'closure_complete_machine_result'
           and item.content_type =
             'application/vnd.tiangong.scope-closure-manifest+json'
       ) <> 1
       or not exists (
         select 1
         from public.lcia_scope_closure_artifact_write_set_items bundle
         join public.lcia_scope_closure_artifact_write_set_items manifest
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
      return public.lcia_scope_closure_error(
        'artifact_write_set_v2_primary_roles_invalid',
        409,
        'Fresh publication primary artifact roles are invalid'
      );
    end if;
  elsif v_item_count <> 1
     or not exists (
       select 1
       from public.lcia_scope_closure_artifact_write_set_items item
       where item.write_set_id = v_write_set.id
         and item.artifact_role = 'closure_report'
         and item.artifact_type = 'closure_report_xlsx'
         and item.content_type =
           'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
     ) then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_primary_roles_invalid',
      409,
      'Reused publication primary artifact role is invalid'
    );
  end if;

  update public.lcia_scope_closure_artifact_write_sets
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
$$;

create or replace function public.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
  p_write_set_id uuid,
  p_write_token uuid,
  p_worker_job_id uuid,
  p_worker_lease_token uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
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

create or replace function public.svc_lcia_scope_closure_artifact_write_set_fail_v2(
  p_write_set_id uuid,
  p_write_token uuid,
  p_worker_job_id uuid,
  p_worker_lease_token uuid,
  p_error text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_failure_invalid', 400, 'Failure reason is required'
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

  if v_write_set.status = 'cleanup_pending' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data',
        private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
    );
  end if;
  if v_write_set.status not in ('registration_open', 'staging') then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_finalizable',
      409,
      'Artifact write-set cannot be failed from its current state'
    );
  end if;

  update public.lcia_scope_closure_artifact_write_sets
  set status = 'cleanup_pending',
      failure_reason = left(trim(p_error), 1000),
      updated_at = now()
  where id = v_write_set.id
  returning * into v_write_set;

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', private.lcia_scope_closure_artifact_v2_status_json(v_write_set.id)
  );
end;
$$;

-- Keep the v1 inspect shape for v1 rows, but require the locator-free v2
-- status RPC for staged rows so ambiguous-response recovery cannot disclose
-- owner identity or registered object locators.
create or replace function public.svc_lcia_scope_closure_artifact_write_set_inspect(
  p_write_set_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_temp
as $$
declare
  v_data jsonb;
  v_contract_version text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select contract_version into v_contract_version
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id;
  if not found then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_contract_version is not null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_status_required',
      409,
      'Use the lease-fenced v2 status RPC'
    );
  end if;
  v_data := public.lcia_scope_closure_artifact_write_set_json(p_write_set_id);
  return jsonb_build_object('ok', true, 'data', v_data);
end;
$$;

-- Preserve byte-for-byte v1 finalize behavior for v1 rows.  A v2 row must use
-- the overload that carries the worker job and lease fence.
create or replace function public.svc_lcia_scope_closure_artifact_write_set_finalize(
  p_write_set_id uuid,
  p_write_token uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_report_id uuid;
  v_manifest_id uuid;
  v_bundle_id uuid;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null then
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
  if v_write_set.contract_version is not null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_fence_required',
      409,
      'Use the worker-lease-fenced v2 finalize RPC'
    );
  end if;
  if v_write_set.status = 'ready' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
    );
  end if;
  if v_write_set.status <> 'staging'
     or v_write_set.staging_expires_at <= now() then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_finalizable',
      409,
      'Artifact write-set is not finalizable'
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
    'data', public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_write_set_fail(
  p_write_set_id uuid,
  p_write_token uuid,
  p_error text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_failure_invalid', 400, 'Failure reason is required'
    );
  end if;
  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null then
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
  if v_write_set.contract_version is not null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_fence_required',
      409,
      'Use the worker-lease-fenced v2 failure RPC'
    );
  end if;
  if v_write_set.status <> 'staging' then
    return public.lcia_scope_closure_error(
      'artifact_write_set_token_invalid',
      409,
      'Artifact write-set token is not current'
    );
  end if;
  update public.lcia_scope_closure_artifact_write_sets
  set status = 'cleanup_pending',
      failure_reason = left(trim(p_error), 1000),
      updated_at = now()
  where id = v_write_set.id;
  return jsonb_build_object(
    'ok', true,
    'data', public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

-- Abandoned registration_open rows enter the same fenced cleanup path as
-- expired v1 staging rows.  A registration-open set has no authorized upload,
-- so deleting its planned locators is an idempotent zero-orphan safeguard.
create or replace function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  p_limit integer default 100,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_token uuid := gen_random_uuid();
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
  v_claimed_ids uuid[] := array[]::uuid[];
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit is null
     or p_limit not between 1 and 500
     or p_lease_seconds is null
     or p_lease_seconds not between 1 and 3600 then
    return public.lcia_scope_closure_error(
      'artifact_write_set_reconcile_invalid', 400, 'Invalid reconcile bounds'
    );
  end if;
  with candidates as (
    select id
    from public.lcia_scope_closure_artifact_write_sets
    where (
      status = 'cleanup_pending'
      or (
        status in ('registration_open', 'staging')
        and staging_expires_at <= now()
      )
    )
      and (
        reconcile_token is null
        or reconcile_expires_at is null
        or reconcile_expires_at <= now()
      )
    order by staging_expires_at, created_at, id
    for update skip locked
    limit p_limit
  ), claimed as (
    update public.lcia_scope_closure_artifact_write_sets write_set
    set status = 'cleanup_pending',
        reconcile_token = v_token,
        reconcile_claimed_at = now(),
        reconcile_expires_at = v_lease_expires_at,
        updated_at = now()
    from candidates
    where write_set.id = candidates.id
    returning write_set.id
  )
  select coalesce(array_agg(claimed.id order by claimed.id), array[]::uuid[])
  into v_claimed_ids
  from claimed;

  select coalesce(jsonb_agg(
    public.lcia_scope_closure_artifact_write_set_json(claimed_id)
    order by claimed_id
  ), '[]'::jsonb)
  into v_items
  from unnest(v_claimed_ids) claimed_id;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'reconcileToken', v_token,
    'leaseExpiresAt', v_lease_expires_at,
    'writeSets', v_items
  ));
end;
$$;

revoke all on function private.lcia_scope_closure_artifact_v2_lease_sha256(
  uuid
) from public, anon, authenticated, service_role;
revoke all on function private.lcia_scope_closure_artifact_v2_required_roles(
  text
) from public, anon, authenticated, service_role;
revoke all on function private.lcia_scope_closure_artifact_v2_descriptor_set(
  uuid
) from public, anon, authenticated, service_role;
revoke all on function private.lcia_scope_closure_artifact_v2_status_json(
  uuid
) from public, anon, authenticated, service_role;
revoke all on function private.lcia_scope_closure_artifact_v2_item_guard()
  from public, anon, authenticated, service_role;

revoke all on function public.svc_lcia_scope_closure_artifact_write_set_create_v2(
  uuid, uuid, uuid, uuid, text, integer, text, jsonb, integer, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
  uuid, uuid, uuid, uuid, uuid, jsonb
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_status_v2(
  uuid, uuid, uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_seal_v2(
  uuid, uuid, uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
  uuid, uuid, uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_fail_v2(
  uuid, uuid, uuid, uuid, text
) from public, anon, authenticated, service_role;

grant execute on function public.svc_lcia_scope_closure_artifact_write_set_create_v2(
  uuid, uuid, uuid, uuid, text, integer, text, jsonb, integer, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
  uuid, uuid, uuid, uuid, uuid, jsonb
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_status_v2(
  uuid, uuid, uuid, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_seal_v2(
  uuid, uuid, uuid, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
  uuid, uuid, uuid, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_fail_v2(
  uuid, uuid, uuid, uuid, text
) to service_role;

comment on table public.lcia_scope_closure_artifact_write_set_batches is
  'Idempotency receipts for bounded v2 descriptor registration. A batch UUID is replayable only with the same canonical request bytes.';
comment on column public.lcia_scope_closure_artifact_write_sets.contract_version is
  'NULL identifies the retained v1 adapter; v2 identifies lease-fenced staged registration.';
comment on column public.lcia_scope_closure_artifact_write_sets.worker_lease_token_sha256 is
  'One-way binding to the Worker lease generation. Raw lease authority is never persisted in the write-set registry or returned by status.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_create_v2(
  uuid, uuid, uuid, uuid, text, integer, text, jsonb, integer, uuid
) is
  'Service-only v2 header create. Binds closure check, Worker job/current lease generation, request UUID, exact count, canonical descriptor digest, required primary roles, and write-set fence while remaining upload-ineligible.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
  uuid, uuid, uuid, uuid, uuid, jsonb
) is
  'Service-only bounded registration of 1..500 exact descriptors. Batch UUID plus canonical bytes is idempotent; conflicting ordinal, client key, locator, or retry fails without partial insertion.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_status_v2(
  uuid, uuid, uuid, uuid
) is
  'Service-only current-lease status/readback for ambiguous responses. Returns fences, counts, batch ranges, and post-seal artifact IDs without owner identity, object locators, raw lease authority, or service credentials.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_seal_v2(
  uuid, uuid, uuid, uuid
) is
  'Service-only atomic seal. Under one write-set lock it verifies exact contiguous cardinality, canonical digest, primary roles, metadata and locator invariants, and current fences before the single registration_open-to-staging upload authorization.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
  uuid, uuid, uuid, uuid
) is
  'Service-only current-lease atomic finalize after object upload. Inserts every registered artifact as ready and updates the closure projection in the same transaction.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_fail_v2(
  uuid, uuid, uuid, uuid, text
) is
  'Service-only current-lease failure transition from registration_open or sealed staging into the existing fenced reconciliation/GC lifecycle.';
