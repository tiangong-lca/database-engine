-- Database-first scope-closure publication and fenced GC control.
--
-- Every object locator is registered before the Worker uploads bytes.  A
-- ready write-set is published atomically; stale/failed staging remains
-- service-reconcilable even when the uploader process disappears.

create or replace function public.lcia_scope_closure_artifact_lineage_eligible(
  p_check public.lcia_scope_closure_checks,
  p_artifact public.worker_job_artifacts,
  p_public_artifact_role text
) returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select case p_public_artifact_role
    when 'closure_report_xlsx' then
      (p_artifact).id = (p_check).report_artifact_id
      and (p_artifact).job_id = (p_check).worker_job_id
      and (p_artifact).artifact_role = 'closure_report'
      and (p_artifact).artifact_type = 'closure_report_xlsx'
    when 'closure_issue_manifest' then
      (p_artifact).id = (p_check).complete_machine_result_artifact_id
      and (p_artifact).artifact_role = 'complete_machine_result'
      and (p_artifact).artifact_type = 'closure_complete_machine_result'
      and (
        (p_artifact).job_id = (p_check).worker_job_id
        or exists (
          select 1
          from public.lcia_scope_closure_checks source_check
          where source_check.id = (p_check).reused_from_check_id
            and source_check.worker_job_id = (p_artifact).job_id
        )
      )
    else false
  end
$$;

create or replace function public.get_lcia_scope_closure_report_download(
  p_closure_check_id uuid,
  p_artifact_role text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
  v_artifact public.worker_job_artifacts%rowtype;
  v_artifact_id uuid;
  v_expected_media_type text;
  v_format text;
  v_filename text;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if p_artifact_role = 'closure_report_xlsx' then
    v_artifact_id := v_check.report_artifact_id;
    v_expected_media_type :=
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    v_format := 'xlsx';
    v_filename := 'scope-closure-' || v_check.id::text || '.xlsx';
  elsif p_artifact_role = 'closure_issue_manifest' then
    v_artifact_id := v_check.complete_machine_result_artifact_id;
    v_expected_media_type :=
      'application/vnd.tiangong.scope-closure-manifest+json';
    v_format := 'json';
    v_filename :=
      'scope-closure-' || v_check.id::text || '-manifest.json';
  else
    return public.lcia_scope_closure_error(
      'closure_artifact_role_invalid',
      400,
      'Unsupported closure artifact role'
    );
  end if;
  select * into v_artifact
  from public.worker_job_artifacts
  where id = v_artifact_id;
  if v_artifact.id is not null
     and public.lcia_scope_closure_artifact_lineage_eligible(
       v_check,
       v_artifact,
       p_artifact_role
     )
     and (
       v_artifact.lifecycle_state = 'expired'
       or (
         v_artifact.lifecycle_state <> 'deleted'
         and v_artifact.expires_at <= now()
       )
     ) then
    return public.lcia_scope_closure_error(
      'closure_report_expired', 410, 'Closure report has expired'
    );
  end if;
  if v_artifact.id is null
     or not public.lcia_scope_closure_artifact_lineage_eligible(
       v_check,
       v_artifact,
       p_artifact_role
     )
     or v_check.status not in ('passed', 'blocked')
     or v_artifact.lifecycle_state <> 'ready'
     or v_artifact.expires_at is null
     or v_artifact.expires_at <= now()
     or nullif(trim(v_artifact.storage_bucket), '') is null
     or nullif(trim(v_artifact.storage_path), '') is null
     or v_artifact.content_type is distinct from v_expected_media_type
     or v_artifact.byte_size is null
     or v_artifact.byte_size < 0
     or v_artifact.checksum_sha256 is null
     or v_artifact.checksum_sha256 !~ '^[a-f0-9]{64}$' then
    return public.lcia_scope_closure_error(
      'closure_report_unavailable', 404, 'Closure report is not available'
    );
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'artifactId', v_artifact.id,
    'artifactRole', p_artifact_role,
    'artifactState', 'ready',
    'filename', v_filename,
    'format', v_format,
    'mediaType', v_expected_media_type,
    'size', v_artifact.byte_size,
    'checksumSha256', v_artifact.checksum_sha256,
    'artifactExpiresAt', v_artifact.expires_at,
    'bucket', v_artifact.storage_bucket,
    'objectPath', v_artifact.storage_path
  ));
end;
$$;

create or replace function public.get_lcia_scope_closure_check(
  p_closure_check_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
  v_artifacts jsonb;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  select * into v_job
  from public.worker_jobs
  where id = v_check.worker_job_id;
  with public_roles (
    ordinal,
    artifact_role,
    artifact_id,
    filename,
    format,
    media_type
  ) as (
    values
      (
        1,
        'closure_report_xlsx'::text,
        v_check.report_artifact_id,
        'scope-closure-' || v_check.id::text || '.xlsx',
        'xlsx'::text,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'::text
      ),
      (
        2,
        'closure_issue_manifest'::text,
        v_check.complete_machine_result_artifact_id,
        'scope-closure-' || v_check.id::text || '-manifest.json',
        'json'::text,
        'application/vnd.tiangong.scope-closure-manifest+json'::text
      )
  ), summaries as (
    select
      role.ordinal,
      jsonb_build_object(
        'artifactRole', role.artifact_role,
        'artifactState', case
          when artifact.id is null
               and v_check.status in ('queued', 'running') then 'pending'
          when artifact.id is null then 'failed'
          when not public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then 'failed'
          when artifact.lifecycle_state = 'deleted' then 'deleted'
          when artifact.lifecycle_state = 'expired'
               or artifact.expires_at <= now() then 'expired'
          when v_check.status in ('passed', 'blocked')
               and artifact.lifecycle_state = 'ready'
               and nullif(trim(artifact.storage_bucket), '') is not null
               and nullif(trim(artifact.storage_path), '') is not null
               and artifact.content_type = role.media_type
               and artifact.byte_size is not null
               and artifact.byte_size >= 0
               and artifact.checksum_sha256 ~ '^[0-9a-f]{64}$'
               and artifact.expires_at is not null then 'ready'
          else 'failed'
        end,
        'filename', role.filename,
        'format', role.format,
        'mediaType', role.media_type,
        'size', case
          when public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then artifact.byte_size
        end,
        'checksumSha256', case
          when public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then artifact.checksum_sha256
        end,
        'artifactExpiresAt', case
          when public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then artifact.expires_at
        end
      ) as summary
    from public_roles role
    left join public.worker_job_artifacts artifact
      on artifact.id = role.artifact_id
  )
  select jsonb_agg(summary order by ordinal)
  into v_artifacts
  from summaries;
  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-check.v1',
      'closureCheckId', v_check.id,
      'runStatus', v_check.status,
      'scanCompleteness', coalesce(v_check.scan_completeness, 'unknown'),
      'certificateValidity', case
        when v_check.certificate_status = 'pending' then 'unavailable'
        else v_check.certificate_status
      end,
      'requestedScopeHash', v_check.requested_scope_hash,
      'effectiveScopeHash', v_check.effective_scope_hash,
      'policyFingerprint', v_check.policy_fingerprint,
      'dataSnapshotToken', v_check.data_snapshot_token,
      'blockerCodes', to_jsonb(v_check.blocker_codes),
      'summary', v_check.result_summary,
      'scanExecutionId', v_check.scan_execution_id,
      'reusedFromCheckId', v_check.reused_from_check_id,
      'createdAt', v_check.created_at,
      'updatedAt', v_check.updated_at,
      'finishedAt', v_check.finished_at,
      'workerJob', case
        when v_job.id is null then null
        else jsonb_strip_nulls(jsonb_build_object(
          'jobId', v_job.id,
          'status', v_job.status,
          'phase', v_job.phase,
          'progressFraction', v_job.progress,
          'errorCode', v_job.error_code,
          'blockerCodes', to_jsonb(v_job.blocker_codes),
          'createdAt', v_job.created_at,
          'updatedAt', v_job.updated_at,
          'finishedAt', v_job.finished_at
        ))
      end
    )) || jsonb_build_object('artifacts', v_artifacts)
  );
end;
$$;

create table public.lcia_scope_closure_artifact_write_sets (
  id uuid primary key default gen_random_uuid(),
  closure_check_id uuid not null
    references public.lcia_scope_closure_checks(id) on delete restrict,
  worker_job_id uuid not null
    references public.worker_jobs(id) on delete restrict,
  requested_by uuid not null,
  publication_mode text not null,
  reused_from_check_id uuid
    references public.lcia_scope_closure_checks(id) on delete restrict,
  idempotency_key text not null,
  request_sha256 text not null,
  status text not null default 'staging',
  write_token uuid not null default gen_random_uuid(),
  staging_expires_at timestamptz not null,
  reconcile_token uuid,
  reconcile_claimed_at timestamptz,
  reconcile_expires_at timestamptz,
  failure_reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  finalized_at timestamptz,
  cleaned_at timestamptz,
  constraint lcia_scope_closure_artifact_write_sets_status_check
    check (status in ('staging', 'ready', 'cleanup_pending', 'cleaned')),
  constraint lcia_scope_closure_artifact_write_sets_mode_check
    check (
      (publication_mode = 'fresh' and reused_from_check_id is null)
      or
      (publication_mode = 'reused' and reused_from_check_id is not null)
    ),
  constraint lcia_scope_closure_artifact_write_sets_key_check
    check (length(trim(idempotency_key)) between 1 and 200),
  constraint lcia_scope_closure_artifact_write_sets_hash_check
    check (request_sha256 ~ '^[a-f0-9]{64}$'),
  constraint lcia_scope_closure_artifact_write_sets_deadline_check
    check (staging_expires_at > created_at),
  constraint lcia_scope_closure_artifact_write_sets_owner_uidx
    unique (closure_check_id, idempotency_key)
);

create table public.lcia_scope_closure_artifact_write_set_items (
  id uuid primary key default gen_random_uuid(),
  write_set_id uuid not null
    references public.lcia_scope_closure_artifact_write_sets(id)
    on delete restrict,
  ordinal integer not null,
  client_key text not null,
  artifact_type text not null,
  artifact_role text not null,
  storage_bucket text not null,
  storage_path text not null,
  content_type text not null,
  byte_size bigint not null,
  checksum_sha256 text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint lcia_scope_closure_artifact_write_set_items_ordinal_check
    check (ordinal > 0),
  constraint lcia_scope_closure_artifact_write_set_items_client_key_check
    check (length(trim(client_key)) between 1 and 500),
  constraint lcia_scope_closure_artifact_write_set_items_locator_check
    check (
      length(trim(storage_bucket)) > 0
      and length(trim(storage_path)) > 0
    ),
  constraint lcia_scope_closure_artifact_write_set_items_size_check
    check (byte_size >= 0),
  constraint lcia_scope_closure_artifact_write_set_items_checksum_check
    check (checksum_sha256 ~ '^[a-f0-9]{64}$'),
  constraint lcia_scope_closure_artifact_write_set_items_metadata_check
    check (jsonb_typeof(metadata) = 'object'),
  constraint lcia_scope_closure_artifact_write_set_items_role_check
    check (
      artifact_role is not distinct from
        public.lcia_scope_closure_artifact_role(artifact_type)
    ),
  constraint lcia_scope_closure_artifact_write_set_items_ordinal_uidx
    unique (write_set_id, ordinal),
  constraint lcia_scope_closure_artifact_write_set_items_client_uidx
    unique (write_set_id, client_key),
  constraint lcia_scope_closure_artifact_write_set_items_locator_uidx
    unique (write_set_id, storage_bucket, storage_path)
);

create index lcia_scope_closure_artifact_write_sets_reconcile_idx
  on public.lcia_scope_closure_artifact_write_sets (
    status,
    staging_expires_at,
    reconcile_expires_at,
    created_at,
    id
  );

create unique index lcia_scope_closure_artifact_write_sets_active_uidx
  on public.lcia_scope_closure_artifact_write_sets (closure_check_id)
  where status in ('staging', 'ready');

alter table public.lcia_scope_closure_artifact_write_sets enable row level security;
alter table public.lcia_scope_closure_artifact_write_set_items
  enable row level security;
revoke all on public.lcia_scope_closure_artifact_write_sets
  from public, anon, authenticated, service_role;
revoke all on public.lcia_scope_closure_artifact_write_set_items
  from public, anon, authenticated, service_role;

create or replace function public.lcia_scope_closure_artifact_write_set_json(
  p_write_set_id uuid
) returns jsonb
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'writeSetId', write_set.id,
    'closureCheckId', write_set.closure_check_id,
    'workerJobId', write_set.worker_job_id,
    'requestedBy', write_set.requested_by,
    'publicationMode', write_set.publication_mode,
    'reusedFromCheckId', write_set.reused_from_check_id,
    'status', write_set.status,
    'writeToken', write_set.write_token,
    'stagingExpiresAt', write_set.staging_expires_at,
    'reconcileToken', write_set.reconcile_token,
    'reconcileLeaseExpiresAt', write_set.reconcile_expires_at,
    'failureReason', write_set.failure_reason,
    'createdAt', write_set.created_at,
    'updatedAt', write_set.updated_at,
    'finalizedAt', write_set.finalized_at,
    'cleanedAt', write_set.cleaned_at,
    'artifactMap', coalesce((
      select jsonb_object_agg(item.client_key, item.id order by item.client_key)
      from public.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ), '{}'::jsonb),
    'items', coalesce((
      select jsonb_agg(jsonb_build_object(
        'artifactId', item.id,
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
      ) order by item.ordinal)
      from public.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ), '[]'::jsonb)
  )
  from public.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = p_write_set_id
$$;

create or replace function public.svc_lcia_scope_closure_artifact_write_set_create(
  p_closure_check_id uuid,
  p_idempotency_key text,
  p_items jsonb,
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
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
  v_request_sha256 text;
  v_item_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_idempotency_key, '')), '') is null
     or length(trim(p_idempotency_key)) > 200
     or jsonb_typeof(p_items) <> 'array'
     or jsonb_array_length(p_items) not between 1 and 500
     or p_staging_seconds is null
     or p_staging_seconds not between 1 and 86400 then
    return public.lcia_scope_closure_error(
      'artifact_write_set_invalid', 400, 'Invalid artifact write-set request'
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
  if v_check.status in ('failed', 'cancelled') then
    return public.lcia_scope_closure_error(
      'artifact_write_set_unavailable', 409, 'Closure check cannot publish artifacts'
    );
  end if;

  v_request_sha256 := public.lcia_scope_closure_sha256(
    jsonb_build_object(
      'items', p_items,
      'publicationMode',
        case when p_reused_from_check_id is null then 'fresh' else 'reused' end,
      'reusedFromCheckId', p_reused_from_check_id
    )
  );
  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where closure_check_id = p_closure_check_id
    and idempotency_key = trim(p_idempotency_key);
  if v_write_set.id is not null then
    if v_write_set.request_sha256 <> v_request_sha256 then
      return public.lcia_scope_closure_error(
        'artifact_write_set_idempotency_conflict',
        409,
        'Artifact write-set idempotency key conflicts'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
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
       or public.lcia_scope_closure_artifact_role(
            item->>'artifactType'
          ) is distinct from item->>'artifactRole'
  ) then
    return public.lcia_scope_closure_error(
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
    return public.lcia_scope_closure_error(
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
      return public.lcia_scope_closure_error(
        'artifact_write_set_invalid',
        400,
        'Fresh publication requires report, manifest, and bundle roles'
      );
    end if;
  else
    if jsonb_array_length(p_items) <> 1
       or p_items #>> '{0,artifactRole}' <> 'closure_report'
       or p_items #>> '{0,artifactType}' <> 'closure_report_xlsx' then
      return public.lcia_scope_closure_error(
        'artifact_write_set_invalid',
        400,
        'Reused publication accepts exactly one XLSX report'
      );
    end if;
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
    update public.lcia_scope_closure_checks
    set reused_from_check_id = v_source_check.id,
        complete_machine_result_artifact_id =
          v_source_check.complete_machine_result_artifact_id,
        closure_bundle_artifact_id = v_source_check.closure_bundle_artifact_id,
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
    'data', public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

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
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  v_data := public.lcia_scope_closure_artifact_write_set_json(p_write_set_id);
  if v_data is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  return jsonb_build_object('ok', true, 'data', v_data);
end;
$$;

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
      'artifact_write_set_token_invalid', 409, 'Artifact write-set token is not current'
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
      'artifact_write_set_not_finalizable', 409, 'Artifact write-set is not finalizable'
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
  if v_write_set.write_token is distinct from p_write_token
     or v_write_set.status <> 'staging' then
    return public.lcia_scope_closure_error(
      'artifact_write_set_token_invalid', 409, 'Artifact write-set token is not current'
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
      or (status = 'staging' and staging_expires_at <= now())
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

  -- Render in a new command after the data-modifying CTE. Calling the
  -- projection helper inside the CTE statement observes the pre-update
  -- command snapshot and can return status=staging for a persisted
  -- cleanup_pending claim.
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

create or replace function public.svc_lcia_scope_closure_artifact_write_set_reconcile_complete(
  p_write_set_id uuid,
  p_reconcile_token uuid
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
  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_write_set.status = 'cleaned'
     and v_write_set.reconcile_token = p_reconcile_token then
    return jsonb_build_object('ok', true, 'reused', true);
  end if;
  if v_write_set.status <> 'cleanup_pending'
     or v_write_set.reconcile_token is distinct from p_reconcile_token
     or v_write_set.reconcile_expires_at is null
     or v_write_set.reconcile_expires_at < now() then
    return public.lcia_scope_closure_error(
      'artifact_write_set_reconcile_invalid',
      409,
      'Artifact write-set reconcile claim is not current'
    );
  end if;
  update public.lcia_scope_closure_artifact_write_sets
  set status = 'cleaned',
      cleaned_at = now(),
      updated_at = now(),
      reconcile_expires_at = null
  where id = v_write_set.id;
  return jsonb_build_object('ok', true, 'reused', false, 'data',
    public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_gc_preview(
  p_limit integer default 100
) returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_temp
as $$
declare
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit is null or p_limit not between 1 and 500 then
    return public.lcia_scope_closure_error(
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
    from public.worker_job_artifacts
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

create or replace function public.svc_lcia_scope_closure_artifact_gc_claim(
  p_limit integer default 100,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_claim_token uuid := gen_random_uuid();
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
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
      'invalid_gc_claim', 400, 'Invalid GC claim bounds'
    );
  end if;
  with candidates as (
    select id
    from public.worker_job_artifacts
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
    update public.worker_job_artifacts artifact
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

create or replace function public.svc_lcia_scope_closure_artifact_gc_renew(
  p_claim_token uuid,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
  v_artifact_ids jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_claim_token is null
     or p_lease_seconds is null
     or p_lease_seconds not between 1 and 3600 then
    return public.lcia_scope_closure_error(
      'invalid_gc_renewal', 400, 'Invalid GC renewal request'
    );
  end if;
  with renewed as (
    update public.worker_job_artifacts artifact
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
    return public.lcia_scope_closure_error(
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

-- Re-apply current guards on existing Preview branches where earlier migration
-- files were already recorded.
create or replace function public.lcia_scope_closure_artifact_lifecycle_guard()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_old_expected_role text := case
    when tg_op = 'UPDATE'
    then public.lcia_scope_closure_artifact_role(old.artifact_type)
  end;
  v_new_expected_role text :=
    public.lcia_scope_closure_artifact_role(new.artifact_type);
begin
  if tg_op = 'INSERT'
     and v_new_expected_role is null
     and new.artifact_role is null then
    return new;
  end if;
  if tg_op = 'UPDATE'
     and v_old_expected_role is null
     and v_new_expected_role is null
     and old.artifact_role is null
     and new.artifact_role is null then
    return new;
  end if;
  if tg_op = 'INSERT' then
    new.artifact_role := coalesce(new.artifact_role, v_new_expected_role);
    new.expires_at := coalesce(
      new.expires_at,
      coalesce(new.created_at, now()) + interval '7 days'
    );
    new.lifecycle_state := coalesce(
      new.lifecycle_state,
      case when new.expires_at <= now() then 'expired' else 'ready' end
    );
  else
    if new.artifact_type is distinct from old.artifact_type
       or new.artifact_role is distinct from old.artifact_role
       or new.created_at is distinct from old.created_at
       or new.expires_at is distinct from old.expires_at then
      raise exception 'scope_closure_artifact_identity_or_expiry_is_immutable'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'deleted'
       and new.lifecycle_state is distinct from 'deleted' then
      raise exception 'scope_closure_artifact_deleted_is_terminal'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'expired'
       and new.lifecycle_state not in ('expired', 'deleted') then
      raise exception 'scope_closure_artifact_cannot_return_to_ready'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'ready'
       and new.lifecycle_state not in ('ready', 'expired') then
      raise exception 'scope_closure_artifact_invalid_transition'
        using errcode = '23514';
    end if;
  end if;
  if v_new_expected_role is null
     or new.artifact_role is distinct from v_new_expected_role
     or new.expires_at is null
     or new.expires_at > coalesce(new.created_at, now()) + interval '7 days'
     or new.lifecycle_state is null then
    raise exception 'invalid_scope_closure_artifact_lifecycle'
      using errcode = '23514';
  end if;
  if new.lifecycle_state = 'ready'
     and (
       nullif(trim(new.storage_bucket), '') is null
       or nullif(trim(new.storage_path), '') is null
       or new.content_type is null
       or new.byte_size is null
       or new.checksum_sha256 is null
     ) then
    raise exception 'scope_closure_artifact_ready_metadata_incomplete'
      using errcode = '23514';
  end if;
  if new.lifecycle_state = 'deleted' then
    new.deleted_at := coalesce(new.deleted_at, now());
    new.storage_bucket := null;
    new.storage_path := null;
  end if;
  return new;
end;
$$;

update public.worker_job_artifacts
set artifact_role = public.lcia_scope_closure_artifact_role(artifact_type)
where artifact_role is distinct from
  public.lcia_scope_closure_artifact_role(artifact_type);

update public.worker_job_artifacts
set lifecycle_state = 'expired'
where public.lcia_scope_closure_artifact_role(artifact_type) is not null
  and lifecycle_state = 'ready'
  and expires_at <= now();

alter table public.worker_job_artifacts
  drop constraint if exists worker_job_artifacts_artifact_role_check,
  add constraint worker_job_artifacts_artifact_role_check
    check (
      artifact_role is not distinct from
        public.lcia_scope_closure_artifact_role(artifact_type)
    );

with downgraded as (
  update public.lcia_scope_closure_checks closure_check
  set certificate_status = 'stale',
      updated_at = now()
  where closure_check.certificate_status = 'valid'
    and not public.lcia_scope_closure_evidence_usable(closure_check)
  returning closure_check.id
)
insert into public.lcia_scope_closure_certificate_events (
  closure_check_id,
  certificate_status,
  reason,
  created_by
)
select
  id,
  'stale',
  'artifact_retention_reconciliation_evidence_expired_or_incomplete',
  null
from downgraded;

create or replace function public.lcia_scope_closure_certificate_validity_guard()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_report public.worker_job_artifacts%rowtype;
  v_machine_result public.worker_job_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
begin
  if new.certificate_status <> 'valid' then
    return new;
  end if;
  select * into v_report
  from public.worker_job_artifacts where id = new.report_artifact_id;
  select * into v_bundle
  from public.worker_job_artifacts where id = new.closure_bundle_artifact_id;
  select * into v_machine_result
  from public.worker_job_artifacts
  where id = coalesce(
    new.complete_machine_result_artifact_id,
    case
      when coalesce(
        v_bundle.metadata->>'completeMachineResultArtifactId', ''
      ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      then (v_bundle.metadata->>'completeMachineResultArtifactId')::uuid
    end
  );
  if v_report.id is null
     or v_report.job_id <> new.worker_job_id
     or v_report.artifact_role <> 'closure_report'
     or v_report.lifecycle_state <> 'ready'
     or v_machine_result.id is null
     or (
       v_machine_result.job_id <> new.worker_job_id
       and not exists (
         select 1
         from public.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_machine_result.job_id
       )
     )
     or v_machine_result.artifact_role <> 'complete_machine_result'
     or v_machine_result.lifecycle_state <> 'ready'
     or v_bundle.id is null
     or (
       v_bundle.job_id <> new.worker_job_id
       and not exists (
         select 1
         from public.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_bundle.job_id
       )
     )
     or v_bundle.artifact_role <> 'closure_bundle'
     or v_bundle.lifecycle_state <> 'ready'
     or v_report.expires_at is null
     or v_machine_result.expires_at is null
     or v_bundle.expires_at is null then
    raise exception 'closure_certificate_evidence_lifecycle_invalid'
      using errcode = '23514';
  end if;
  new.complete_machine_result_artifact_id := v_machine_result.id;
  new.valid_until := least(
    v_report.expires_at,
    v_machine_result.expires_at,
    v_bundle.expires_at
  );
  if new.valid_until <= coalesce(new.finished_at, now())
     or new.valid_until <= now() then
    raise exception 'closure_certificate_evidence_already_expired'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists lcia_scope_closure_checks_certificate_validity
  on public.lcia_scope_closure_checks;
create trigger lcia_scope_closure_checks_certificate_validity
before insert or update on public.lcia_scope_closure_checks
for each row execute function public.lcia_scope_closure_certificate_validity_guard();

revoke all on function public.lcia_scope_closure_artifact_write_set_json(uuid)
  from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_artifact_lineage_eligible(
  public.lcia_scope_closure_checks,
  public.worker_job_artifacts,
  text
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_create(
  uuid, text, jsonb, integer, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_inspect(
  uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_finalize(
  uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_fail(
  uuid, uuid, text
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  integer, integer
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_write_set_reconcile_complete(
  uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_preview(integer)
  from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_renew(
  uuid, integer
) from public, anon, authenticated, service_role;

grant execute on function public.svc_lcia_scope_closure_artifact_write_set_create(
  uuid, text, jsonb, integer, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_inspect(
  uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_finalize(
  uuid, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_fail(
  uuid, uuid, text
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  integer, integer
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_reconcile_complete(
  uuid, uuid
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_preview(integer)
  to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_renew(
  uuid, integer
) to service_role;

comment on table public.lcia_scope_closure_artifact_write_sets is
  'DB-first scope-closure publication registry. Every possible uploaded object is registered before upload and becomes ready only through atomic finalize.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_create(
  uuid, text, jsonb, integer, uuid
) is
  'Service-only DB-first publication create. NULL reused source requires the full fresh shape; a source check UUID requires exactly one XLSX report, freezes source manifest/bundle lineage, and returns artifactMap as the authoritative clientKey-to-artifactId mapping.';
comment on function public.svc_lcia_scope_closure_artifact_write_set_finalize(
  uuid, uuid
) is
  'Service-only fenced atomic finalize. Fresh mode resolves completeMachineResultClientKey to completeMachineResultArtifactId and publishes all items; reused mode publishes only the new report and preserves source evidence bindings.';
comment on function public.svc_lcia_scope_closure_artifact_gc_preview(integer) is
  'Service-only, strictly non-mutating ordered preview of immediately claimable scope-closure GC candidates.';
comment on function public.svc_lcia_scope_closure_artifact_gc_renew(uuid, integer) is
  'Service-only fenced heartbeat that extends only a current unexpired GC claim token.';
