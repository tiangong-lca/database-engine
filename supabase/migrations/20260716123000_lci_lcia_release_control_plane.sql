-- Durable control-plane facts for canonical Unit Process and standalone
-- LifecycleModel + Result releases. Calculation and package materialization
-- happen outside the database; this migration owns authorization, exact-hash
-- state transitions, approval, publication, readback, and public projections.

create table if not exists public.lca_release_runs (
  id uuid primary key,
  release_version text not null,
  scope_mode text not null default 'global_eligible',
  selection_manifest_hash text not null,
  input_manifest_hash text not null,
  calculation_bundle_hash text not null,
  calculation_bundle_ref jsonb not null,
  profile_lock_hash text not null,
  publish_plan_hash text not null,
  publish_plan jsonb not null,
  artifact_set_hash text not null,
  release_manifest_hash text,
  release_manifest jsonb,
  status text not null default 'prepared',
  idempotency_key text not null,
  request_hash text not null,
  created_by uuid not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  artifacts_finalized_at timestamptz,
  approved_at timestamptz,
  published_at timestamptz,
  readback_verified_at timestamptz,
  readback_receipt jsonb,
  constraint lca_release_runs_release_version_chk check (
    release_version ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
  ),
  constraint lca_release_runs_scope_chk check (scope_mode = 'global_eligible'),
  constraint lca_release_runs_hashes_chk check (
    selection_manifest_hash ~ '^[0-9a-f]{64}$'
    and input_manifest_hash ~ '^[0-9a-f]{64}$'
    and calculation_bundle_hash ~ '^[0-9a-f]{64}$'
    and profile_lock_hash ~ '^[0-9a-f]{64}$'
    and publish_plan_hash ~ '^[0-9a-f]{64}$'
    and artifact_set_hash ~ '^[0-9a-f]{64}$'
    and request_hash ~ '^[0-9a-f]{64}$'
    and (release_manifest_hash is null or release_manifest_hash ~ '^[0-9a-f]{64}$')
  ),
  constraint lca_release_runs_json_chk check (
    jsonb_typeof(calculation_bundle_ref) = 'object'
    and jsonb_typeof(publish_plan) = 'object'
    and (release_manifest is null or jsonb_typeof(release_manifest) = 'object')
    and (readback_receipt is null or jsonb_typeof(readback_receipt) = 'object')
  ),
  constraint lca_release_runs_status_chk check (
    status in (
      'prepared',
      'ready_for_approval',
      'approved',
      'published',
      'readback_verified',
      'unpublished',
      'failed',
      'abandoned'
    )
  )
);

create unique index if not exists lca_release_runs_version_uidx
  on public.lca_release_runs (release_version);

create unique index if not exists lca_release_runs_actor_idempotency_uidx
  on public.lca_release_runs (created_by, idempotency_key);

create index if not exists lca_release_runs_status_created_idx
  on public.lca_release_runs (status, created_at desc);

create table if not exists public.lca_release_dataset_versions (
  id bigint generated always as identity primary key,
  release_run_id uuid not null references public.lca_release_runs(id) on delete restrict,
  dataset_type text not null,
  dataset_role text not null,
  dataset_uuid uuid not null,
  dataset_version text not null,
  version_significant_hash text not null,
  semantic_hash text not null,
  canonical_content_hash text not null,
  artifact_ref jsonb not null,
  created_at timestamptz not null default now(),
  constraint lca_release_dataset_type_chk check (
    dataset_type in (
      'process', 'lifecyclemodel', 'flow', 'flowproperty', 'unitgroup',
      'lciamethod', 'source', 'contact'
    )
  ),
  constraint lca_release_dataset_role_chk check (
    dataset_role in ('unit_process', 'result_process', 'lifecycle_model', 'support')
  ),
  constraint lca_release_dataset_version_chk check (
    dataset_version ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
  ),
  constraint lca_release_dataset_hashes_chk check (
    version_significant_hash ~ '^[0-9a-f]{64}$'
    and semantic_hash ~ '^[0-9a-f]{64}$'
    and canonical_content_hash ~ '^[0-9a-f]{64}$'
  ),
  constraint lca_release_dataset_artifact_chk check (
    jsonb_typeof(artifact_ref) = 'object'
  ),
  constraint lca_release_dataset_key_unique unique (
    release_run_id, dataset_type, dataset_uuid, dataset_version
  )
);

create index if not exists lca_release_dataset_lookup_idx
  on public.lca_release_dataset_versions (dataset_type, dataset_uuid, dataset_version);

create table if not exists public.lca_release_artifacts (
  id uuid primary key default gen_random_uuid(),
  release_run_id uuid not null references public.lca_release_runs(id) on delete restrict,
  profile_id text not null,
  artifact_format text not null,
  storage_bucket text not null,
  object_key text not null,
  sha256 text not null,
  byte_size bigint not null,
  media_type text not null,
  closure_hash text not null,
  verified_at timestamptz not null,
  pinned boolean not null default false,
  published_at timestamptz,
  created_at timestamptz not null default now(),
  constraint lca_release_artifacts_profile_chk check (
    profile_id in (
      'unit-process-full-closure.v1',
      'standalone-lifecyclemodel-result-full-closure.v1'
    )
  ),
  constraint lca_release_artifacts_format_chk check (
    artifact_format in ('tidas', 'ilcd')
  ),
  constraint lca_release_artifacts_storage_chk check (
    length(trim(storage_bucket)) > 0 and length(trim(object_key)) > 0
  ),
  constraint lca_release_artifacts_sha_chk check (
    sha256 ~ '^[0-9a-f]{64}$' and closure_hash ~ '^[0-9a-f]{64}$'
  ),
  constraint lca_release_artifacts_size_chk check (byte_size >= 0),
  constraint lca_release_artifacts_profile_format_unique unique (
    release_run_id, profile_id, artifact_format
  ),
  constraint lca_release_artifacts_object_unique unique (storage_bucket, object_key)
);

create index if not exists lca_release_artifacts_run_idx
  on public.lca_release_artifacts (release_run_id, profile_id, artifact_format);

create table if not exists public.lca_release_approvals (
  id uuid primary key default gen_random_uuid(),
  release_run_id uuid not null references public.lca_release_runs(id) on delete restrict,
  publish_plan_hash text not null,
  approval_hash text not null,
  status text not null default 'approved',
  approved_by uuid not null,
  approved_at timestamptz not null,
  expires_at timestamptz not null,
  reason text,
  consumed_by uuid,
  consumed_at timestamptz,
  revoked_at timestamptz,
  audit_correlation jsonb not null default '{}'::jsonb,
  constraint lca_release_approvals_hash_chk check (
    publish_plan_hash ~ '^[0-9a-f]{64}$' and approval_hash ~ '^[0-9a-f]{64}$'
  ),
  constraint lca_release_approvals_status_chk check (
    status in ('approved', 'consumed', 'expired', 'revoked')
  ),
  constraint lca_release_approvals_expiry_chk check (expires_at > approved_at),
  constraint lca_release_approvals_audit_chk check (
    jsonb_typeof(audit_correlation) = 'object'
  )
);

create unique index if not exists lca_release_approvals_active_uidx
  on public.lca_release_approvals (release_run_id)
  where status = 'approved';

create index if not exists lca_release_approvals_run_idx
  on public.lca_release_approvals (release_run_id, approved_at desc);

create table if not exists public.lca_release_publications (
  id uuid primary key default gen_random_uuid(),
  release_run_id uuid not null unique references public.lca_release_runs(id) on delete restrict,
  release_version text not null unique,
  publication_series_key text not null default 'global',
  publication_channel text not null default 'public',
  visibility_scope text not null default 'public',
  status text not null default 'current',
  is_current boolean not null default true,
  approval_id uuid not null references public.lca_release_approvals(id) on delete restrict,
  approval_hash text not null,
  publish_plan_hash text not null,
  release_manifest_hash text not null,
  artifact_set_hash text not null,
  approved_by uuid not null,
  executed_by uuid not null,
  credential_fingerprint text not null,
  idempotency_key text not null,
  published_at timestamptz not null,
  superseded_by uuid references public.lca_release_publications(id) on delete restrict,
  superseded_at timestamptz,
  unpublished_by uuid,
  unpublished_at timestamptz,
  reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint lca_release_publications_scope_chk check (
    publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
  ),
  constraint lca_release_publications_status_chk check (
    status in ('current', 'superseded', 'unpublished')
  ),
  constraint lca_release_publications_hashes_chk check (
    approval_hash ~ '^[0-9a-f]{64}$'
    and publish_plan_hash ~ '^[0-9a-f]{64}$'
    and release_manifest_hash ~ '^[0-9a-f]{64}$'
    and artifact_set_hash ~ '^[0-9a-f]{64}$'
    and credential_fingerprint ~ '^[0-9a-f]{64}$'
  )
);

create unique index if not exists lca_release_publications_current_uidx
  on public.lca_release_publications (
    publication_series_key, publication_channel, visibility_scope
  )
  where is_current = true;

create unique index if not exists lca_release_publications_actor_idempotency_uidx
  on public.lca_release_publications (executed_by, idempotency_key);

create index if not exists lca_release_publications_approval_idx
  on public.lca_release_publications (approval_id);

create index if not exists lca_release_publications_superseded_by_idx
  on public.lca_release_publications (superseded_by)
  where superseded_by is not null;

alter table public.lca_release_runs enable row level security;
alter table public.lca_release_dataset_versions enable row level security;
alter table public.lca_release_artifacts enable row level security;
alter table public.lca_release_approvals enable row level security;
alter table public.lca_release_publications enable row level security;

revoke all on table public.lca_release_runs from public, anon, authenticated;
revoke all on table public.lca_release_dataset_versions from public, anon, authenticated;
revoke all on table public.lca_release_artifacts from public, anon, authenticated;
revoke all on table public.lca_release_approvals from public, anon, authenticated;
revoke all on table public.lca_release_publications from public, anon, authenticated;

revoke all on table public.lca_release_runs from service_role;
revoke all on table public.lca_release_dataset_versions from service_role;
revoke all on table public.lca_release_artifacts from service_role;
revoke all on table public.lca_release_approvals from service_role;
revoke all on table public.lca_release_publications from service_role;
revoke all on sequence public.lca_release_dataset_versions_id_seq from service_role;

create or replace function public.lca_release_guard_run_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if old.id is distinct from new.id
     or old.release_version is distinct from new.release_version
     or old.scope_mode is distinct from new.scope_mode
     or old.selection_manifest_hash is distinct from new.selection_manifest_hash
     or old.input_manifest_hash is distinct from new.input_manifest_hash
     or old.calculation_bundle_hash is distinct from new.calculation_bundle_hash
     or old.calculation_bundle_ref is distinct from new.calculation_bundle_ref
     or old.profile_lock_hash is distinct from new.profile_lock_hash
     or old.publish_plan_hash is distinct from new.publish_plan_hash
     or old.publish_plan is distinct from new.publish_plan
     or old.artifact_set_hash is distinct from new.artifact_set_hash
     or old.idempotency_key is distinct from new.idempotency_key
     or old.request_hash is distinct from new.request_hash
     or old.created_by is distinct from new.created_by
     or old.created_at is distinct from new.created_at then
    raise exception 'lca_release_run_immutable_content'
      using errcode = '23514';
  end if;

  if old.release_manifest_hash is not null
     and (
       old.release_manifest_hash is distinct from new.release_manifest_hash
       or old.release_manifest is distinct from new.release_manifest
       or old.artifacts_finalized_at is distinct from new.artifacts_finalized_at
     ) then
    raise exception 'lca_release_manifest_immutable'
      using errcode = '23514';
  end if;

  if old.status is distinct from new.status
     and not (
       (old.status = 'prepared' and new.status in ('ready_for_approval', 'failed', 'abandoned'))
       or (old.status = 'ready_for_approval' and new.status in ('approved', 'failed', 'abandoned'))
       or (old.status = 'approved' and new.status in ('published', 'failed', 'abandoned'))
       or (old.status = 'published' and new.status in ('readback_verified', 'unpublished'))
       or (old.status = 'readback_verified' and new.status = 'unpublished')
     ) then
    raise exception 'lca_release_state_transition_invalid:%->%', old.status, new.status
      using errcode = '23514';
  end if;

  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists lca_release_runs_guard_update on public.lca_release_runs;
create trigger lca_release_runs_guard_update
before update on public.lca_release_runs
for each row execute function public.lca_release_guard_run_update();

create or replace function public.lca_release_guard_dataset_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  raise exception 'lca_release_dataset_index_immutable'
    using errcode = '23514';
end;
$$;

drop trigger if exists lca_release_dataset_versions_guard_update
  on public.lca_release_dataset_versions;
create trigger lca_release_dataset_versions_guard_update
before update on public.lca_release_dataset_versions
for each row execute function public.lca_release_guard_dataset_update();

create or replace function public.lca_release_guard_artifact_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if old.id is distinct from new.id
     or old.release_run_id is distinct from new.release_run_id
     or old.profile_id is distinct from new.profile_id
     or old.artifact_format is distinct from new.artifact_format
     or old.storage_bucket is distinct from new.storage_bucket
     or old.object_key is distinct from new.object_key
     or old.sha256 is distinct from new.sha256
     or old.byte_size is distinct from new.byte_size
     or old.media_type is distinct from new.media_type
     or old.closure_hash is distinct from new.closure_hash
     or old.verified_at is distinct from new.verified_at
     or old.created_at is distinct from new.created_at
     or (old.pinned and not new.pinned)
     or (old.published_at is not null and old.published_at is distinct from new.published_at) then
    raise exception 'lca_release_artifact_immutable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists lca_release_artifacts_guard_update
  on public.lca_release_artifacts;
create trigger lca_release_artifacts_guard_update
before update on public.lca_release_artifacts
for each row execute function public.lca_release_guard_artifact_update();

create or replace function public.lca_release_guard_approval_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if old.id is distinct from new.id
     or old.release_run_id is distinct from new.release_run_id
     or old.publish_plan_hash is distinct from new.publish_plan_hash
     or old.approval_hash is distinct from new.approval_hash
     or old.approved_by is distinct from new.approved_by
     or old.approved_at is distinct from new.approved_at
     or old.expires_at is distinct from new.expires_at
     or old.reason is distinct from new.reason
     or old.audit_correlation is distinct from new.audit_correlation
     or (old.status <> 'approved' and old.status is distinct from new.status)
     or (old.status = 'approved' and new.status not in ('approved', 'consumed', 'expired', 'revoked')) then
    raise exception 'lca_release_approval_immutable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists lca_release_approvals_guard_update
  on public.lca_release_approvals;
create trigger lca_release_approvals_guard_update
before update on public.lca_release_approvals
for each row execute function public.lca_release_guard_approval_update();

create or replace function public.lca_release_guard_publication_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if old.id is distinct from new.id
     or old.release_run_id is distinct from new.release_run_id
     or old.release_version is distinct from new.release_version
     or old.publication_series_key is distinct from new.publication_series_key
     or old.publication_channel is distinct from new.publication_channel
     or old.visibility_scope is distinct from new.visibility_scope
     or old.approval_id is distinct from new.approval_id
     or old.approval_hash is distinct from new.approval_hash
     or old.publish_plan_hash is distinct from new.publish_plan_hash
     or old.release_manifest_hash is distinct from new.release_manifest_hash
     or old.artifact_set_hash is distinct from new.artifact_set_hash
     or old.approved_by is distinct from new.approved_by
     or old.executed_by is distinct from new.executed_by
     or old.credential_fingerprint is distinct from new.credential_fingerprint
     or old.idempotency_key is distinct from new.idempotency_key
     or old.published_at is distinct from new.published_at
     or old.created_at is distinct from new.created_at
     or (not old.is_current and new.is_current)
     or (old.status <> 'current' and old.status is distinct from new.status)
     or (old.status = 'current' and new.status not in ('current', 'superseded', 'unpublished')) then
    raise exception 'lca_release_publication_immutable'
      using errcode = '23514';
  end if;
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists lca_release_publications_guard_update
  on public.lca_release_publications;
create trigger lca_release_publications_guard_update
before update on public.lca_release_publications
for each row execute function public.lca_release_guard_publication_update();

create or replace function public.lca_release_error(
  p_code text,
  p_status integer,
  p_message text
)
returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'ok', false,
    'code', p_code,
    'status', p_status,
    'message', p_message
  )
$$;

create or replace function public.lca_release_is_manager()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select auth.uid() is not null
     and public.policy_is_current_user_in_roles(
       '00000000-0000-0000-0000-000000000000'::uuid,
       array['data_product_manager']
     )
$$;

create or replace function public.assert_lca_release_manager()
returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error(
      'not_data_product_manager',
      403,
      'Data product manager role is required'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'userId', v_actor,
      'role', 'data_product_manager'
    )
  );
end;
$$;

create or replace function public.lca_release_is_service_request()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select current_user = 'service_role'
      or coalesce(util.is_service_request(), false)
$$;

create or replace function public.cmd_lca_release_prepare(
  p_release_run_id uuid,
  p_release_version text,
  p_selection_manifest_hash text,
  p_input_manifest_hash text,
  p_calculation_bundle_ref jsonb,
  p_calculation_bundle_hash text,
  p_profile_lock_hash text,
  p_publish_plan jsonb,
  p_publish_plan_hash text,
  p_idempotency_key text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_existing public.lca_release_runs%rowtype;
  v_request_hash text;
  v_artifact_set_hash text;
  v_pair_count integer;
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if p_release_run_id is null then
    return public.lca_release_error('release_run_id_required', 400, 'releaseRunId is required');
  end if;
  if coalesce(p_release_version, '') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return public.lca_release_error('invalid_release_version', 400, 'releaseVersion must use NN.NN.NNN');
  end if;
  if coalesce(p_selection_manifest_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_input_manifest_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_calculation_bundle_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_profile_lock_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_publish_plan_hash, '') !~ '^[0-9a-f]{64}$' then
    return public.lca_release_error('invalid_hash', 400, 'Release hashes must be lowercase SHA-256 values');
  end if;
  if jsonb_typeof(coalesce(p_calculation_bundle_ref, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_publish_plan, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_audit, 'null'::jsonb)) <> 'object' then
    return public.lca_release_error('invalid_payload', 400, 'Bundle ref, publish plan, and audit must be JSON objects');
  end if;
  if nullif(trim(coalesce(p_idempotency_key, '')), '') is null then
    return public.lca_release_error('idempotency_key_required', 400, 'idempotencyKey is required');
  end if;
  if p_publish_plan->>'schemaVersion' is distinct from 'tiangong.release.publish-plan.v1'
     or p_publish_plan->>'releaseRunId' is distinct from p_release_run_id::text
     or p_publish_plan->>'releaseVersion' is distinct from p_release_version
     or p_publish_plan->>'calculationBundleHash' is distinct from p_calculation_bundle_hash
     or p_publish_plan->>'profileLockHash' is distinct from p_profile_lock_hash
     or p_publish_plan->>'planHash' is distinct from p_publish_plan_hash then
    return public.lca_release_error('publish_plan_mismatch', 400, 'Publish plan identity or exact hashes do not match the request');
  end if;
  v_artifact_set_hash := p_publish_plan->>'artifactSetHash';
  if coalesce(v_artifact_set_hash, '') !~ '^[0-9a-f]{64}$' then
    return public.lca_release_error('invalid_artifact_set_hash', 400, 'Publish plan artifactSetHash must be SHA-256');
  end if;
  if jsonb_typeof(p_publish_plan->'datasets') is distinct from 'array'
     or jsonb_array_length(p_publish_plan->'datasets') = 0
     or jsonb_typeof(p_publish_plan->'packages') is distinct from 'array'
     or jsonb_array_length(p_publish_plan->'packages') <> 4 then
    return public.lca_release_error('publish_plan_incomplete', 400, 'Publish plan must contain datasets and exactly four packages');
  end if;

  select count(distinct (package.value->>'profileId') || ':' || (package.value->>'format'))
    into v_pair_count
  from jsonb_array_elements(p_publish_plan->'packages') as package(value)
  where package.value->>'profileId' in (
          'unit-process-full-closure.v1',
          'standalone-lifecyclemodel-result-full-closure.v1'
        )
    and package.value->>'format' in ('tidas', 'ilcd')
    and package.value->>'sha256' ~ '^[0-9a-f]{64}$';

  if v_pair_count <> 4 then
    return public.lca_release_error('publish_plan_profiles_invalid', 400, 'Publish plan must contain both profiles in TIDAS and ILCD formats');
  end if;

  v_request_hash := encode(
    extensions.digest(
      convert_to(
        concat_ws(
          '|', p_release_run_id::text, p_release_version,
          p_selection_manifest_hash, p_input_manifest_hash,
          p_calculation_bundle_hash, p_profile_lock_hash,
          p_publish_plan_hash, v_artifact_set_hash
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  select * into v_existing
  from public.lca_release_runs
  where id = p_release_run_id
     or (created_by = v_actor and idempotency_key = trim(p_idempotency_key))
  order by (id = p_release_run_id) desc
  limit 1
  for update;

  if v_existing.id is not null then
    if v_existing.id = p_release_run_id
       and v_existing.created_by = v_actor
       and v_existing.request_hash = v_request_hash
       and v_existing.calculation_bundle_ref = p_calculation_bundle_ref
       and v_existing.publish_plan = p_publish_plan then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'releaseRunId', v_existing.id,
          'releaseVersion', v_existing.release_version,
          'status', v_existing.status,
          'publishPlanHash', v_existing.publish_plan_hash
        )
      );
    end if;
    return public.lca_release_error('release_prepare_conflict', 409, 'Release run id or idempotency key is already bound to different content');
  end if;

  insert into public.lca_release_runs (
    id, release_version, scope_mode, selection_manifest_hash,
    input_manifest_hash, calculation_bundle_hash, calculation_bundle_ref,
    profile_lock_hash, publish_plan_hash, publish_plan, artifact_set_hash,
    status, idempotency_key, request_hash, created_by
  ) values (
    p_release_run_id, p_release_version, 'global_eligible', p_selection_manifest_hash,
    p_input_manifest_hash, p_calculation_bundle_hash, p_calculation_bundle_ref,
    p_profile_lock_hash, p_publish_plan_hash, p_publish_plan, v_artifact_set_hash,
    'prepared', trim(p_idempotency_key), v_request_hash, v_actor
  );

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_prepare', v_actor, 'lca_release_runs', p_release_run_id,
    p_release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'publishPlanHash', p_publish_plan_hash,
      'calculationBundleHash', p_calculation_bundle_hash,
      'inputManifestHash', p_input_manifest_hash,
      'artifactSetHash', v_artifact_set_hash
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'releaseRunId', p_release_run_id,
      'releaseVersion', p_release_version,
      'status', 'prepared',
      'publishPlanHash', p_publish_plan_hash
    )
  );
exception
  when unique_violation then
    return public.lca_release_error('release_prepare_conflict', 409, 'Release version, run id, or idempotency key already exists');
end;
$$;

create or replace function public.cmd_lca_release_artifacts_finalize_service(
  p_release_run_id uuid,
  p_publish_plan_hash text,
  p_release_manifest jsonb,
  p_release_manifest_hash text,
  p_artifacts jsonb,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_run public.lca_release_runs%rowtype;
  v_invalid_count integer;
  v_dataset_count integer;
  v_artifact_count integer;
begin
  if not public.lca_release_is_service_request() then
    return public.lca_release_error('service_role_required', 403, 'Internal service identity is required to finalize artifacts');
  end if;
  if jsonb_typeof(coalesce(p_release_manifest, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_artifacts, 'null'::jsonb)) <> 'array'
     or jsonb_typeof(coalesce(p_audit, 'null'::jsonb)) <> 'object' then
    return public.lca_release_error('invalid_payload', 400, 'Release manifest, artifacts, and audit have invalid JSON shapes');
  end if;
  if coalesce(p_publish_plan_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_release_manifest_hash, '') !~ '^[0-9a-f]{64}$' then
    return public.lca_release_error('invalid_hash', 400, 'Plan and release manifest hashes must be lowercase SHA-256 values');
  end if;

  select * into v_run
  from public.lca_release_runs
  where id = p_release_run_id
  for update;

  if v_run.id is null then
    return public.lca_release_error('release_run_not_found', 404, 'Release run not found');
  end if;
  if v_run.publish_plan_hash <> p_publish_plan_hash then
    return public.lca_release_error('publish_plan_hash_mismatch', 409, 'Artifact finalize plan hash does not match the prepared run');
  end if;
  if v_run.status <> 'prepared' then
    if v_run.status in ('ready_for_approval', 'approved', 'published', 'readback_verified')
       and v_run.release_manifest_hash = p_release_manifest_hash then
      with existing as (
        select profile_id, artifact_format, storage_bucket, object_key, sha256, byte_size, media_type
        from public.lca_release_artifacts
        where release_run_id = v_run.id
      ),
      supplied as (
        select
          artifact.value->>'profileId' as profile_id,
          artifact.value->>'format' as artifact_format,
          artifact.value->>'storageBucket' as storage_bucket,
          artifact.value->>'objectKey' as object_key,
          artifact.value->>'sha256' as sha256,
          (artifact.value->>'byteSize')::bigint as byte_size,
          artifact.value->>'mediaType' as media_type
        from jsonb_array_elements(p_artifacts) as artifact(value)
      )
      select count(*) into v_invalid_count
      from existing
      full join supplied using (profile_id, artifact_format)
      where existing.profile_id is null
         or supplied.profile_id is null
         or existing.storage_bucket is distinct from supplied.storage_bucket
         or existing.object_key is distinct from supplied.object_key
         or existing.sha256 is distinct from supplied.sha256
         or existing.byte_size is distinct from supplied.byte_size
         or existing.media_type is distinct from supplied.media_type;

      if v_invalid_count = 0 and jsonb_array_length(p_artifacts) = 4 then
        return jsonb_build_object(
          'ok', true,
          'reused', true,
          'data', jsonb_build_object(
            'releaseRunId', v_run.id,
            'status', v_run.status,
            'releaseManifestHash', v_run.release_manifest_hash
          )
        );
      end if;
      return public.lca_release_error('release_finalize_conflict', 409, 'Finalize retry artifact refs differ from durable content');
    end if;
    return public.lca_release_error('release_state_conflict', 409, 'Release run is not in prepared state');
  end if;

  if p_release_manifest->>'schemaVersion' is distinct from 'tiangong.release-manifest.v1'
     or p_release_manifest->>'releaseRunId' is distinct from v_run.id::text
     or p_release_manifest->>'releaseVersion' is distinct from v_run.release_version
     or p_release_manifest->>'profileLockHash' is distinct from v_run.profile_lock_hash
     or p_release_manifest->>'publishPlanHash' is distinct from v_run.publish_plan_hash
     or p_release_manifest->>'artifactSetHash' is distinct from v_run.artifact_set_hash
     or p_release_manifest->'scope'->>'coverageMode' is distinct from 'global_eligible'
     or p_release_manifest->'scope'->>'selectionManifestHash' is distinct from v_run.selection_manifest_hash
     or p_release_manifest->'calculationBundle'->>'bundleContentHash' is distinct from v_run.calculation_bundle_hash then
    return public.lca_release_error('release_manifest_mismatch', 409, 'Release manifest does not match the prepared run and exact hashes');
  end if;
  if jsonb_typeof(p_release_manifest->'datasets') is distinct from 'array'
     or jsonb_array_length(p_release_manifest->'datasets') = 0
     or jsonb_typeof(p_release_manifest->'packages') is distinct from 'array'
     or jsonb_array_length(p_release_manifest->'packages') <> 4
     or jsonb_array_length(p_artifacts) <> 4 then
    return public.lca_release_error('release_manifest_incomplete', 400, 'Release manifest requires datasets and four uploaded package artifacts');
  end if;
  if coalesce(
    p_release_manifest->'validation'->'tidas'->>'status' = 'passed'
    and p_release_manifest->'validation'->'ilcd'->>'status' = 'passed'
    and p_release_manifest->'validation'->'semanticRoundtrip'->>'status' = 'passed'
    and p_release_manifest->'validation'->'referenceClosure'->>'status' = 'passed'
    and p_release_manifest->'validation'->'numericParity'->>'status' = 'passed',
    false
  ) is not true then
    return public.lca_release_error('validation_not_passed', 400, 'All release validation gates must pass before artifact finalization');
  end if;

  with expected as (
    select
      package.value->>'profileId' as profile_id,
      package.value->>'format' as artifact_format,
      package.value->>'closureHash' as closure_hash,
      package.value->'artifact'->>'sha256' as sha256,
      (package.value->'artifact'->>'byteSize')::bigint as byte_size,
      package.value->'artifact'->>'mediaType' as media_type
    from jsonb_array_elements(p_release_manifest->'packages') as package(value)
  ),
  uploaded as (
    select
      artifact.value->>'profileId' as profile_id,
      artifact.value->>'format' as artifact_format,
      artifact.value->>'storageBucket' as storage_bucket,
      artifact.value->>'objectKey' as object_key,
      artifact.value->>'sha256' as sha256,
      (artifact.value->>'byteSize')::bigint as byte_size,
      artifact.value->>'mediaType' as media_type
    from jsonb_array_elements(p_artifacts) as artifact(value)
  )
  select count(*) into v_invalid_count
  from expected
  full join uploaded using (profile_id, artifact_format)
  where expected.profile_id is null
     or uploaded.profile_id is null
     or expected.profile_id not in (
          'unit-process-full-closure.v1',
          'standalone-lifecyclemodel-result-full-closure.v1'
        )
     or expected.artifact_format not in ('tidas', 'ilcd')
     or expected.sha256 !~ '^[0-9a-f]{64}$'
     or expected.closure_hash !~ '^[0-9a-f]{64}$'
     or expected.sha256 is distinct from uploaded.sha256
     or expected.byte_size is distinct from uploaded.byte_size
     or expected.media_type is distinct from uploaded.media_type
     or coalesce(length(trim(uploaded.storage_bucket)), 0) = 0
     or coalesce(length(trim(uploaded.object_key)), 0) = 0;

  if v_invalid_count <> 0 then
    return public.lca_release_error('artifact_set_mismatch', 409, 'Uploaded artifact refs do not exactly match the release manifest');
  end if;

  with dataset_rows as (
    select dataset.value
    from jsonb_array_elements(p_release_manifest->'datasets') as dataset(value)
  )
  select count(*) into v_invalid_count
  from dataset_rows
  where value->>'datasetType' not in (
          'process', 'lifecyclemodel', 'flow', 'flowproperty', 'unitgroup',
          'lciamethod', 'source', 'contact'
        )
     or value->>'role' not in ('unit_process', 'result_process', 'lifecycle_model', 'support')
     or value->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
     or value->>'versionSignificantHash' !~ '^[0-9a-f]{64}$'
     or value->>'semanticHash' !~ '^[0-9a-f]{64}$'
     or value->>'canonicalContentHash' !~ '^[0-9a-f]{64}$'
     or jsonb_typeof(value->'artifact') is distinct from 'object';

  if v_invalid_count <> 0 then
    return public.lca_release_error('dataset_index_invalid', 400, 'Release dataset index contains invalid identities, versions, or hashes');
  end if;

  insert into public.lca_release_dataset_versions (
    release_run_id, dataset_type, dataset_role, dataset_uuid, dataset_version,
    version_significant_hash, semantic_hash, canonical_content_hash, artifact_ref
  )
  select
    v_run.id,
    dataset.value->>'datasetType',
    dataset.value->>'role',
    (dataset.value->>'uuid')::uuid,
    dataset.value->>'version',
    dataset.value->>'versionSignificantHash',
    dataset.value->>'semanticHash',
    dataset.value->>'canonicalContentHash',
    dataset.value->'artifact'
  from jsonb_array_elements(p_release_manifest->'datasets') as dataset(value);
  get diagnostics v_dataset_count = row_count;

  insert into public.lca_release_artifacts (
    release_run_id, profile_id, artifact_format, storage_bucket, object_key,
    sha256, byte_size, media_type, closure_hash, verified_at
  )
  select
    v_run.id,
    package.value->>'profileId',
    package.value->>'format',
    artifact.value->>'storageBucket',
    artifact.value->>'objectKey',
    artifact.value->>'sha256',
    (artifact.value->>'byteSize')::bigint,
    artifact.value->>'mediaType',
    package.value->>'closureHash',
    now()
  from jsonb_array_elements(p_release_manifest->'packages') as package(value)
  join jsonb_array_elements(p_artifacts) as artifact(value)
    on artifact.value->>'profileId' = package.value->>'profileId'
   and artifact.value->>'format' = package.value->>'format';
  get diagnostics v_artifact_count = row_count;

  update public.lca_release_runs
  set release_manifest = p_release_manifest,
      release_manifest_hash = p_release_manifest_hash,
      status = 'ready_for_approval',
      artifacts_finalized_at = now(),
      updated_at = now()
  where id = v_run.id;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_artifacts_finalize_service', v_run.created_by,
    'lca_release_runs', v_run.id, v_run.release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'serviceCallback', true,
      'publishPlanHash', p_publish_plan_hash,
      'releaseManifestHash', p_release_manifest_hash,
      'artifactSetHash', v_run.artifact_set_hash,
      'artifactCount', v_artifact_count,
      'datasetCount', v_dataset_count
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'releaseRunId', v_run.id,
      'status', 'ready_for_approval',
      'releaseManifestHash', p_release_manifest_hash,
      'artifactCount', v_artifact_count,
      'datasetCount', v_dataset_count
    )
  );
exception
  when invalid_text_representation or numeric_value_out_of_range then
    return public.lca_release_error('release_manifest_invalid', 400, 'Release manifest contains invalid UUID or byte-size values');
  when unique_violation then
    return public.lca_release_error('release_artifact_conflict', 409, 'Release dataset or artifact identity already exists with conflicting content');
end;
$$;

create or replace function public.cmd_lca_release_approve(
  p_release_run_id uuid,
  p_publish_plan_hash text,
  p_expires_at timestamptz default null,
  p_reason text default null,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_run public.lca_release_runs%rowtype;
  v_approval public.lca_release_approvals%rowtype;
  v_now timestamptz := clock_timestamp();
  v_expires_at timestamptz;
  v_approval_hash text;
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if coalesce(p_publish_plan_hash, '') !~ '^[0-9a-f]{64}$' then
    return public.lca_release_error('invalid_publish_plan_hash', 400, 'publishPlanHash must be SHA-256');
  end if;

  select * into v_run
  from public.lca_release_runs
  where id = p_release_run_id
  for update;

  if v_run.id is null then
    return public.lca_release_error('release_run_not_found', 404, 'Release run not found');
  end if;
  if v_run.publish_plan_hash <> p_publish_plan_hash then
    return public.lca_release_error('publish_plan_hash_mismatch', 409, 'Approval must bind the exact immutable publish plan');
  end if;
  if v_run.status not in ('ready_for_approval', 'approved') then
    return public.lca_release_error('release_not_ready_for_approval', 409, 'Release run is not ready for approval');
  end if;
  if v_run.release_manifest_hash is null
     or (select count(*) from public.lca_release_artifacts where release_run_id = v_run.id) <> 4 then
    return public.lca_release_error('release_artifacts_incomplete', 409, 'Four verified release artifacts are required before approval');
  end if;

  update public.lca_release_approvals
  set status = 'expired'
  where release_run_id = v_run.id
    and status = 'approved'
    and expires_at <= v_now;

  select * into v_approval
  from public.lca_release_approvals
  where release_run_id = v_run.id
    and status = 'approved'
  for update;

  if v_approval.id is not null then
    if v_approval.publish_plan_hash = p_publish_plan_hash then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'approvalId', v_approval.id,
          'approvalHash', v_approval.approval_hash,
          'publishPlanHash', v_approval.publish_plan_hash,
          'approvedBy', v_approval.approved_by,
          'approvedAt', v_approval.approved_at,
          'expiresAt', v_approval.expires_at
        )
      );
    end if;
    return public.lca_release_error('active_approval_conflict', 409, 'An active approval exists for different content');
  end if;

  v_expires_at := coalesce(p_expires_at, v_now + interval '24 hours');
  if v_expires_at <= v_now then
    return public.lca_release_error('approval_expiry_invalid', 400, 'Approval expiry must be in the future');
  end if;
  v_approval_hash := encode(
    extensions.digest(
      convert_to(
        concat_ws('|', v_run.id::text, p_publish_plan_hash, v_actor::text, v_now::text),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  insert into public.lca_release_approvals (
    release_run_id, publish_plan_hash, approval_hash, status,
    approved_by, approved_at, expires_at, reason, audit_correlation
  ) values (
    v_run.id, p_publish_plan_hash, v_approval_hash, 'approved',
    v_actor, v_now, v_expires_at, nullif(trim(coalesce(p_reason, '')), ''),
    coalesce(p_audit, '{}'::jsonb)
  )
  returning * into v_approval;

  update public.lca_release_runs
  set status = 'approved', approved_at = v_now, updated_at = v_now
  where id = v_run.id;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_approve', v_actor, 'lca_release_approvals', v_approval.id,
    v_run.release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'releaseRunId', v_run.id,
      'publishPlanHash', p_publish_plan_hash,
      'approvalHash', v_approval_hash,
      'expiresAt', v_expires_at
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'approvalId', v_approval.id,
      'approvalHash', v_approval.approval_hash,
      'publishPlanHash', v_approval.publish_plan_hash,
      'approvedBy', v_approval.approved_by,
      'approvedAt', v_approval.approved_at,
      'expiresAt', v_approval.expires_at
    )
  );
end;
$$;

create or replace function public.cmd_lca_release_publish(
  p_release_run_id uuid,
  p_approval_id uuid,
  p_approval_hash text,
  p_publish_plan_hash text,
  p_idempotency_key text,
  p_credential_fingerprint text,
  p_reason text default null,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_run public.lca_release_runs%rowtype;
  v_approval public.lca_release_approvals%rowtype;
  v_existing public.lca_release_publications%rowtype;
  v_publication public.lca_release_publications%rowtype;
  v_previous public.lca_release_publications%rowtype;
  v_now timestamptz := clock_timestamp();
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if coalesce(p_approval_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_publish_plan_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_credential_fingerprint, '') !~ '^[0-9a-f]{64}$' then
    return public.lca_release_error('invalid_publish_hash', 400, 'Approval, plan, and credential fingerprint must be lowercase SHA-256 values');
  end if;
  if nullif(trim(coalesce(p_idempotency_key, '')), '') is null then
    return public.lca_release_error('idempotency_key_required', 400, 'idempotencyKey is required');
  end if;

  select * into v_run
  from public.lca_release_runs
  where id = p_release_run_id
  for update;

  if v_run.id is null then
    return public.lca_release_error('release_run_not_found', 404, 'Release run not found');
  end if;

  select * into v_existing
  from public.lca_release_publications
  where release_run_id = v_run.id
     or (executed_by = v_actor and idempotency_key = trim(p_idempotency_key))
  order by (release_run_id = v_run.id) desc
  limit 1;

  if v_existing.id is not null then
    if v_existing.release_run_id = v_run.id
       and v_existing.executed_by = v_actor
       and v_existing.approval_id = p_approval_id
       and v_existing.approval_hash = p_approval_hash
       and v_existing.publish_plan_hash = p_publish_plan_hash
       and v_existing.idempotency_key = trim(p_idempotency_key)
       and v_existing.credential_fingerprint = p_credential_fingerprint then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'publicationId', v_existing.id,
          'releaseRunId', v_existing.release_run_id,
          'releaseVersion', v_existing.release_version,
          'status', v_existing.status,
          'publishedAt', v_existing.published_at
        )
      );
    end if;
    return public.lca_release_error('publish_idempotency_conflict', 409, 'Publish idempotency key is bound to different content');
  end if;

  if v_run.status <> 'approved' then
    return public.lca_release_error('release_not_approved', 409, 'Release run must have a durable active approval before publish');
  end if;
  if v_run.publish_plan_hash <> p_publish_plan_hash then
    return public.lca_release_error('publish_plan_hash_mismatch', 409, 'Publish plan hash differs from the prepared release');
  end if;

  select * into v_approval
  from public.lca_release_approvals
  where id = p_approval_id
    and release_run_id = v_run.id
  for update;

  if v_approval.id is null
     or v_approval.status <> 'approved'
     or v_approval.publish_plan_hash <> p_publish_plan_hash
     or v_approval.approval_hash <> p_approval_hash
     or v_approval.expires_at <= v_now then
    return public.lca_release_error('approval_invalid', 409, 'Approval is missing, expired, consumed, or does not bind the exact plan');
  end if;
  if v_run.release_manifest_hash is null
     or v_run.release_manifest->>'publishPlanHash' <> p_publish_plan_hash
     or v_run.release_manifest->>'artifactSetHash' <> v_run.artifact_set_hash
     or (select count(*) from public.lca_release_artifacts where release_run_id = v_run.id and verified_at is not null) <> 4 then
    return public.lca_release_error('release_artifacts_incomplete', 409, 'Verified manifest and all four artifacts are required before publish');
  end if;

  lock table public.lca_release_publications in exclusive mode;

  select * into v_previous
  from public.lca_release_publications
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
  for update;

  if v_previous.id is not null then
    update public.lca_release_publications
    set is_current = false,
        status = 'superseded',
        superseded_at = v_now,
        updated_at = v_now
    where id = v_previous.id;
  end if;

  insert into public.lca_release_publications (
    release_run_id, release_version, publication_series_key,
    publication_channel, visibility_scope, status, is_current,
    approval_id, approval_hash, publish_plan_hash, release_manifest_hash,
    artifact_set_hash, approved_by, executed_by, credential_fingerprint,
    idempotency_key, published_at, reason
  ) values (
    v_run.id, v_run.release_version, 'global', 'public', 'public', 'current', true,
    v_approval.id, v_approval.approval_hash, v_run.publish_plan_hash,
    v_run.release_manifest_hash, v_run.artifact_set_hash,
    v_approval.approved_by, v_actor, p_credential_fingerprint,
    trim(p_idempotency_key), v_now, nullif(trim(coalesce(p_reason, '')), '')
  )
  returning * into v_publication;

  if v_previous.id is not null then
    update public.lca_release_publications
    set superseded_by = v_publication.id,
        updated_at = v_now
    where id = v_previous.id;
  end if;

  update public.lca_release_approvals
  set status = 'consumed', consumed_by = v_actor, consumed_at = v_now
  where id = v_approval.id;

  update public.lca_release_artifacts
  set pinned = true, published_at = v_now
  where release_run_id = v_run.id;

  update public.lca_release_runs
  set status = 'published', published_at = v_now, updated_at = v_now
  where id = v_run.id;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_publish', v_actor, 'lca_release_publications', v_publication.id,
    v_run.release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'releaseRunId', v_run.id,
      'approvalId', v_approval.id,
      'approvalHash', v_approval.approval_hash,
      'publishPlanHash', v_run.publish_plan_hash,
      'releaseManifestHash', v_run.release_manifest_hash,
      'artifactSetHash', v_run.artifact_set_hash,
      'approvedBy', v_approval.approved_by,
      'executedBy', v_actor,
      'credentialFingerprint', p_credential_fingerprint,
      'previousPublicationId', v_previous.id
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'releaseRunId', v_run.id,
      'releaseVersion', v_run.release_version,
      'status', 'current',
      'approvedBy', v_approval.approved_by,
      'executedBy', v_actor,
      'publishedAt', v_now
    )
  );
exception
  when unique_violation then
    return public.lca_release_error('publication_conflict', 409, 'Release version or publish idempotency key already has a publication');
end;
$$;

create or replace function public.cmd_lca_release_readback_verify(
  p_release_run_id uuid,
  p_release_manifest_hash text,
  p_artifact_hashes jsonb,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_run public.lca_release_runs%rowtype;
  v_mismatch_count integer;
  v_now timestamptz := clock_timestamp();
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if jsonb_typeof(coalesce(p_artifact_hashes, 'null'::jsonb)) <> 'array' then
    return public.lca_release_error('invalid_readback_payload', 400, 'artifactHashes must be a JSON array');
  end if;

  select * into v_run
  from public.lca_release_runs
  where id = p_release_run_id
  for update;

  if v_run.id is null then
    return public.lca_release_error('release_run_not_found', 404, 'Release run not found');
  end if;
  if v_run.status not in ('published', 'readback_verified') then
    return public.lca_release_error('release_not_published', 409, 'Release must be published before readback verification');
  end if;
  if v_run.release_manifest_hash <> p_release_manifest_hash then
    return public.lca_release_error('readback_manifest_hash_mismatch', 409, 'Readback release manifest hash differs from the published value');
  end if;
  if jsonb_array_length(p_artifact_hashes) <> 4 then
    return public.lca_release_error('readback_artifacts_incomplete', 409, 'Readback must verify all four release artifacts');
  end if;

  with expected as (
    select id::text as artifact_id, sha256
    from public.lca_release_artifacts
    where release_run_id = v_run.id
  ),
  observed as (
    select value->>'artifactId' as artifact_id, value->>'sha256' as sha256
    from jsonb_array_elements(p_artifact_hashes)
  )
  select count(*) into v_mismatch_count
  from expected
  full join observed using (artifact_id)
  where expected.artifact_id is null
     or observed.artifact_id is null
     or expected.sha256 is distinct from observed.sha256;

  if v_mismatch_count <> 0 then
    return public.lca_release_error('readback_artifact_hash_mismatch', 409, 'Readback artifact hashes differ from published immutable refs');
  end if;

  update public.lca_release_runs
  set status = 'readback_verified',
      readback_verified_at = coalesce(readback_verified_at, v_now),
      readback_receipt = jsonb_build_object(
        'releaseManifestHash', p_release_manifest_hash,
        'artifactHashes', p_artifact_hashes,
        'verifiedBy', v_actor,
        'verifiedAt', v_now
      ),
      updated_at = v_now
  where id = v_run.id;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_readback_verify', v_actor, 'lca_release_runs', v_run.id,
    v_run.release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'releaseManifestHash', p_release_manifest_hash,
      'artifactCount', jsonb_array_length(p_artifact_hashes)
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'releaseRunId', v_run.id,
      'status', 'readback_verified',
      'releaseManifestHash', p_release_manifest_hash,
      'verifiedAt', v_now
    )
  );
end;
$$;

create or replace function public.cmd_lca_release_unpublish(
  p_publication_id uuid,
  p_reason text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_publication public.lca_release_publications%rowtype;
  v_now timestamptz := clock_timestamp();
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if nullif(trim(coalesce(p_reason, '')), '') is null then
    return public.lca_release_error('reason_required', 400, 'Unpublish requires an audit reason');
  end if;

  select * into v_publication
  from public.lca_release_publications
  where id = p_publication_id
  for update;

  if v_publication.id is null then
    return public.lca_release_error('publication_not_found', 404, 'Publication not found');
  end if;
  if v_publication.status = 'unpublished' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', jsonb_build_object('publicationId', v_publication.id, 'status', 'unpublished')
    );
  end if;
  if not v_publication.is_current or v_publication.status <> 'current' then
    return public.lca_release_error('publication_not_current', 409, 'Only the current publication can be unpublished');
  end if;

  update public.lca_release_publications
  set status = 'unpublished', is_current = false, unpublished_by = v_actor,
      unpublished_at = v_now, reason = p_reason, updated_at = v_now
  where id = v_publication.id;

  update public.lca_release_runs
  set status = 'unpublished', updated_at = v_now
  where id = v_publication.release_run_id;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_unpublish', v_actor, 'lca_release_publications',
    v_publication.id, v_publication.release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('reason', p_reason)
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object('publicationId', v_publication.id, 'status', 'unpublished')
  );
end;
$$;

create or replace function public.get_lca_release_run(p_release_run_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_run public.lca_release_runs%rowtype;
  v_is_public boolean;
begin
  select * into v_run
  from public.lca_release_runs
  where id = p_release_run_id;

  if v_run.id is null then
    return public.lca_release_error('release_run_not_found', 404, 'Release run not found');
  end if;
  select exists (
    select 1 from public.lca_release_publications
    where release_run_id = v_run.id and status in ('current', 'superseded')
  ) into v_is_public;

  if not v_is_public and not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required for private release runs');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'releaseRunId', v_run.id,
      'releaseVersion', v_run.release_version,
      'status', v_run.status,
      'scopeMode', v_run.scope_mode,
      'selectionManifestHash', v_run.selection_manifest_hash,
      'inputManifestHash', v_run.input_manifest_hash,
      'calculationBundleHash', v_run.calculation_bundle_hash,
      'publishPlanHash', v_run.publish_plan_hash,
      'releaseManifestHash', v_run.release_manifest_hash,
      'artifactSetHash', v_run.artifact_set_hash,
      'createdBy', v_run.created_by,
      'createdAt', v_run.created_at,
      'approvedAt', v_run.approved_at,
      'publishedAt', v_run.published_at,
      'readbackVerifiedAt', v_run.readback_verified_at,
      'calculationBundle', case when public.lca_release_is_manager() then v_run.calculation_bundle_ref else null end,
      'artifacts', (
        select coalesce(jsonb_agg(jsonb_build_object(
          'artifactId', artifact.id,
          'profileId', artifact.profile_id,
          'format', artifact.artifact_format,
          'sha256', artifact.sha256,
          'byteSize', artifact.byte_size,
          'mediaType', artifact.media_type,
          'pinned', artifact.pinned
        ) order by artifact.profile_id, artifact.artifact_format), '[]'::jsonb)
        from public.lca_release_artifacts as artifact
        where artifact.release_run_id = v_run.id
      ),
      'blockers', case
        when v_run.status = 'prepared' then jsonb_build_array('artifacts_not_finalized')
        when v_run.status = 'ready_for_approval' then jsonb_build_array('approval_required')
        when v_run.status = 'approved' then jsonb_build_array('publish_required')
        when v_run.status = 'published' then jsonb_build_array('readback_verification_required')
        else '[]'::jsonb
      end
    )
  );
end;
$$;

create or replace function public.get_current_lca_release()
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_publication public.lca_release_publications%rowtype;
begin
  select * into v_publication
  from public.lca_release_publications
  where is_current = true and status = 'current'
  order by published_at desc
  limit 1;

  if v_publication.id is null then
    return public.lca_release_error('publication_not_found', 404, 'No current public LCA release exists');
  end if;

  return public.get_lca_release_run(v_publication.release_run_id);
end;
$$;

create or replace function public.get_lca_release_artifact_download(
  p_artifact_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_artifact public.lca_release_artifacts%rowtype;
  v_is_public boolean;
begin
  select * into v_artifact
  from public.lca_release_artifacts
  where id = p_artifact_id;

  if v_artifact.id is null then
    return public.lca_release_error('artifact_not_found', 404, 'Release artifact not found');
  end if;
  select exists (
    select 1 from public.lca_release_publications
    where release_run_id = v_artifact.release_run_id
      and status in ('current', 'superseded')
  ) into v_is_public;

  if not v_is_public and not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required for private artifacts');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'artifactId', v_artifact.id,
      'releaseRunId', v_artifact.release_run_id,
      'profileId', v_artifact.profile_id,
      'format', v_artifact.artifact_format,
      'storageBucket', v_artifact.storage_bucket,
      'objectKey', v_artifact.object_key,
      'sha256', v_artifact.sha256,
      'byteSize', v_artifact.byte_size,
      'mediaType', v_artifact.media_type,
      'public', v_is_public
    )
  );
end;
$$;

create or replace function public.get_lcia_result_calculation_bundle(
  p_package_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_package public.lcia_result_packages%rowtype;
  v_result public.lca_results%rowtype;
  v_bundle jsonb;
begin
  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select * into v_package
  from public.lcia_result_packages
  where id = p_package_id;
  if v_package.id is null then
    return public.lcia_result_error('package_not_found', 404, 'Package not found');
  end if;

  select * into v_result
  from public.lca_results
  where id = v_package.result_id;

  v_bundle := coalesce(
    v_package.artifact_manifest->'calculationBundle',
    v_result.diagnostics->'calculation_bundle'
  );
  if v_bundle is null then
    return public.lcia_result_error('calculation_bundle_not_available', 404, 'Calculation Bundle is not available for this legacy package');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'snapshotId', v_package.snapshot_id,
      'resultId', v_package.result_id,
      'calculationBundle', v_bundle,
      'availableImpactCategories', v_package.available_impact_categories
    )
  );
end;
$$;

revoke all on function public.lca_release_guard_run_update() from public, anon, authenticated, service_role;
revoke all on function public.lca_release_guard_dataset_update() from public, anon, authenticated, service_role;
revoke all on function public.lca_release_guard_artifact_update() from public, anon, authenticated, service_role;
revoke all on function public.lca_release_guard_approval_update() from public, anon, authenticated, service_role;
revoke all on function public.lca_release_guard_publication_update() from public, anon, authenticated, service_role;
revoke all on function public.lca_release_error(text, integer, text) from public, anon, authenticated, service_role;
revoke all on function public.lca_release_is_manager() from public, anon, authenticated, service_role;
revoke all on function public.assert_lca_release_manager() from public, anon, authenticated, service_role;
revoke all on function public.lca_release_is_service_request() from public, anon, authenticated, service_role;
revoke all on function public.cmd_lca_release_prepare(uuid, text, text, text, jsonb, text, text, jsonb, text, text, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lca_release_artifacts_finalize_service(uuid, text, jsonb, text, jsonb, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lca_release_approve(uuid, text, timestamptz, text, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lca_release_publish(uuid, uuid, text, text, text, text, text, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lca_release_readback_verify(uuid, text, jsonb, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lca_release_unpublish(uuid, text, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.get_lca_release_run(uuid) from public, anon, authenticated, service_role;
revoke all on function public.get_current_lca_release() from public, anon, authenticated, service_role;
revoke all on function public.get_lca_release_artifact_download(uuid) from public, anon, authenticated, service_role;
revoke all on function public.get_lcia_result_calculation_bundle(uuid) from public, anon, authenticated, service_role;

grant execute on function public.cmd_lca_release_prepare(uuid, text, text, text, jsonb, text, text, jsonb, text, text, jsonb) to authenticated;
grant execute on function public.assert_lca_release_manager() to authenticated;
grant execute on function public.cmd_lca_release_approve(uuid, text, timestamptz, text, jsonb) to authenticated;
grant execute on function public.cmd_lca_release_publish(uuid, uuid, text, text, text, text, text, jsonb) to authenticated;
grant execute on function public.cmd_lca_release_readback_verify(uuid, text, jsonb, jsonb) to authenticated;
grant execute on function public.cmd_lca_release_unpublish(uuid, text, jsonb) to authenticated;
grant execute on function public.get_lca_release_run(uuid) to anon, authenticated, service_role;
grant execute on function public.get_current_lca_release() to anon, authenticated, service_role;
grant execute on function public.get_lca_release_artifact_download(uuid) to anon, authenticated, service_role;
grant execute on function public.get_lcia_result_calculation_bundle(uuid) to authenticated;
grant execute on function public.cmd_lca_release_artifacts_finalize_service(uuid, text, jsonb, text, jsonb, jsonb) to service_role;

comment on table public.lca_release_runs is
  'Durable release control-plane facts. Canonical TIDAS datasets and ZIP bytes remain immutable object artifacts, not editable authoring rows.';
comment on table public.lca_release_dataset_versions is
  'Read/index projection for exact UUID+version release datasets; generated Model/Result datasets are not inserted into authoring tables.';
comment on table public.lca_release_artifacts is
  'Verified immutable TIDAS/ILCD package refs. Published rows remain pinned after supersede or unpublish.';
comment on table public.lca_release_approvals is
  'Durable human approval receipts bound to an exact immutable publish-plan hash.';
comment on table public.lca_release_publications is
  'Public release facts with distinct approved_by and executed_by audit identities.';
