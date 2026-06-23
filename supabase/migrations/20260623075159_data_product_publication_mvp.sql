-- LCIA result package build/publication MVP.
-- Worker execution state stays in public.worker_jobs. This migration only adds
-- the durable LCIA result package and public/latest publication facts.

alter table public.roles
  drop constraint if exists roles_role_check;

alter table public.roles
  add constraint roles_role_check
  check (
    role::text = any (
      array[
        'owner',
        'admin',
        'member',
        'is_invited',
        'rejected',
        'review-admin',
        'review-member',
        'data_product_manager'
      ]::text[]
    )
  );

alter table public.lca_network_snapshots
  drop constraint if exists lca_network_snapshots_scope_chk;

alter table public.lca_network_snapshots
  add constraint lca_network_snapshots_scope_chk
  check (scope = any (array['full_library'::text, 'data_product'::text]));

insert into public.worker_job_kinds (
  job_kind,
  worker_runtime,
  worker_queue,
  default_visibility,
  default_priority,
  default_max_attempts,
  default_lease_seconds,
  payload_schema_version,
  result_schema_version,
  user_visible,
  description
) values (
  'lcia_result.package_build',
  'calculator',
  'solver',
  'operator',
  0,
  3,
  3600,
  'lcia_result.package_build.request.v1',
  'lcia_result.package_build.result.v1',
  false,
  'Builds an immutable LCIA result package from a published-only data-product snapshot and pinned/copied result artifacts.'
) on conflict (job_kind) do update
set worker_runtime = excluded.worker_runtime,
    worker_queue = excluded.worker_queue,
    default_visibility = excluded.default_visibility,
    default_priority = excluded.default_priority,
    default_max_attempts = excluded.default_max_attempts,
    default_lease_seconds = excluded.default_lease_seconds,
    payload_schema_version = excluded.payload_schema_version,
    result_schema_version = excluded.result_schema_version,
    user_visible = excluded.user_visible,
    description = excluded.description,
    updated_at = now();

create table if not exists public.lcia_result_packages (
  id uuid primary key default gen_random_uuid(),
  build_id uuid not null,
  build_worker_job_id uuid not null references public.worker_jobs(id) on delete restrict,
  package_version text not null,
  coverage_mode text not null,
  input_status_filter jsonb not null default '{"state_code":{"between":[100,199]}}'::jsonb,
  eligibility_definition jsonb not null default '{}'::jsonb,
  eligibility_resolved_at timestamptz not null,
  eligible_input_count integer not null,
  included_input_count integer not null,
  input_manifest_hash text not null,
  input_manifest jsonb not null,
  snapshot_id uuid not null references public.lca_network_snapshots(id) on delete restrict,
  result_id uuid not null references public.lca_results(id) on delete restrict,
  latest_all_unit_result_id uuid references public.lca_latest_all_unit_results(id) on delete set null,
  result_artifact_ref jsonb not null default '{}'::jsonb,
  query_artifact_ref jsonb not null default '{}'::jsonb,
  artifact_manifest jsonb not null default '{}'::jsonb,
  package_result_hash text,
  lcia_method_set jsonb not null default '[]'::jsonb,
  available_impact_categories jsonb not null default '[]'::jsonb,
  postprocess_manifest jsonb not null default '{"postprocess_mode":"skipped"}'::jsonb,
  default_impact_category text,
  status text not null default 'preview_ready',
  created_by uuid not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint lcia_result_packages_counts_chk check (
    eligible_input_count >= 0 and included_input_count >= 0
  ),
  constraint lcia_result_packages_coverage_chk check (
    coverage_mode in ('subset', 'global_eligible')
  ),
  constraint lcia_result_packages_input_manifest_object_chk check (
    jsonb_typeof(input_manifest) = 'object'
  ),
  constraint lcia_result_packages_artifact_refs_object_chk check (
    jsonb_typeof(result_artifact_ref) = 'object'
    and jsonb_typeof(query_artifact_ref) = 'object'
    and jsonb_typeof(artifact_manifest) = 'object'
    and jsonb_typeof(postprocess_manifest) = 'object'
  ),
  constraint lcia_result_packages_lcia_method_set_chk check (
    jsonb_typeof(lcia_method_set) = 'array'
  ),
  constraint lcia_result_packages_available_impacts_chk check (
    jsonb_typeof(available_impact_categories) = 'array'
  ),
  constraint lcia_result_packages_status_chk check (
    status in ('preview_ready', 'deprecated', 'failed')
  )
);

create unique index if not exists lcia_result_packages_build_uidx
  on public.lcia_result_packages (build_id);

create unique index if not exists lcia_result_packages_build_worker_job_uidx
  on public.lcia_result_packages (build_worker_job_id);

create unique index if not exists lcia_result_packages_package_version_uidx
  on public.lcia_result_packages (package_version);

create index if not exists lcia_result_packages_created_idx
  on public.lcia_result_packages (created_at desc);

create table if not exists public.lcia_result_publications (
  id uuid primary key default gen_random_uuid(),
  package_id uuid not null references public.lcia_result_packages(id) on delete restrict,
  publication_series_key text not null default 'global',
  publication_channel text not null default 'public',
  visibility_scope text not null default 'public',
  is_current boolean not null default false,
  status text not null,
  display_default_impact_category text,
  published_by uuid,
  published_at timestamptz,
  unpublished_by uuid,
  unpublished_at timestamptz,
  reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint lcia_result_publications_channel_chk check (
    publication_channel = 'public'
  ),
  constraint lcia_result_publications_visibility_chk check (
    visibility_scope = 'public'
  ),
  constraint lcia_result_publications_status_chk check (
    status in ('current', 'superseded', 'unpublished')
  )
);

create unique index if not exists lcia_result_publications_current_uidx
  on public.lcia_result_publications (
    publication_series_key,
    publication_channel,
    visibility_scope
  )
  where is_current = true;

create index if not exists lcia_result_publications_package_idx
  on public.lcia_result_publications (package_id, created_at desc);

alter table public.lcia_result_packages enable row level security;
alter table public.lcia_result_publications enable row level security;

revoke all on table public.lcia_result_packages from anon, authenticated;
revoke all on table public.lcia_result_publications from anon, authenticated;
grant all on table public.lcia_result_packages to service_role;
grant all on table public.lcia_result_publications to service_role;

create or replace function public.lcia_result_is_service_request()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select current_user = 'service_role'
      or coalesce(util.is_service_request(), false)
$$;

create or replace function public.lcia_result_is_manager()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select public.policy_is_current_user_in_roles(
    '00000000-0000-0000-0000-000000000000'::uuid,
    array['data_product_manager']
  )
$$;

create or replace function public.lcia_result_error(
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

create or replace function public.lcia_result_current_eligible_manifest()
returns jsonb
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  with eligible as (
    select id, version, state_code
    from public.processes
    where state_code between 100 and 199
    order by id, version
  ),
  aggregated as (
    select
      count(*)::integer as eligible_count,
      md5(
        coalesce(
          string_agg(id::text || ':' || version, ',' order by id, version),
          ''
        ) || '|published:100-199:v1'
      ) as input_manifest_hash,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'id', id,
            'version', version,
            'stateCode', state_code
          )
          order by id, version
        ),
        '[]'::jsonb
      ) as processes
    from eligible
  )
  select jsonb_build_object(
    'predicateVersion', 'published-state-code-100-199:v1',
    'inputStatusFilter', jsonb_build_object(
      'state_code',
      jsonb_build_object('between', jsonb_build_array(100, 199))
    ),
    'eligibleInputCount', eligible_count,
    'includedInputCount', eligible_count,
    'inputManifestHash', input_manifest_hash,
    'inputManifest', jsonb_build_object(
      'predicateVersion', 'published-state-code-100-199:v1',
      'selectionMode', 'all_eligible',
      'processes', processes
    )
  )
  from aggregated
$$;

create or replace function public.cmd_system_change_member_role(
  p_user_id uuid,
  p_role text default null::text,
  p_action text default 'set'::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_action text := lower(coalesce(p_action, 'set'));
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_actor_is_owner boolean;
  v_actor_is_manager boolean;
  v_existing_role text;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object('ok', false, 'code', 'AUTH_REQUIRED', 'status', 401, 'message', 'Authentication required');
  end if;

  if p_user_id is null then
    return jsonb_build_object('ok', false, 'code', 'USER_ID_REQUIRED', 'status', 400, 'message', 'userId is required');
  end if;

  v_actor_is_owner := public.cmd_membership_is_system_owner(v_actor);
  v_actor_is_manager := public.cmd_membership_is_system_manager(v_actor);

  if not v_actor_is_manager then
    return jsonb_build_object('ok', false, 'code', 'FORBIDDEN', 'status', 403, 'message', 'The actor cannot manage system members');
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = v_team_id
  for update;

  if v_action = 'remove' then
    if v_existing_role is null then
      return jsonb_build_object('ok', false, 'code', 'ROLE_NOT_FOUND', 'status', 404, 'message', 'Role not found');
    end if;

    if p_user_id = v_actor or v_existing_role = 'owner' then
      return jsonb_build_object('ok', false, 'code', 'FORBIDDEN', 'status', 403, 'message', 'The actor cannot remove this system member');
    end if;

    delete from public.roles
    where user_id = p_user_id
      and team_id = v_team_id;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_system_change_member_role',
      v_actor,
      'roles',
      p_user_id,
      v_team_id::text,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('action', 'remove')
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object('removed', true, 'user_id', p_user_id, 'team_id', v_team_id)
    );
  end if;

  if v_action <> 'set' then
    return jsonb_build_object('ok', false, 'code', 'INVALID_ACTION', 'status', 400, 'message', 'Unsupported action');
  end if;

  if p_role not in ('member', 'admin', 'data_product_manager') then
    return jsonb_build_object('ok', false, 'code', 'INVALID_ROLE', 'status', 400, 'message', 'Unsupported system role transition');
  end if;

  if p_role = 'admin' and not v_actor_is_owner then
    return jsonb_build_object('ok', false, 'code', 'FORBIDDEN', 'status', 403, 'message', 'Only the system owner can assign admin roles');
  end if;

  if p_role = 'member' and v_existing_role = 'admin' and not v_actor_is_owner then
    return jsonb_build_object('ok', false, 'code', 'FORBIDDEN', 'status', 403, 'message', 'Only the system owner can demote an admin');
  end if;

  if v_existing_role is null then
    insert into public.roles (user_id, team_id, role, modified_at)
    values (p_user_id, v_team_id, p_role, now())
    returning to_jsonb(roles.*)
      into v_role_row;
  elsif v_existing_role in ('owner', 'admin', 'member', 'data_product_manager') then
    if v_existing_role = 'owner' then
      return jsonb_build_object('ok', false, 'code', 'FORBIDDEN', 'status', 403, 'message', 'The owner role cannot be modified');
    end if;

    update public.roles
      set role = p_role,
          modified_at = now()
    where user_id = p_user_id
      and team_id = v_team_id
    returning to_jsonb(roles.*)
      into v_role_row;
  else
    return jsonb_build_object('ok', false, 'code', 'ROLE_CONFLICT', 'status', 409, 'message', 'The existing zero-team role belongs to another scope');
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_system_change_member_role',
    v_actor,
    'roles',
    p_user_id,
    v_team_id::text,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('action', 'set', 'role', p_role)
  );

  return jsonb_build_object('ok', true, 'data', v_role_row);
exception
  when unique_violation then
    return jsonb_build_object('ok', false, 'code', 'ROLE_CONFLICT', 'status', 409, 'message', 'The existing zero-team role belongs to another scope');
end;
$$;

create or replace function public.lcia_result_prevent_ready_package_content_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if old.status = 'preview_ready'
     and (
       old.build_id is distinct from new.build_id
       or old.build_worker_job_id is distinct from new.build_worker_job_id
       or old.package_version is distinct from new.package_version
       or old.coverage_mode is distinct from new.coverage_mode
       or old.input_status_filter is distinct from new.input_status_filter
       or old.eligibility_definition is distinct from new.eligibility_definition
       or old.eligibility_resolved_at is distinct from new.eligibility_resolved_at
       or old.eligible_input_count is distinct from new.eligible_input_count
       or old.included_input_count is distinct from new.included_input_count
       or old.input_manifest_hash is distinct from new.input_manifest_hash
       or old.input_manifest is distinct from new.input_manifest
       or old.snapshot_id is distinct from new.snapshot_id
       or old.result_id is distinct from new.result_id
       or old.latest_all_unit_result_id is distinct from new.latest_all_unit_result_id
       or old.result_artifact_ref is distinct from new.result_artifact_ref
       or old.query_artifact_ref is distinct from new.query_artifact_ref
       or old.artifact_manifest is distinct from new.artifact_manifest
       or old.package_result_hash is distinct from new.package_result_hash
       or old.lcia_method_set is distinct from new.lcia_method_set
       or old.available_impact_categories is distinct from new.available_impact_categories
       or old.postprocess_manifest is distinct from new.postprocess_manifest
       or old.default_impact_category is distinct from new.default_impact_category
     ) then
    raise exception 'lcia_result_package_immutable'
      using errcode = '23514';
  end if;

  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists lcia_result_packages_prevent_ready_update
  on public.lcia_result_packages;

create trigger lcia_result_packages_prevent_ready_update
before update on public.lcia_result_packages
for each row
execute function public.lcia_result_prevent_ready_package_content_update();

create or replace function public.cmd_lcia_result_build_request(
  p_name text,
  p_processes jsonb default null::jsonb,
  p_coverage_mode text default 'global_eligible'::text,
  p_default_impact_category text default null::text,
  p_lcia_method_set jsonb default '[]'::jsonb,
  p_idempotency_key text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_coverage_mode text := lower(trim(coalesce(p_coverage_mode, 'global_eligible')));
  v_build_id uuid;
  v_idempotency_key text;
  v_current_manifest jsonb;
  v_input_manifest jsonb;
  v_input_manifest_hash text;
  v_eligible_input_count integer;
  v_included_input_count integer;
  v_invalid_count integer := 0;
  v_duplicate_count integer := 0;
  v_request_hash text;
  v_worker_payload jsonb;
  v_worker_job jsonb;
begin
  if v_actor is null then
    return public.lcia_result_error('auth_required', 401, 'Authentication required');
  end if;

  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  if v_coverage_mode not in ('subset', 'global_eligible') then
    return public.lcia_result_error('invalid_coverage_mode', 400, 'coverage_mode must be subset or global_eligible');
  end if;

  if v_coverage_mode = 'global_eligible'
     and p_processes is not null
     and jsonb_array_length(p_processes) > 0 then
    return public.lcia_result_error('invalid_coverage_mode', 400, 'global_eligible builds are resolved from the current published input predicate');
  end if;

  if jsonb_typeof(coalesce(p_lcia_method_set, '[]'::jsonb)) <> 'array' then
    return public.lcia_result_error('invalid_lcia_method_set', 400, 'lcia_method_set must be a JSON array');
  end if;

  if p_processes is not null and jsonb_typeof(p_processes) <> 'array' then
    return public.lcia_result_error('invalid_process_selection', 400, 'process selection must be a JSON array');
  end if;

  v_current_manifest := public.lcia_result_current_eligible_manifest();
  v_eligible_input_count := (v_current_manifest->>'eligibleInputCount')::integer;

  if v_coverage_mode = 'global_eligible' then
    v_input_manifest := v_current_manifest->'inputManifest';
    v_input_manifest_hash := v_current_manifest->>'inputManifestHash';
    v_included_input_count := v_eligible_input_count;
  else
    with requested as (
      select
        (item.value->>'id')::uuid as process_id,
        coalesce(item.value->>'version', item.value->>'process_version')::character(9) as process_version,
        item.ordinality::integer as ordinal
      from jsonb_array_elements(coalesce(p_processes, '[]'::jsonb)) with ordinality as item(value, ordinality)
    ),
    duplicate_rows as (
      select process_id, process_version, count(*)::integer as duplicate_count
      from requested
      group by process_id, process_version
      having count(*) > 1
    ),
    resolved as (
      select
        r.process_id,
        r.process_version,
        r.ordinal,
        p.state_code
      from requested as r
      left join public.processes as p
        on p.id = r.process_id
       and p.version = r.process_version
    ),
    aggregated as (
      select
        count(*) filter (where state_code between 100 and 199)::integer as included_count,
        count(*) filter (where state_code is null or state_code not between 100 and 199)::integer as invalid_count,
        coalesce((select sum(duplicate_count - 1)::integer from duplicate_rows), 0) as duplicate_count,
        md5(
          coalesce(
            string_agg(
              process_id::text || ':' || process_version,
              ','
              order by process_id, process_version
            ) filter (where state_code between 100 and 199),
            ''
          ) || '|published:100-199:v1'
        ) as manifest_hash,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'id', process_id,
              'version', process_version,
              'stateCode', state_code
            )
            order by ordinal
          ) filter (where state_code between 100 and 199),
          '[]'::jsonb
        ) as selected_processes
      from resolved
    )
    select
      included_count,
      invalid_count,
      duplicate_count,
      manifest_hash,
      jsonb_build_object(
        'predicateVersion', 'published-state-code-100-199:v1',
        'selectionMode', 'manual',
        'processes', selected_processes
      )
    into
      v_included_input_count,
      v_invalid_count,
      v_duplicate_count,
      v_input_manifest_hash,
      v_input_manifest
    from aggregated;

    if v_duplicate_count > 0 then
      return public.lcia_result_error('invalid_process_selection', 400, 'process selection contains duplicate inputs');
    end if;

    if v_invalid_count > 0 then
      return public.lcia_result_error('input_not_eligible', 400, 'All LCIA result package inputs must be published process rows');
    end if;
  end if;

  if coalesce(v_included_input_count, 0) = 0 then
    return public.lcia_result_error('input_empty', 400, 'LCIA result package build requires at least one eligible process');
  end if;

  v_idempotency_key := 'lcia_result.package_build:' || coalesce(
    nullif(trim(coalesce(p_idempotency_key, '')), ''),
    gen_random_uuid()::text
  );

  if nullif(trim(coalesce(p_idempotency_key, '')), '') is not null then
    v_build_id := (
      substr(md5(v_actor::text || ':' || p_idempotency_key), 1, 8) || '-' ||
      substr(md5(v_actor::text || ':' || p_idempotency_key), 9, 4) || '-' ||
      substr(md5(v_actor::text || ':' || p_idempotency_key), 13, 4) || '-' ||
      substr(md5(v_actor::text || ':' || p_idempotency_key), 17, 4) || '-' ||
      substr(md5(v_actor::text || ':' || p_idempotency_key), 21, 12)
    )::uuid;
  else
    v_build_id := gen_random_uuid();
  end if;

  v_request_hash := md5(
    v_input_manifest_hash || '|' ||
    coalesce(p_default_impact_category, '') || '|' ||
    coalesce(p_lcia_method_set::text, '[]')
  );

  v_worker_payload := jsonb_build_object(
    'type', 'lcia_result_package_build',
    'build_id', v_build_id,
    'requested_by', v_actor,
    'name', nullif(trim(coalesce(p_name, '')), ''),
    'coverage_mode', v_coverage_mode,
    'input_status_filter', v_current_manifest->'inputStatusFilter',
    'eligibility_definition', v_current_manifest - 'eligibleInputCount' - 'includedInputCount' - 'inputManifestHash' - 'inputManifest',
    'eligibility_resolved_at', now(),
    'eligible_input_count', v_eligible_input_count,
    'included_input_count', v_included_input_count,
    'input_manifest_hash', v_input_manifest_hash,
    'input_manifest', v_input_manifest,
    'lcia_method_set', coalesce(p_lcia_method_set, '[]'::jsonb),
    'default_impact_category', nullif(trim(coalesce(p_default_impact_category, '')), ''),
    'postprocess_manifest', jsonb_build_object(
      'postprocess_mode', 'skipped',
      'postprocess_reason', 'MVP does not aggregate process results'
    )
  );

  v_worker_job := jsonb_build_object(
    'jobKind', 'lcia_result.package_build',
    'payload', v_worker_payload,
    'payloadSchemaVersion', 'lcia_result.package_build.request.v1',
    'subjectType', 'lcia_result_build',
    'subjectId', v_build_id,
    'subjectVersion', null,
    'requestedBy', v_actor,
    'requesterType', 'operator',
    'idempotencyKey', v_idempotency_key,
    'requestHash', v_request_hash,
    'queueKey', v_build_id,
    'visibility', 'operator'
  );

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_lcia_result_build_request',
    v_actor,
    'worker_jobs',
    v_build_id,
    null,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'coverageMode', v_coverage_mode,
      'eligibleInputCount', v_eligible_input_count,
      'includedInputCount', v_included_input_count,
      'inputManifestHash', v_input_manifest_hash
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'buildId', v_build_id,
      'coverageMode', v_coverage_mode,
      'eligibleInputCount', v_eligible_input_count,
      'includedInputCount', v_included_input_count,
      'inputManifestHash', v_input_manifest_hash,
      'workerJob', v_worker_job
    ),
    'reused', false
  );
exception
  when invalid_text_representation then
    return public.lcia_result_error('invalid_process_selection', 400, 'process ids and versions must be valid');
end;
$$;

create or replace function public.cmd_lcia_result_package_mark_ready(
  p_build_worker_job_id uuid,
  p_package_version text,
  p_snapshot_id uuid,
  p_result_id uuid,
  p_latest_all_unit_result_id uuid default null::uuid,
  p_result_artifact_ref jsonb default '{}'::jsonb,
  p_query_artifact_ref jsonb default '{}'::jsonb,
  p_artifact_manifest jsonb default '{}'::jsonb,
  p_available_impact_categories jsonb default '[]'::jsonb,
  p_default_impact_category text default null::text,
  p_package_result_hash text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_latest public.lca_latest_all_unit_results%rowtype;
  v_package public.lcia_result_packages%rowtype;
  v_result_artifact_ref jsonb;
  v_query_artifact_ref jsonb;
  v_default_impact text;
begin
  if not public.lcia_result_is_service_request() then
    return public.lcia_result_error('service_role_required', 403, 'Service role is required to mark LCIA result packages ready');
  end if;

  if jsonb_typeof(coalesce(p_result_artifact_ref, '{}'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_query_artifact_ref, '{}'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_artifact_manifest, '{}'::jsonb)) <> 'object' then
    return public.lcia_result_error('invalid_artifact_payload', 400, 'artifact payloads must be JSON objects');
  end if;

  if jsonb_typeof(coalesce(p_available_impact_categories, '[]'::jsonb)) <> 'array' then
    return public.lcia_result_error('invalid_available_impact_categories', 400, 'available impact categories must be a JSON array');
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_build_worker_job_id
  for update;

  if v_job.id is null or v_job.job_kind <> 'lcia_result.package_build' then
    return public.lcia_result_error('build_worker_job_not_found', 404, 'LCIA result package build worker job not found');
  end if;

  select *
    into v_result
  from public.lca_results
  where id = p_result_id;

  if v_result.id is null then
    return public.lcia_result_error('result_not_found', 404, 'LCIA result artifact row not found');
  end if;

  if p_latest_all_unit_result_id is not null then
    select *
      into v_latest
    from public.lca_latest_all_unit_results
    where id = p_latest_all_unit_result_id;

    if v_latest.id is null then
      return public.lcia_result_error('latest_all_unit_result_not_found', 404, 'Latest all-unit LCIA result row not found');
    end if;
  end if;

  v_result_artifact_ref := case
    when coalesce(p_result_artifact_ref, '{}'::jsonb) <> '{}'::jsonb then p_result_artifact_ref
    else jsonb_strip_nulls(
      jsonb_build_object(
        'artifactUrl', v_result.artifact_url,
        'artifactSha256', v_result.artifact_sha256,
        'artifactByteSize', v_result.artifact_byte_size,
        'artifactFormat', v_result.artifact_format
      )
    )
  end;

  v_query_artifact_ref := case
    when coalesce(p_query_artifact_ref, '{}'::jsonb) <> '{}'::jsonb then p_query_artifact_ref
    when v_latest.id is not null then jsonb_strip_nulls(
      jsonb_build_object(
        'artifactUrl', v_latest.query_artifact_url,
        'artifactSha256', v_latest.query_artifact_sha256,
        'artifactByteSize', v_latest.query_artifact_byte_size,
        'artifactFormat', v_latest.query_artifact_format
      )
    )
    else '{}'::jsonb
  end;

  if v_result_artifact_ref = '{}'::jsonb then
    return public.lcia_result_error('result_artifact_missing', 400, 'LCIA result package requires a persisted result artifact reference');
  end if;

  v_default_impact := coalesce(
    nullif(trim(coalesce(p_default_impact_category, '')), ''),
    nullif(trim(coalesce(v_job.payload_json->>'default_impact_category', '')), '')
  );

  insert into public.lcia_result_packages (
    build_id,
    build_worker_job_id,
    package_version,
    coverage_mode,
    input_status_filter,
    eligibility_definition,
    eligibility_resolved_at,
    eligible_input_count,
    included_input_count,
    input_manifest_hash,
    input_manifest,
    snapshot_id,
    result_id,
    latest_all_unit_result_id,
    result_artifact_ref,
    query_artifact_ref,
    artifact_manifest,
    package_result_hash,
    lcia_method_set,
    available_impact_categories,
    postprocess_manifest,
    default_impact_category,
    status,
    created_by
  )
  values (
    (v_job.payload_json->>'build_id')::uuid,
    v_job.id,
    nullif(trim(coalesce(p_package_version, '')), ''),
    v_job.payload_json->>'coverage_mode',
    coalesce(v_job.payload_json->'input_status_filter', '{"state_code":{"between":[100,199]}}'::jsonb),
    coalesce(v_job.payload_json->'eligibility_definition', '{}'::jsonb),
    coalesce((v_job.payload_json->>'eligibility_resolved_at')::timestamptz, now()),
    coalesce((v_job.payload_json->>'eligible_input_count')::integer, 0),
    coalesce((v_job.payload_json->>'included_input_count')::integer, 0),
    nullif(v_job.payload_json->>'input_manifest_hash', ''),
    coalesce(v_job.payload_json->'input_manifest', '{}'::jsonb),
    p_snapshot_id,
    v_result.id,
    v_latest.id,
    v_result_artifact_ref,
    v_query_artifact_ref,
    coalesce(p_artifact_manifest, '{}'::jsonb),
    nullif(trim(coalesce(p_package_result_hash, '')), ''),
    coalesce(v_job.payload_json->'lcia_method_set', '[]'::jsonb),
    coalesce(p_available_impact_categories, '[]'::jsonb),
    coalesce(v_job.payload_json->'postprocess_manifest', '{"postprocess_mode":"skipped"}'::jsonb),
    v_default_impact,
    'preview_ready',
    v_job.requested_by
  )
  returning *
    into v_package;

  if v_job.status in ('queued', 'running', 'waiting', 'stale') then
    update public.worker_jobs
      set result_ref = coalesce(result_ref, '{}'::jsonb) || jsonb_build_object(
            'packageId', v_package.id,
            'packageVersion', v_package.package_version
          ),
          updated_at = now()
    where id = v_job.id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'status', v_package.status,
      'buildWorkerJobId', v_package.build_worker_job_id,
      'includedInputCount', v_package.included_input_count
    )
  );
exception
  when unique_violation then
    return public.lcia_result_error('package_conflict', 409, 'LCIA result package already exists for this build or package version');
  when foreign_key_violation then
    return public.lcia_result_error('package_reference_invalid', 400, 'LCIA result package references invalid worker, snapshot, or result rows');
  when invalid_text_representation then
    return public.lcia_result_error('invalid_package_payload', 400, 'LCIA result package payload contains invalid ids or numeric values');
  when not_null_violation then
    return public.lcia_result_error('invalid_package_payload', 400, 'LCIA result package payload is missing required fields');
  when check_violation then
    return public.lcia_result_error('invalid_package_payload', 400, 'LCIA result package payload violates schema constraints');
end;
$$;

create or replace function public.cmd_lcia_result_package_publish(
  p_package_id uuid,
  p_display_default_impact_category text default null::text,
  p_reason text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_package public.lcia_result_packages%rowtype;
  v_current_manifest jsonb;
  v_default_impact text;
  v_previous_id uuid;
  v_publication public.lcia_result_publications%rowtype;
begin
  if v_actor is null then
    return public.lcia_result_error('auth_required', 401, 'Authentication required');
  end if;

  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select *
    into v_package
  from public.lcia_result_packages
  where id = p_package_id
  for update;

  if v_package.id is null or v_package.status <> 'preview_ready' then
    return public.lcia_result_error('package_not_ready', 400, 'Package must be preview_ready before publish');
  end if;

  if v_package.coverage_mode <> 'global_eligible'
     or v_package.included_input_count <> v_package.eligible_input_count then
    return public.lcia_result_error('package_not_global_eligible', 400, 'Only full global eligible packages can publish as global latest');
  end if;

  v_current_manifest := public.lcia_result_current_eligible_manifest();

  if v_package.eligible_input_count <> (v_current_manifest->>'eligibleInputCount')::integer
     or v_package.input_manifest_hash <> v_current_manifest->>'inputManifestHash' then
    return public.lcia_result_error('package_stale_eligibility', 409, 'Eligible process set changed after package creation');
  end if;

  v_default_impact := coalesce(
    nullif(trim(p_display_default_impact_category), ''),
    v_package.default_impact_category
  );

  if v_default_impact is null then
    return public.lcia_result_error('default_impact_missing', 400, 'Default impact category is required before publication');
  end if;

  if jsonb_array_length(v_package.available_impact_categories) > 0
     and not exists (
       select 1
       from jsonb_array_elements_text(v_package.available_impact_categories) as impact(value)
       where impact.value = v_default_impact
     ) then
    return public.lcia_result_error('default_impact_missing', 400, 'Default impact category is not present in the package impact category list');
  end if;

  if v_package.result_artifact_ref = '{}'::jsonb then
    return public.lcia_result_error('result_artifact_missing', 400, 'Package result artifact is required before publication');
  end if;

  lock table public.lcia_result_publications in exclusive mode;

  update public.lcia_result_publications
    set is_current = false,
        status = 'superseded',
        updated_at = now()
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
  returning id
    into v_previous_id;

  insert into public.lcia_result_publications (
    package_id,
    publication_series_key,
    publication_channel,
    visibility_scope,
    is_current,
    status,
    display_default_impact_category,
    published_by,
    published_at,
    reason
  )
  values (
    v_package.id,
    'global',
    'public',
    'public',
    true,
    'current',
    v_default_impact,
    v_actor,
    now(),
    nullif(trim(coalesce(p_reason, '')), '')
  )
  returning *
    into v_publication;

  update public.lca_results
    set is_pinned = true
  where id = v_package.result_id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_lcia_result_package_publish',
    v_actor,
    'lcia_result_publications',
    v_publication.id,
    v_package.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'packageId', v_package.id,
      'previousPublicationId', v_previous_id,
      'displayDefaultImpactCategory', v_default_impact
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'packageId', v_package.id,
      'previousPublicationId', v_previous_id,
      'isCurrent', v_publication.is_current
    )
  );
exception
  when unique_violation then
    return public.lcia_result_error('latest_conflict', 409, 'Another current publication already exists');
end;
$$;

create or replace function public.cmd_lcia_result_publication_unpublish(
  p_publication_id uuid,
  p_reason text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_publication public.lcia_result_publications%rowtype;
begin
  if v_actor is null then
    return public.lcia_result_error('auth_required', 401, 'Authentication required');
  end if;

  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  update public.lcia_result_publications
    set is_current = false,
        status = 'unpublished',
        unpublished_by = v_actor,
        unpublished_at = now(),
        reason = coalesce(nullif(trim(p_reason), ''), reason),
        updated_at = now()
  where id = p_publication_id
  returning *
    into v_publication;

  if v_publication.id is null then
    return public.lcia_result_error('publication_not_found', 404, 'Publication not found');
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_lcia_result_publication_unpublish',
    v_actor,
    'lcia_result_publications',
    v_publication.id,
    null,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('reason', p_reason)
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'packageId', v_publication.package_id,
      'status', v_publication.status
    )
  );
end;
$$;

create or replace function public.get_lcia_result_package_preview(
  p_package_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_package public.lcia_result_packages%rowtype;
begin
  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select *
    into v_package
  from public.lcia_result_packages
  where id = p_package_id;

  if v_package.id is null then
    return public.lcia_result_error('package_not_found', 404, 'Package not found');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'summary', jsonb_build_object(
        'packageId', v_package.id,
        'packageVersion', v_package.package_version,
        'status', v_package.status,
        'coverageMode', v_package.coverage_mode,
        'eligibleInputCount', v_package.eligible_input_count,
        'includedInputCount', v_package.included_input_count,
        'inputManifestHash', v_package.input_manifest_hash,
        'defaultImpactCategory', v_package.default_impact_category,
        'availableImpactCategories', v_package.available_impact_categories
      ),
      'resultArtifact', v_package.result_artifact_ref,
      'queryArtifact', v_package.query_artifact_ref,
      'artifactManifest', v_package.artifact_manifest,
      'inputManifest', v_package.input_manifest
    )
  );
end;
$$;

create or replace function public.get_published_lcia_result_package(
  p_process_id uuid,
  p_process_version text,
  p_impact_category_id text default null::text
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_publication public.lcia_result_publications%rowtype;
  v_package public.lcia_result_packages%rowtype;
  v_process_in_package boolean := false;
begin
  select *
    into v_publication
  from public.lcia_result_publications
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
    and status = 'current'
  order by published_at desc nulls last, created_at desc
  limit 1;

  if v_publication.id is null then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', null,
        'package', null,
        'resultArtifact', null,
        'queryArtifact', null,
        'rowCount', 0
      )
    );
  end if;

  select *
    into v_package
  from public.lcia_result_packages
  where id = v_publication.package_id
    and status = 'preview_ready';

  if v_package.id is null then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', null,
        'package', null,
        'resultArtifact', null,
        'queryArtifact', null,
        'rowCount', 0
      )
    );
  end if;

  select exists (
    select 1
    from jsonb_array_elements(coalesce(v_package.input_manifest->'processes', '[]'::jsonb)) as process(value)
    where process.value->>'id' = p_process_id::text
      and process.value->>'version' = p_process_version
  )
    into v_process_in_package;

  if not v_process_in_package then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', jsonb_build_object(
          'publicationId', v_publication.id,
          'publicationSeriesKey', v_publication.publication_series_key,
          'publicationChannel', v_publication.publication_channel,
          'visibilityScope', v_publication.visibility_scope,
          'displayDefaultImpactCategory', v_publication.display_default_impact_category,
          'publishedAt', v_publication.published_at
        ),
        'package', null,
        'resultArtifact', null,
        'queryArtifact', null,
        'rowCount', 0
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publication', jsonb_build_object(
        'publicationId', v_publication.id,
        'publicationSeriesKey', v_publication.publication_series_key,
        'publicationChannel', v_publication.publication_channel,
        'visibilityScope', v_publication.visibility_scope,
        'displayDefaultImpactCategory', v_publication.display_default_impact_category,
        'publishedAt', v_publication.published_at
      ),
      'package', jsonb_build_object(
        'packageId', v_package.id,
        'packageVersion', v_package.package_version,
        'defaultImpactCategory', v_package.default_impact_category,
        'availableImpactCategories', v_package.available_impact_categories
      ),
      'process', jsonb_build_object(
        'processId', p_process_id,
        'processVersion', p_process_version,
        'impactCategoryId', p_impact_category_id
      ),
      'resultArtifact', v_package.result_artifact_ref,
      'queryArtifact', v_package.query_artifact_ref,
      'artifactManifest', v_package.artifact_manifest,
      'rowCount', 1
    )
  );
end;
$$;

revoke all on function public.lcia_result_is_service_request() from public;
revoke all on function public.lcia_result_is_manager() from public;
revoke all on function public.lcia_result_error(text, integer, text) from public;
revoke all on function public.lcia_result_current_eligible_manifest() from public;
revoke all on function public.lcia_result_prevent_ready_package_content_update() from public;

revoke all on function public.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb) from public;
revoke all on function public.cmd_lcia_result_package_mark_ready(uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb) from public;
revoke all on function public.cmd_lcia_result_package_publish(uuid, text, text, jsonb) from public;
revoke all on function public.cmd_lcia_result_publication_unpublish(uuid, text, jsonb) from public;
revoke all on function public.get_lcia_result_package_preview(uuid) from public;
revoke all on function public.get_published_lcia_result_package(uuid, text, text) from public;

grant execute on function public.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb) to authenticated;
grant execute on function public.cmd_lcia_result_package_mark_ready(uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb) to service_role;
grant execute on function public.cmd_lcia_result_package_publish(uuid, text, text, jsonb) to authenticated;
grant execute on function public.cmd_lcia_result_publication_unpublish(uuid, text, jsonb) to authenticated;
grant execute on function public.get_lcia_result_package_preview(uuid) to authenticated;
grant execute on function public.get_published_lcia_result_package(uuid, text, text) to anon, authenticated, service_role;

grant execute on function public.lcia_result_is_service_request() to service_role;
grant execute on function public.lcia_result_is_manager() to authenticated, service_role;
