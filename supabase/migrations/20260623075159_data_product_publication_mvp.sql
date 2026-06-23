-- Data product calculation/publication MVP.
-- This migration intentionally keeps data product package facts separate from
-- process publication state and from transient LCA query caches.

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
  'data_product.package_build',
  'calculator',
  'solver',
  'operator',
  0,
  3,
  3600,
  'data_product.package_build.request.v1',
  'data_product.package_build.result.v1',
  false,
  'Builds a data product package from a published-only snapshot, materialized LCIA rows, and pinned/copied artifacts.'
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

create table if not exists public.data_product_runs (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  input_scope jsonb not null default '{}'::jsonb,
  input_status_filter jsonb not null default '{"state_code":{"between":[100,199]}}'::jsonb,
  coverage_mode text not null,
  eligibility_definition jsonb not null default '{}'::jsonb,
  eligibility_resolved_at timestamptz not null default now(),
  eligible_input_count integer not null default 0,
  included_input_count integer not null default 0,
  input_manifest_hash text not null,
  lcia_method_set jsonb not null default '[]'::jsonb,
  default_impact_category text,
  postprocess_mode text not null default 'skipped',
  postprocess_status text not null default 'skipped',
  status text not null default 'created',
  worker_job_id uuid references public.worker_jobs(id) on delete set null,
  idempotency_key text,
  created_by uuid not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint data_product_runs_counts_chk check (
    eligible_input_count >= 0 and included_input_count >= 0
  ),
  constraint data_product_runs_coverage_chk check (
    coverage_mode in ('subset', 'global_eligible')
  ),
  constraint data_product_runs_lcia_method_set_chk check (
    jsonb_typeof(lcia_method_set) = 'array'
  ),
  constraint data_product_runs_status_chk check (
    status in ('created', 'queued', 'computing', 'computed', 'failed', 'deprecated')
  )
);

create unique index if not exists data_product_runs_idempotency_uidx
  on public.data_product_runs (created_by, idempotency_key)
  where idempotency_key is not null;

create table if not exists public.data_product_run_inputs (
  run_id uuid not null references public.data_product_runs(id) on delete cascade,
  process_id uuid not null,
  process_version character(9) not null,
  state_code integer not null,
  ordinal integer not null,
  created_at timestamptz not null default now(),
  primary key (run_id, process_id, process_version),
  constraint data_product_run_inputs_process_fk
    foreign key (process_id, process_version)
    references public.processes(id, version)
    on delete restrict,
  constraint data_product_run_inputs_state_chk check (
    state_code between 100 and 199
  )
);

create table if not exists public.data_product_packages (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references public.data_product_runs(id) on delete restrict,
  package_version text not null,
  coverage_mode text not null,
  eligibility_definition jsonb not null,
  eligibility_resolved_at timestamptz not null,
  eligible_input_count integer not null,
  included_input_count integer not null,
  input_manifest_hash text not null,
  snapshot_id uuid references public.lca_network_snapshots(id) on delete restrict,
  source_result_id uuid references public.lca_results(id) on delete set null,
  package_result_hash text not null,
  lcia_method_set jsonb not null default '[]'::jsonb,
  postprocess_mode text not null default 'skipped',
  postprocess_status text not null default 'skipped',
  default_impact_category text,
  status text not null default 'preview_ready',
  created_by uuid not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint data_product_packages_counts_chk check (
    eligible_input_count >= 0 and included_input_count >= 0
  ),
  constraint data_product_packages_coverage_chk check (
    coverage_mode in ('subset', 'global_eligible')
  ),
  constraint data_product_packages_status_chk check (
    status in ('preview_ready', 'deprecated', 'failed')
  ),
  constraint data_product_packages_lcia_method_set_chk check (
    jsonb_typeof(lcia_method_set) = 'array'
  )
);

create unique index if not exists data_product_packages_package_version_uidx
  on public.data_product_packages (package_version);

create index if not exists data_product_packages_run_idx
  on public.data_product_packages (run_id, created_at desc);

create table if not exists public.data_product_package_items (
  package_id uuid not null references public.data_product_packages(id) on delete cascade,
  process_id uuid not null,
  process_version character(9) not null,
  state_code integer not null,
  ordinal integer not null,
  created_at timestamptz not null default now(),
  primary key (package_id, process_id, process_version),
  constraint data_product_package_items_process_fk
    foreign key (process_id, process_version)
    references public.processes(id, version)
    on delete restrict,
  constraint data_product_package_items_state_chk check (
    state_code between 100 and 199
  )
);

create table if not exists public.data_product_lcia_results (
  id uuid primary key default gen_random_uuid(),
  package_id uuid not null references public.data_product_packages(id) on delete cascade,
  process_id uuid not null,
  process_version character(9) not null,
  impact_category_id text not null,
  impact_category_version text,
  impact_label_snapshot text,
  value numeric not null,
  unit text,
  source_result_id uuid references public.lca_results(id) on delete set null,
  source_artifact_sha256 text,
  created_at timestamptz not null default now(),
  constraint data_product_lcia_results_package_item_fk
    foreign key (package_id, process_id, process_version)
    references public.data_product_package_items(package_id, process_id, process_version)
    on delete cascade
);

create unique index if not exists data_product_lcia_results_unique_row_uidx
  on public.data_product_lcia_results (
    package_id,
    process_id,
    process_version,
    impact_category_id
  );

create index if not exists data_product_lcia_results_public_lookup_idx
  on public.data_product_lcia_results (
    process_id,
    process_version,
    impact_category_id
  );

create table if not exists public.data_product_artifacts (
  id uuid primary key default gen_random_uuid(),
  run_id uuid references public.data_product_runs(id) on delete set null,
  package_id uuid references public.data_product_packages(id) on delete cascade,
  artifact_type text not null,
  storage_ref text not null,
  sha256 text,
  byte_size bigint,
  format text,
  persistence_mode text not null,
  is_persisted boolean not null default false,
  source_result_id uuid references public.lca_results(id) on delete set null,
  snapshot_id uuid references public.lca_network_snapshots(id) on delete set null,
  worker_job_id uuid references public.worker_jobs(id) on delete set null,
  created_at timestamptz not null default now(),
  constraint data_product_artifacts_size_chk check (
    byte_size is null or byte_size >= 0
  ),
  constraint data_product_artifacts_persistence_chk check (
    persistence_mode in ('pinned', 'copied')
  ),
  constraint data_product_artifacts_type_chk check (
    artifact_type in (
      'snapshot',
      'snapshot_index',
      'source_result',
      'query_sidecar',
      'input_manifest',
      'postprocess_manifest',
      'package_result_manifest'
    )
  )
);

create index if not exists data_product_artifacts_package_idx
  on public.data_product_artifacts (package_id, artifact_type);

create table if not exists public.data_product_publications (
  id uuid primary key default gen_random_uuid(),
  package_id uuid not null references public.data_product_packages(id) on delete restrict,
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
  constraint data_product_publications_channel_chk check (
    publication_channel = 'public'
  ),
  constraint data_product_publications_visibility_chk check (
    visibility_scope = 'public'
  ),
  constraint data_product_publications_status_chk check (
    status in ('current', 'superseded', 'unpublished')
  )
);

create unique index if not exists data_product_publications_current_uidx
  on public.data_product_publications (
    publication_series_key,
    publication_channel,
    visibility_scope
  )
  where is_current = true;

create index if not exists data_product_publications_package_idx
  on public.data_product_publications (package_id, created_at desc);

alter table public.data_product_runs enable row level security;
alter table public.data_product_run_inputs enable row level security;
alter table public.data_product_packages enable row level security;
alter table public.data_product_package_items enable row level security;
alter table public.data_product_lcia_results enable row level security;
alter table public.data_product_artifacts enable row level security;
alter table public.data_product_publications enable row level security;

grant all on table public.data_product_runs to service_role;
grant all on table public.data_product_run_inputs to service_role;
grant all on table public.data_product_packages to service_role;
grant all on table public.data_product_package_items to service_role;
grant all on table public.data_product_lcia_results to service_role;
grant all on table public.data_product_artifacts to service_role;
grant all on table public.data_product_publications to service_role;

create or replace function public.data_product_is_service_request()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select coalesce(current_setting('request.jwt.claim.role', true), '') = 'service_role'
     or current_user = 'service_role'
$$;

create or replace function public.data_product_is_manager()
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

create or replace function public.data_product_can_write()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select public.data_product_is_manager()
      or public.data_product_is_service_request()
$$;

create or replace function public.data_product_error(
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

create or replace function public.data_product_current_eligible_manifest()
returns jsonb
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  with eligible as (
    select id, version
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
      ) as input_manifest_hash
    from eligible
  )
  select jsonb_build_object(
    'predicateVersion', 'published-state-code-100-199:v1',
    'inputStatusFilter', jsonb_build_object(
      'state_code',
      jsonb_build_object('between', jsonb_build_array(100, 199))
    ),
    'eligibleInputCount', eligible_count,
    'inputManifestHash', input_manifest_hash
  )
  from aggregated
$$;

create or replace function public.data_product_selected_manifest(p_run_id uuid)
returns jsonb
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  with selected as (
    select process_id, process_version
    from public.data_product_run_inputs
    where run_id = p_run_id
    order by process_id, process_version
  ),
  aggregated as (
    select
      count(*)::integer as included_count,
      md5(
        coalesce(
          string_agg(process_id::text || ':' || process_version, ',' order by process_id, process_version),
          ''
        ) || '|published:100-199:v1'
      ) as input_manifest_hash
    from selected
  )
  select jsonb_build_object(
    'includedInputCount', included_count,
    'inputManifestHash', input_manifest_hash
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

create or replace function public.data_product_prevent_ready_package_content_update()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if old.status = 'preview_ready'
     and (
       old.run_id is distinct from new.run_id
       or old.coverage_mode is distinct from new.coverage_mode
       or old.eligibility_definition is distinct from new.eligibility_definition
       or old.eligibility_resolved_at is distinct from new.eligibility_resolved_at
       or old.eligible_input_count is distinct from new.eligible_input_count
       or old.included_input_count is distinct from new.included_input_count
       or old.input_manifest_hash is distinct from new.input_manifest_hash
       or old.snapshot_id is distinct from new.snapshot_id
       or old.source_result_id is distinct from new.source_result_id
       or old.package_result_hash is distinct from new.package_result_hash
       or old.lcia_method_set is distinct from new.lcia_method_set
       or old.postprocess_mode is distinct from new.postprocess_mode
       or old.postprocess_status is distinct from new.postprocess_status
       or old.default_impact_category is distinct from new.default_impact_category
     ) then
    raise exception 'data_product_package_immutable'
      using errcode = '23514';
  end if;

  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists data_product_packages_prevent_ready_update
  on public.data_product_packages;

create trigger data_product_packages_prevent_ready_update
before update on public.data_product_packages
for each row
execute function public.data_product_prevent_ready_package_content_update();

create or replace function public.cmd_data_product_run_create(
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
  v_current_manifest jsonb;
  v_existing public.data_product_runs%rowtype;
  v_run public.data_product_runs%rowtype;
  v_selected_count integer;
  v_invalid_count integer;
  v_selected_manifest jsonb;
begin
  if not public.data_product_is_manager() then
    return public.data_product_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  if v_actor is null then
    return public.data_product_error('auth_required', 401, 'Authentication required');
  end if;

  if v_coverage_mode not in ('subset', 'global_eligible') then
    return public.data_product_error('invalid_coverage_mode', 400, 'coverage_mode must be subset or global_eligible');
  end if;

  if v_coverage_mode = 'global_eligible'
     and p_processes is not null
     and jsonb_array_length(p_processes) > 0 then
    return public.data_product_error('invalid_coverage_mode', 400, 'global_eligible runs are resolved from the current published input predicate');
  end if;

  if jsonb_typeof(coalesce(p_lcia_method_set, '[]'::jsonb)) <> 'array' then
    return public.data_product_error('invalid_lcia_method_set', 400, 'lcia_method_set must be a JSON array');
  end if;

  if p_processes is not null and jsonb_typeof(p_processes) <> 'array' then
    return public.data_product_error('invalid_process_selection', 400, 'process selection must be a JSON array');
  end if;

  if p_idempotency_key is not null then
    select *
      into v_existing
    from public.data_product_runs
    where created_by = v_actor
      and idempotency_key = p_idempotency_key
    order by created_at desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', true,
        'data', jsonb_build_object(
          'runId', v_existing.id,
          'coverageMode', v_existing.coverage_mode,
          'eligibleInputCount', v_existing.eligible_input_count,
          'includedInputCount', v_existing.included_input_count,
          'inputManifestHash', v_existing.input_manifest_hash
        ),
        'reused', true
      );
    end if;
  end if;

  v_current_manifest := public.data_product_current_eligible_manifest();

  insert into public.data_product_runs (
    name,
    input_scope,
    input_status_filter,
    coverage_mode,
    eligibility_definition,
    eligibility_resolved_at,
    eligible_input_count,
    included_input_count,
    input_manifest_hash,
    lcia_method_set,
    default_impact_category,
    status,
    idempotency_key,
    created_by
  )
  values (
    nullif(trim(coalesce(p_name, '')), ''),
    jsonb_build_object('selectionMode', case when p_processes is null or jsonb_array_length(p_processes) = 0 then 'all_eligible' else 'manual' end),
    v_current_manifest->'inputStatusFilter',
    v_coverage_mode,
    v_current_manifest - 'eligibleInputCount' - 'inputManifestHash',
    now(),
    (v_current_manifest->>'eligibleInputCount')::integer,
    0,
    'pending',
    coalesce(p_lcia_method_set, '[]'::jsonb),
    nullif(trim(coalesce(p_default_impact_category, '')), ''),
    'created',
    nullif(trim(coalesce(p_idempotency_key, '')), ''),
    v_actor
  )
  returning *
    into v_run;

  if p_processes is null or jsonb_array_length(p_processes) = 0 then
    insert into public.data_product_run_inputs (
      run_id,
      process_id,
      process_version,
      state_code,
      ordinal
    )
    select
      v_run.id,
      p.id,
      p.version,
      p.state_code,
      row_number() over (order by p.id, p.version)::integer
    from public.processes as p
    where p.state_code between 100 and 199
    order by p.id, p.version;
  else
    with requested as (
      select
        (item.value->>'id')::uuid as process_id,
        coalesce(item.value->>'version', item.value->>'process_version')::character(9) as process_version,
        item.ordinality::integer as ordinal
      from jsonb_array_elements(p_processes) with ordinality as item(value, ordinality)
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
    )
    insert into public.data_product_run_inputs (
      run_id,
      process_id,
      process_version,
      state_code,
      ordinal
    )
    select
      v_run.id,
      process_id,
      process_version,
      state_code,
      ordinal
    from resolved
    where state_code between 100 and 199;

    with requested as (
      select
        (item.value->>'id')::uuid as process_id,
        coalesce(item.value->>'version', item.value->>'process_version')::character(9) as process_version
      from jsonb_array_elements(p_processes) as item(value)
    )
    select count(*)::integer
      into v_invalid_count
    from requested as r
    left join public.processes as p
      on p.id = r.process_id
     and p.version = r.process_version
    where p.id is null
       or p.state_code is null
       or p.state_code not between 100 and 199;

    if v_invalid_count > 0 then
      delete from public.data_product_runs where id = v_run.id;
      return public.data_product_error('input_not_eligible', 400, 'All data product inputs must be published process rows');
    end if;
  end if;

  select count(*)::integer
    into v_selected_count
  from public.data_product_run_inputs
  where run_id = v_run.id;

  if v_selected_count = 0 then
    delete from public.data_product_runs where id = v_run.id;
    return public.data_product_error('input_empty', 400, 'Data product run requires at least one eligible process');
  end if;

  v_selected_manifest := public.data_product_selected_manifest(v_run.id);

  update public.data_product_runs
    set included_input_count = v_selected_count,
        input_manifest_hash = v_selected_manifest->>'inputManifestHash',
        updated_at = now()
  where id = v_run.id
  returning *
    into v_run;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_data_product_run_create',
    v_actor,
    'data_product_runs',
    v_run.id,
    null,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'coverageMode', v_run.coverage_mode,
      'eligibleInputCount', v_run.eligible_input_count,
      'includedInputCount', v_run.included_input_count
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'runId', v_run.id,
      'coverageMode', v_run.coverage_mode,
      'eligibleInputCount', v_run.eligible_input_count,
      'includedInputCount', v_run.included_input_count,
      'inputManifestHash', v_run.input_manifest_hash
    ),
    'reused', false
  );
exception
  when unique_violation then
    return public.data_product_error('invalid_process_selection', 400, 'process selection contains duplicate inputs');
  when not_null_violation then
    return public.data_product_error('invalid_run_request', 400, 'Data product run request is invalid');
  when invalid_text_representation then
    return public.data_product_error('invalid_process_selection', 400, 'process ids and versions must be valid');
end;
$$;

create or replace function public.cmd_data_product_package_mark_ready(
  p_run_id uuid,
  p_package_version text,
  p_snapshot_id uuid,
  p_source_result_id uuid,
  p_package_result_hash text,
  p_default_impact_category text,
  p_result_rows jsonb default '[]'::jsonb,
  p_artifacts jsonb default '[]'::jsonb,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_run public.data_product_runs%rowtype;
  v_package public.data_product_packages%rowtype;
begin
  if not public.data_product_can_write() then
    return public.data_product_error('not_data_product_manager', 403, 'Data product manager or service role is required');
  end if;

  if jsonb_typeof(coalesce(p_result_rows, '[]'::jsonb)) <> 'array' then
    return public.data_product_error('invalid_result_rows', 400, 'result rows must be a JSON array');
  end if;

  if jsonb_typeof(coalesce(p_artifacts, '[]'::jsonb)) <> 'array' then
    return public.data_product_error('invalid_artifacts', 400, 'artifacts must be a JSON array');
  end if;

  select *
    into v_run
  from public.data_product_runs
  where id = p_run_id
  for update;

  if v_run.id is null then
    return public.data_product_error('run_not_found', 404, 'Data product run not found');
  end if;

  insert into public.data_product_packages (
    run_id,
    package_version,
    coverage_mode,
    eligibility_definition,
    eligibility_resolved_at,
    eligible_input_count,
    included_input_count,
    input_manifest_hash,
    snapshot_id,
    source_result_id,
    package_result_hash,
    lcia_method_set,
    postprocess_mode,
    postprocess_status,
    default_impact_category,
    status,
    created_by
  )
  values (
    v_run.id,
    nullif(trim(coalesce(p_package_version, '')), ''),
    v_run.coverage_mode,
    v_run.eligibility_definition,
    v_run.eligibility_resolved_at,
    v_run.eligible_input_count,
    v_run.included_input_count,
    v_run.input_manifest_hash,
    p_snapshot_id,
    p_source_result_id,
    nullif(trim(coalesce(p_package_result_hash, '')), ''),
    v_run.lcia_method_set,
    v_run.postprocess_mode,
    v_run.postprocess_status,
    nullif(trim(coalesce(p_default_impact_category, v_run.default_impact_category, '')), ''),
    'preview_ready',
    coalesce(v_actor, v_run.created_by)
  )
  returning *
    into v_package;

  insert into public.data_product_package_items (
    package_id,
    process_id,
    process_version,
    state_code,
    ordinal
  )
  select
    v_package.id,
    process_id,
    process_version,
    state_code,
    ordinal
  from public.data_product_run_inputs
  where run_id = v_run.id
  order by ordinal;

  insert into public.data_product_lcia_results (
    package_id,
    process_id,
    process_version,
    impact_category_id,
    impact_category_version,
    impact_label_snapshot,
    value,
    unit,
    source_result_id,
    source_artifact_sha256
  )
  select
    v_package.id,
    (row.value->>'process_id')::uuid,
    coalesce(row.value->>'process_version', row.value->>'version')::character(9),
    row.value->>'impact_category_id',
    nullif(row.value->>'impact_category_version', ''),
    nullif(row.value->>'impact_label_snapshot', ''),
    (row.value->>'value')::numeric,
    nullif(row.value->>'unit', ''),
    nullif(row.value->>'source_result_id', '')::uuid,
    nullif(row.value->>'source_artifact_sha256', '')
  from jsonb_array_elements(coalesce(p_result_rows, '[]'::jsonb)) as row(value);

  insert into public.data_product_artifacts (
    run_id,
    package_id,
    artifact_type,
    storage_ref,
    sha256,
    byte_size,
    format,
    persistence_mode,
    is_persisted,
    source_result_id,
    snapshot_id,
    worker_job_id
  )
  select
    v_run.id,
    v_package.id,
    artifact.value->>'artifact_type',
    artifact.value->>'storage_ref',
    nullif(artifact.value->>'sha256', ''),
    nullif(artifact.value->>'byte_size', '')::bigint,
    nullif(artifact.value->>'format', ''),
    coalesce(nullif(artifact.value->>'persistence_mode', ''), 'copied'),
    coalesce((artifact.value->>'is_persisted')::boolean, false),
    nullif(artifact.value->>'source_result_id', '')::uuid,
    nullif(artifact.value->>'snapshot_id', '')::uuid,
    nullif(artifact.value->>'worker_job_id', '')::uuid
  from jsonb_array_elements(coalesce(p_artifacts, '[]'::jsonb)) as artifact(value);

  update public.data_product_runs
    set status = 'computed',
        updated_at = now()
  where id = v_run.id;

  if v_actor is not null then
    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_data_product_package_mark_ready',
      v_actor,
      'data_product_packages',
      v_package.id,
      v_package.package_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'runId', v_run.id,
        'resultRowCount', jsonb_array_length(coalesce(p_result_rows, '[]'::jsonb)),
        'artifactCount', jsonb_array_length(coalesce(p_artifacts, '[]'::jsonb))
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'status', v_package.status,
      'includedInputCount', v_package.included_input_count
    )
  );
exception
  when unique_violation then
    return public.data_product_error('package_conflict', 409, 'Data product package already exists or contains duplicate result rows');
  when foreign_key_violation then
    return public.data_product_error('package_reference_invalid', 400, 'Data product package references invalid run, process, snapshot, result, or job rows');
  when invalid_text_representation then
    return public.data_product_error('invalid_package_payload', 400, 'Data product package payload contains invalid ids or numeric values');
  when not_null_violation then
    return public.data_product_error('invalid_package_payload', 400, 'Data product package payload is missing required fields');
  when check_violation then
    return public.data_product_error('invalid_package_payload', 400, 'Data product package payload violates schema constraints');
end;
$$;

create or replace function public.cmd_data_product_package_publish(
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
  v_package public.data_product_packages%rowtype;
  v_current_manifest jsonb;
  v_default_impact text;
  v_previous_id uuid;
  v_publication public.data_product_publications%rowtype;
begin
  if not public.data_product_is_manager() then
    return public.data_product_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  if v_actor is null then
    return public.data_product_error('auth_required', 401, 'Authentication required');
  end if;

  select *
    into v_package
  from public.data_product_packages
  where id = p_package_id
  for update;

  if v_package.id is null or v_package.status <> 'preview_ready' then
    return public.data_product_error('package_not_ready', 400, 'Package must be preview_ready before publish');
  end if;

  if v_package.coverage_mode <> 'global_eligible'
     or v_package.included_input_count <> v_package.eligible_input_count then
    return public.data_product_error('package_not_global_eligible', 400, 'Only full global eligible packages can publish as global latest');
  end if;

  v_current_manifest := public.data_product_current_eligible_manifest();

  if v_package.eligible_input_count <> (v_current_manifest->>'eligibleInputCount')::integer
     or v_package.input_manifest_hash <> v_current_manifest->>'inputManifestHash' then
    return public.data_product_error('package_stale_eligibility', 409, 'Eligible process set changed after package creation');
  end if;

  if not exists (
    select 1
    from public.data_product_lcia_results
    where package_id = v_package.id
  ) then
    return public.data_product_error('package_not_ready', 400, 'Package has no materialized LCIA result rows');
  end if;

  v_default_impact := coalesce(
    nullif(trim(p_display_default_impact_category), ''),
    v_package.default_impact_category
  );

  if v_default_impact is null
     or not exists (
       select 1
       from public.data_product_lcia_results
       where package_id = v_package.id
         and impact_category_id = v_default_impact
     ) then
    return public.data_product_error('default_impact_missing', 400, 'Default impact category is not present in package rows');
  end if;

  if not exists (
    select 1
    from public.data_product_artifacts
    where package_id = v_package.id
      and is_persisted = true
  )
  or exists (
    select 1
    from public.data_product_artifacts
    where package_id = v_package.id
      and is_persisted = false
  ) then
    return public.data_product_error('artifact_not_persisted', 400, 'Package artifacts must be pinned or copied before publish');
  end if;

  lock table public.data_product_publications in exclusive mode;

  update public.data_product_publications
    set is_current = false,
        status = 'superseded',
        updated_at = now()
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
  returning id
    into v_previous_id;

  insert into public.data_product_publications (
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
  where id = v_package.source_result_id
     or id in (
       select source_result_id
       from public.data_product_lcia_results
       where package_id = v_package.id
         and source_result_id is not null
     )
     or id in (
       select source_result_id
       from public.data_product_artifacts
       where package_id = v_package.id
         and source_result_id is not null
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
    'cmd_data_product_package_publish',
    v_actor,
    'data_product_publications',
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
    return public.data_product_error('latest_conflict', 409, 'Another current publication already exists');
end;
$$;

create or replace function public.cmd_data_product_package_unpublish(
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
  v_publication public.data_product_publications%rowtype;
begin
  if not public.data_product_is_manager() then
    return public.data_product_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  if v_actor is null then
    return public.data_product_error('auth_required', 401, 'Authentication required');
  end if;

  update public.data_product_publications
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
    return public.data_product_error('publication_not_found', 404, 'Publication not found');
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
    'cmd_data_product_package_unpublish',
    v_actor,
    'data_product_publications',
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

create or replace function public.get_data_product_package_preview(
  p_package_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_package public.data_product_packages%rowtype;
  v_rows jsonb;
begin
  if not public.data_product_is_manager() then
    return public.data_product_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select *
    into v_package
  from public.data_product_packages
  where id = p_package_id;

  if v_package.id is null then
    return public.data_product_error('package_not_found', 404, 'Package not found');
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'processId', process_id,
        'processVersion', process_version,
        'impactCategoryId', impact_category_id,
        'impactCategoryVersion', impact_category_version,
        'impactLabel', impact_label_snapshot,
        'value', value,
        'unit', unit
      )
      order by process_id, process_version, impact_category_id
    ),
    '[]'::jsonb
  )
    into v_rows
  from public.data_product_lcia_results
  where package_id = v_package.id;

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
        'defaultImpactCategory', v_package.default_impact_category
      ),
      'rows', v_rows,
      'rowCount', jsonb_array_length(v_rows)
    )
  );
end;
$$;

create or replace function public.get_published_process_lcia_results(
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
  v_publication public.data_product_publications%rowtype;
  v_package public.data_product_packages%rowtype;
  v_rows jsonb;
begin
  select *
    into v_publication
  from public.data_product_publications
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
        'rows', '[]'::jsonb,
        'rowCount', 0
      )
    );
  end if;

  select *
    into v_package
  from public.data_product_packages
  where id = v_publication.package_id
    and status = 'preview_ready';

  if v_package.id is null then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', null,
        'package', null,
        'rows', '[]'::jsonb,
        'rowCount', 0
      )
    );
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'processId', process_id,
        'processVersion', process_version,
        'impactCategoryId', impact_category_id,
        'impactCategoryVersion', impact_category_version,
        'impactLabel', impact_label_snapshot,
        'value', value,
        'unit', unit
      )
      order by impact_category_id
    ),
    '[]'::jsonb
  )
    into v_rows
  from public.data_product_lcia_results
  where package_id = v_package.id
    and process_id = p_process_id
    and process_version = p_process_version::character(9)
    and (
      p_impact_category_id is null
      or impact_category_id = p_impact_category_id
    );

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
        'defaultImpactCategory', v_package.default_impact_category
      ),
      'rows', v_rows,
      'rowCount', jsonb_array_length(v_rows)
    )
  );
end;
$$;

revoke all on function public.data_product_is_service_request() from public;
revoke all on function public.data_product_is_manager() from public;
revoke all on function public.data_product_can_write() from public;
revoke all on function public.data_product_error(text, integer, text) from public;
revoke all on function public.data_product_current_eligible_manifest() from public;
revoke all on function public.data_product_selected_manifest(uuid) from public;
revoke all on function public.data_product_prevent_ready_package_content_update() from public;

revoke all on function public.cmd_data_product_run_create(text, jsonb, text, text, jsonb, text, jsonb) from public;
revoke all on function public.cmd_data_product_package_mark_ready(uuid, text, uuid, uuid, text, text, jsonb, jsonb, jsonb) from public;
revoke all on function public.cmd_data_product_package_publish(uuid, text, text, jsonb) from public;
revoke all on function public.cmd_data_product_package_unpublish(uuid, text, jsonb) from public;
revoke all on function public.get_data_product_package_preview(uuid) from public;
revoke all on function public.get_published_process_lcia_results(uuid, text, text) from public;

grant execute on function public.cmd_data_product_run_create(text, jsonb, text, text, jsonb, text, jsonb) to authenticated;
grant execute on function public.cmd_data_product_package_mark_ready(uuid, text, uuid, uuid, text, text, jsonb, jsonb, jsonb) to authenticated, service_role;
grant execute on function public.cmd_data_product_package_publish(uuid, text, text, jsonb) to authenticated;
grant execute on function public.cmd_data_product_package_unpublish(uuid, text, jsonb) to authenticated;
grant execute on function public.get_data_product_package_preview(uuid) to authenticated;
grant execute on function public.get_published_process_lcia_results(uuid, text, text) to anon, authenticated, service_role;

grant execute on function public.data_product_is_service_request() to service_role;
grant execute on function public.data_product_is_manager() to authenticated, service_role;
grant execute on function public.data_product_can_write() to authenticated, service_role;
