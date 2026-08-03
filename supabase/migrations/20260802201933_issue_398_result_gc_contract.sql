begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
begin
  if not exists (
    select 1
    from supabase_migrations.schema_migrations
    where version = '20260802190427'
  ) then
    raise exception 'issue 398 requires predecessor migration 20260802190427';
  end if;

  if to_regclass('public.lca_results') is null
     or to_regclass('public.lca_result_cache') is null
     or to_regclass('public.lca_latest_all_unit_results') is null
     or to_regclass('public.lcia_result_packages') is null
     or to_regclass('private.worker_jobs') is null then
    raise exception 'issue 398 source relations are incomplete';
  end if;

  if exists (
    select 1
    from pg_attribute
    where attrelid = 'public.lca_results'::regclass
      and attname = 'retention_partition_key'
      and not attisdropped
  ) then
    raise exception 'issue 398 retention partition column already exists outside this migration';
  end if;
end
$preflight$;

do $roles$
declare
  v_role record;
  v_membership record;
  v_collision record;
  v_creator_edge_count integer;
  v_runtime_existed boolean := exists (
    select 1 from pg_roles where rolname = 'lca_worker_runtime'
  );
  v_executor_existed boolean := exists (
    select 1 from pg_roles where rolname = 'lca_result_gc_executor'
  );
begin
  if not v_runtime_existed then
    create role lca_worker_runtime
      nologin inherit nosuperuser nocreatedb nocreaterole nobypassrls;
  end if;
  if not v_executor_existed then
    create role lca_result_gc_executor
      nologin inherit nosuperuser nocreatedb nocreaterole nobypassrls;
  end if;

  for v_role in
    select role_row.rolname,
           role_row.rolsuper,
           role_row.rolcreaterole,
           role_row.rolcreatedb,
           role_row.rolcanlogin,
           role_row.rolbypassrls,
           role_row.rolinherit
    from pg_roles role_row
    where role_row.rolname in (
      'lca_worker_runtime', 'lca_result_gc_executor'
    )
  loop
    if v_role.rolsuper or v_role.rolcreaterole or v_role.rolcreatedb
       or v_role.rolcanlogin or v_role.rolbypassrls
       or not v_role.rolinherit then
      raise exception 'issue 398 role % has unsafe attributes', v_role.rolname;
    end if;
  end loop;

  for v_collision in
    select member_role.rolname as member_name,
           granted_role.rolname as granted_name,
           auth_members.admin_option
    from pg_auth_members auth_members
    join pg_roles member_role on member_role.oid = auth_members.member
    join pg_roles granted_role on granted_role.oid = auth_members.roleid
    where (
      member_role.rolname in (
        'lca_worker_runtime', 'lca_result_gc_executor'
      )
      or granted_role.rolname in (
        'lca_worker_runtime', 'lca_result_gc_executor'
      )
    )
      and not (
        member_role.rolname = 'postgres'
        and granted_role.rolname in (
          'lca_worker_runtime', 'lca_result_gc_executor'
        )
        and auth_members.grantor = 'supabase_admin'::regrole
        and auth_members.admin_option
        and not auth_members.inherit_option
        and not auth_members.set_option
      )
  loop
    raise exception
      'issue 398 preexisting role edge % -> % (admin_option=%) is unsafe',
      v_collision.member_name, v_collision.granted_name,
      v_collision.admin_option;
  end loop;

  select count(*) into v_creator_edge_count
  from pg_auth_members auth_members
  join pg_roles member_role on member_role.oid = auth_members.member
  join pg_roles granted_role on granted_role.oid = auth_members.roleid
  where member_role.rolname = 'postgres'
    and granted_role.rolname in (
      'lca_worker_runtime', 'lca_result_gc_executor'
    )
    and auth_members.grantor = 'supabase_admin'::regrole
    and auth_members.admin_option
    and not auth_members.inherit_option
    and not auth_members.set_option;
  if v_creator_edge_count <> 2 then
    raise exception
      'issue 398 requires two exact non-inheritable creator admin edges';
  end if;

  for v_membership in
    select member_role.rolname as member_name,
           granted_role.rolname as granted_name,
           exists (
             select 1
             from pg_auth_members direct_membership
             where direct_membership.member = member_role.oid
               and direct_membership.roleid = granted_role.oid
               and direct_membership.admin_option
           ) as admin_option
    from pg_roles member_role
    cross join pg_roles granted_role
    where member_role.rolname in (
      'anon', 'authenticated', 'service_role', 'api_internal_executor',
      'lca_worker_runtime', 'lca_result_gc_executor'
    )
      and granted_role.rolname in (
        'lca_worker_runtime', 'lca_result_gc_executor'
      )
      and member_role.rolname <> granted_role.rolname
      and pg_has_role(member_role.oid, granted_role.oid, 'member')
  loop
    raise exception
      'issue 398 protected role membership % -> % (admin_option=%) is unsafe',
      v_membership.member_name, v_membership.granted_name,
      v_membership.admin_option;
  end loop;
end
$roles$;

create schema if not exists private;
grant usage on schema private to lca_result_gc_executor, lca_worker_runtime;
grant usage on schema extensions to lca_result_gc_executor;

alter table public.lca_results
  add column retention_partition_key text;

alter table public.lca_results
  add constraint lca_results_retention_partition_key_chk check (
    retention_partition_key is null
    or retention_partition_key ~ '^[0-9a-f]{64}$'
  ) not valid;

comment on column public.lca_results.retention_partition_key is
  'Issue #398 GC partition hash. NULL means legacy or contract-ineligible. New eligible rows require a preallocated result UUID embedded in a unique immutable locator plus canonical worker request identity.';

create unique index lca_results_gc_locator_uidx
  on public.lca_results (artifact_url)
  where retention_partition_key is not null
    and artifact_url is not null;

create index lca_results_gc_partition_created_idx
  on public.lca_results (retention_partition_key, created_at desc, id desc)
  where retention_partition_key is not null;

create table private.lca_result_gc_control (
  singleton boolean primary key default true check (singleton),
  claims_enabled boolean not null default false,
  enabled_at timestamptz,
  enabled_by text,
  reason text not null default 'issue_398_disabled_by_default',
  updated_at timestamptz not null default now(),
  constraint lca_result_gc_control_enable_metadata_chk check (
    not claims_enabled
    or (
      enabled_at is not null
      and nullif(btrim(enabled_by), '') is not null
      and nullif(btrim(reason), '') is not null
    )
  )
);

insert into private.lca_result_gc_control (
  singleton, claims_enabled, reason
) values (
  true, false, 'issue_398_disabled_by_default'
);

create table private.lca_result_gc_operations (
  operation_id uuid primary key default gen_random_uuid(),
  target_result_id uuid not null,
  live_result_id uuid references public.lca_results(id) on delete set null,
  state text not null default 'claimed',
  generation bigint not null default 1,
  claim_token uuid not null default gen_random_uuid(),
  claimed_by text not null,
  lease_expires_at timestamptz not null,
  retention_partition_key text not null,
  artifact_url text not null,
  artifact_sha256 text not null,
  artifact_byte_size bigint not null,
  artifact_format text not null,
  claimed_at timestamptz not null default now(),
  fenced_at timestamptz,
  finalized_at timestamptz,
  object_outcome text,
  last_error_code text,
  error_count integer not null default 0,
  updated_at timestamptz not null default now(),
  constraint lca_result_gc_operations_state_chk check (
    state in (
      'claimed', 'deleting', 'finalizing', 'finalized', 'ineligible'
    )
  ),
  constraint lca_result_gc_operations_generation_chk check (generation > 0),
  constraint lca_result_gc_operations_worker_chk check (
    nullif(btrim(claimed_by), '') is not null
  ),
  constraint lca_result_gc_operations_locator_chk check (
    nullif(btrim(artifact_url), '') is not null
    and artifact_sha256 ~ '^[0-9a-f]{64}$'
    and artifact_byte_size >= 0
    and nullif(btrim(artifact_format), '') is not null
    and nullif(btrim(retention_partition_key), '') is not null
  ),
  constraint lca_result_gc_operations_phase_chk check (
    (state = 'claimed'
      and live_result_id is not null
      and fenced_at is null
      and finalized_at is null
      and object_outcome is null)
    or (state = 'deleting'
      and live_result_id is not null
      and fenced_at is not null
      and finalized_at is null
      and object_outcome is null)
    or (state = 'finalizing'
      and fenced_at is not null
      and finalized_at is null
      and object_outcome is null)
    or (state = 'finalized'
      and live_result_id is null
      and fenced_at is not null
      and finalized_at is not null
      and object_outcome in ('deleted', 'missing'))
    or (state = 'ineligible'
      and finalized_at is not null
      and object_outcome is null)
  )
);

create unique index lca_result_gc_operations_active_target_uidx
  on private.lca_result_gc_operations (target_result_id)
  where state in ('claimed', 'deleting', 'finalizing');

create index lca_result_gc_operations_claim_queue_idx
  on private.lca_result_gc_operations (
    state, lease_expires_at, claimed_at, operation_id
  )
  where state in ('claimed', 'deleting');

create table private.lca_result_gc_finalize_context (
  backend_pid integer not null,
  transaction_id bigint not null,
  operation_id uuid not null
    references private.lca_result_gc_operations(operation_id)
    on delete cascade,
  claim_token uuid not null,
  created_at timestamptz not null default clock_timestamp(),
  primary key (backend_pid, transaction_id, operation_id)
);

create table private.lca_result_gc_attest_context (
  backend_pid integer not null,
  transaction_id bigint not null,
  result_id uuid not null,
  primary key (backend_pid, transaction_id, result_id)
);

alter table private.lca_result_gc_control enable row level security;
alter table private.lca_result_gc_control force row level security;
alter table private.lca_result_gc_operations enable row level security;
alter table private.lca_result_gc_operations force row level security;
alter table private.lca_result_gc_finalize_context enable row level security;
alter table private.lca_result_gc_finalize_context force row level security;
alter table private.lca_result_gc_attest_context enable row level security;
alter table private.lca_result_gc_attest_context force row level security;

create policy lca_result_gc_control_executor_all
  on private.lca_result_gc_control
  for all to lca_result_gc_executor
  using (true) with check (true);

create policy lca_result_gc_operations_executor_all
  on private.lca_result_gc_operations
  for all to lca_result_gc_executor
  using (true) with check (true);

create policy lca_result_gc_finalize_context_executor_all
  on private.lca_result_gc_finalize_context
  for all to lca_result_gc_executor
  using (true) with check (true);

create policy lca_result_gc_attest_context_executor_all
  on private.lca_result_gc_attest_context
  for all to lca_result_gc_executor
  using (true) with check (true);

create policy lca_results_gc_executor_select
  on public.lca_results
  for select to lca_result_gc_executor
  using (true);

create policy lca_results_gc_executor_delete
  on public.lca_results
  for delete to lca_result_gc_executor
  using (true);

create policy lca_results_gc_executor_update
  on public.lca_results
  for update to lca_result_gc_executor
  using (true) with check (true);

create policy lca_result_cache_gc_executor_select
  on public.lca_result_cache
  for select to lca_result_gc_executor
  using (true);

create policy lca_latest_all_unit_results_gc_executor_select
  on public.lca_latest_all_unit_results
  for select to lca_result_gc_executor
  using (true);

create policy lcia_result_packages_gc_executor_select
  on public.lcia_result_packages
  for select to lca_result_gc_executor
  using (true);

create policy worker_jobs_result_gc_executor_select
  on private.worker_jobs
  for select to lca_result_gc_executor
  using (true);

revoke all on table private.lca_result_gc_control,
  private.lca_result_gc_operations,
  private.lca_result_gc_finalize_context,
  private.lca_result_gc_attest_context from public, anon, authenticated,
  service_role, api_internal_executor, lca_worker_runtime,
  lca_result_gc_executor;

grant select, insert, update, delete
  on private.lca_result_gc_control,
     private.lca_result_gc_operations,
     private.lca_result_gc_finalize_context,
     private.lca_result_gc_attest_context
  to lca_result_gc_executor;
grant select, delete on public.lca_results to lca_result_gc_executor;
grant update (retention_partition_key)
  on public.lca_results to lca_result_gc_executor;
grant select on public.lca_result_cache,
  public.lca_latest_all_unit_results,
  public.lcia_result_packages,
  private.worker_jobs to lca_result_gc_executor;

create function private.lca_result_gc_error(
  p_code text,
  p_status integer,
  p_message text
) returns jsonb
language sql
immutable
security invoker
set search_path = pg_catalog, pg_temp
as $function$
  select pg_catalog.jsonb_build_object(
    'ok', false,
    'code', p_code,
    'status', p_status,
    'message', p_message
  )
$function$;

create function private.lca_result_gc_caller_allowed()
returns boolean
language sql
stable
security invoker
set search_path = pg_catalog, pg_temp
as $function$
  select pg_catalog.pg_has_role(
    session_user,
    'lca_worker_runtime',
    'member'
  )
$function$;

create function private.lca_result_gc_ineligibility_reason(
  p_result_id uuid
) returns text
language sql
stable
security invoker
set search_path = pg_catalog, pg_temp
as $function$
  select case
    when result_row.id is null then 'result_missing'
    when result_row.retention_partition_key is null then 'partition_not_ready'
    when result_row.expires_at is null then 'retention_expiry_missing'
    when result_row.expires_at >= pg_catalog.now() then 'retention_not_expired'
    when result_row.is_pinned then 'result_pinned'
    when nullif(pg_catalog.btrim(result_row.artifact_url), '') is null
      or result_row.artifact_sha256 !~ '^[0-9a-f]{64}$'
      or result_row.artifact_byte_size is null
      or result_row.artifact_byte_size < 0
      or nullif(pg_catalog.btrim(result_row.artifact_format), '') is null
      then 'artifact_identity_incomplete'
    when result_row.artifact_url !~ (
      '^[^?#]*/results/' || result_row.id::text || '/[^?#]+$'
    ) then 'artifact_locator_noncanonical'
    when exists (
      select 1
      from public.lca_results duplicate_row
      where duplicate_row.id <> result_row.id
        and duplicate_row.artifact_url = result_row.artifact_url
    ) then 'artifact_locator_shared'
    when exists (
      select 1
      from public.lca_result_cache cache_row
      where cache_row.result_id = result_row.id
        and cache_row.status in ('pending', 'running', 'ready')
    ) then 'active_cache_reference'
    when exists (
      select 1
      from public.lca_latest_all_unit_results latest_row
      where latest_row.result_id = result_row.id
    ) then 'latest_result_reference'
    when exists (
      select 1
      from public.lcia_result_packages package_row
      where package_row.result_id = result_row.id
         or package_row.latest_all_unit_result_id in (
           select latest_row.id
           from public.lca_latest_all_unit_results latest_row
           where latest_row.result_id = result_row.id
         )
    ) then 'package_reference'
    when not exists (
      select 1
      from public.lca_results newer_row
      where newer_row.retention_partition_key =
            result_row.retention_partition_key
        and (newer_row.created_at, newer_row.id) >
            (result_row.created_at, result_row.id)
    ) then 'partition_newest'
    else null
  end
  from (
    select target.*
    from public.lca_results target
    where target.id = p_result_id
  ) result_row
  right join (select 1) present on true
$function$;

create function private.lca_result_gc_prepare_identity()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
begin
  if tg_op = 'INSERT' and new.retention_partition_key is not null then
    raise exception using
      errcode = '23514',
      message = 'lca_result_gc_attestation_required';
  end if;

  if tg_op = 'UPDATE' and (
    new.retention_partition_key is distinct from old.retention_partition_key
    or (
      old.retention_partition_key is not null and (
        new.id is distinct from old.id
        or new.job_id is distinct from old.job_id
        or new.snapshot_id is distinct from old.snapshot_id
        or new.worker_job_id is distinct from old.worker_job_id
        or new.artifact_url is distinct from old.artifact_url
        or new.artifact_sha256 is distinct from old.artifact_sha256
        or new.artifact_byte_size is distinct from old.artifact_byte_size
        or new.artifact_format is distinct from old.artifact_format
        or new.created_at is distinct from old.created_at
      )
    )
  ) and not exists (
    select 1
    from private.lca_result_gc_attest_context context_row
    where context_row.backend_pid = pg_catalog.pg_backend_pid()
      and context_row.transaction_id = pg_catalog.txid_current()
      and context_row.result_id = old.id
      and old.retention_partition_key is null
      and new.retention_partition_key is not null
  ) then
    raise exception using
      errcode = '23514',
      message = 'lca_result_gc_identity_is_immutable';
  end if;

  return new;
end
$function$;

create function private.worker_lca_result_gc_attest_v1(
  p_result_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_result public.lca_results%rowtype;
  v_requested_by uuid;
  v_request_hash text;
  v_partition_key text;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;

  select result_row.* into v_result
  from public.lca_results result_row
  where result_row.id = p_result_id
  for update;

  if v_result.id is null then
    return private.lca_result_gc_error(
      'result_gc_result_missing', 404, 'result does not exist'
    );
  end if;
  if v_result.retention_partition_key is not null then
    return jsonb_build_object('ok', true, 'outcome', 'replayed',
      'data', jsonb_build_object(
        'resultId', v_result.id,
        'retentionPartitionKey', v_result.retention_partition_key
      ));
  end if;

  select worker_job.requested_by, worker_job.request_hash
    into v_requested_by, v_request_hash
  from private.worker_jobs worker_job
  where worker_job.id = v_result.worker_job_id;

  if v_result.snapshot_id is null
     or v_result.expires_at is null
     or nullif(pg_catalog.btrim(v_result.artifact_url), '') is null
     or v_result.artifact_url !~ (
       '^[^?#]*/results/' || v_result.id::text || '/[^?#]+$'
     )
     or v_result.artifact_sha256 !~ '^[0-9a-f]{64}$'
     or v_result.artifact_byte_size is null
     or v_result.artifact_byte_size < 0
     or nullif(pg_catalog.btrim(v_result.artifact_format), '') is null
     or v_requested_by is null
     or nullif(pg_catalog.btrim(v_request_hash), '') is null then
    return private.lca_result_gc_error(
      'result_gc_attestation_invalid', 409,
      'result lacks canonical locator, expiry, artifact or request identity'
    );
  end if;

  v_partition_key := pg_catalog.encode(
    extensions.digest(
      v_requested_by::text || ':' || v_result.snapshot_id::text || ':' ||
      pg_catalog.btrim(v_request_hash),
      'sha256'
    ),
    'hex'
  );

  insert into private.lca_result_gc_attest_context (
    backend_pid, transaction_id, result_id
  ) values (
    pg_catalog.pg_backend_pid(), pg_catalog.txid_current(), v_result.id
  );

  update public.lca_results result_row
  set retention_partition_key = v_partition_key
  where result_row.id = v_result.id;

  delete from private.lca_result_gc_attest_context context_row
  where context_row.backend_pid = pg_catalog.pg_backend_pid()
    and context_row.transaction_id = pg_catalog.txid_current()
    and context_row.result_id = v_result.id;

  return jsonb_build_object('ok', true, 'outcome', 'attested',
    'data', jsonb_build_object(
      'resultId', v_result.id,
      'retentionPartitionKey', v_partition_key
    ));
end
$function$;

create function private.lca_result_gc_guard_result_write()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_operation_id uuid;
  v_claim_token uuid;
  v_state text;
begin
  begin
    select operation.operation_id, operation.claim_token, operation.state
      into v_operation_id, v_claim_token, v_state
    from private.lca_result_gc_operations operation
    where operation.target_result_id = old.id
      and operation.state in ('claimed', 'deleting', 'finalizing')
    order by operation.claimed_at desc
    limit 1
    for update nowait;
  exception
    when lock_not_available then
      raise exception using
        errcode = '55000',
        message = 'lca_result_gc_fence_blocks_concurrent_write';
  end;

  if v_operation_id is null then
    if tg_op = 'DELETE' then
      if old.retention_partition_key is not null then
        raise exception using
          errcode = '55000',
          message = 'lca_result_gc_attested_delete_requires_finalize';
      end if;
      return old;
    end if;
    return new;
  end if;

  if tg_op = 'DELETE'
     and current_user = 'lca_result_gc_executor'
     and v_state = 'finalizing'
     and exists (
       select 1
       from private.lca_result_gc_finalize_context context_row
       where context_row.backend_pid = pg_catalog.pg_backend_pid()
         and context_row.transaction_id = pg_catalog.txid_current()
         and context_row.operation_id = v_operation_id
         and context_row.claim_token = v_claim_token
     ) then
    return old;
  end if;

  if tg_op = 'DELETE' then
    raise exception using
      errcode = '55000',
      message = 'lca_result_gc_active_operation_blocks_delete';
  end if;

  if v_state in ('deleting', 'finalizing') then
    raise exception using
      errcode = '55000',
      message = 'lca_result_gc_delete_fence_blocks_update';
  end if;

  return new;
end
$function$;

create function private.lca_result_gc_assert_reference_allowed(
  p_result_id uuid
) returns void
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
begin
  if p_result_id is null then
    return;
  end if;

  perform 1
  from private.lca_result_gc_operations operation
  where operation.target_result_id = p_result_id
    and operation.state in ('claimed', 'deleting', 'finalizing')
  order by operation.operation_id
  for update;

  if exists (
    select 1
    from private.lca_result_gc_operations operation
    where operation.target_result_id = p_result_id
      and operation.state in ('deleting', 'finalizing')
  ) then
    raise exception using
      errcode = '55000',
      message = 'lca_result_gc_delete_fence_blocks_reference';
  end if;
end
$function$;

create function private.lca_result_gc_guard_cache_reference()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
begin
  if new.status in ('pending', 'running', 'ready') then
    perform private.lca_result_gc_assert_reference_allowed(new.result_id);
  end if;
  return new;
end
$function$;

create function private.lca_result_gc_guard_latest_reference()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
begin
  perform private.lca_result_gc_assert_reference_allowed(new.result_id);
  return new;
end
$function$;

create function private.lca_result_gc_guard_package_reference()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_latest_result_id uuid;
  v_result_id uuid;
begin
  if new.latest_all_unit_result_id is not null then
    select latest.result_id into v_latest_result_id
    from public.lca_latest_all_unit_results latest
    where latest.id = new.latest_all_unit_result_id;
  end if;
  for v_result_id in
    select distinct target_id
    from unnest(array[new.result_id, v_latest_result_id]) target_id
    where target_id is not null
    order by target_id
  loop
    perform private.lca_result_gc_assert_reference_allowed(v_result_id);
  end loop;
  return new;
end
$function$;

create trigger lca_results_gc_prepare_identity
before insert or update of id, job_id, snapshot_id, worker_job_id, artifact_url,
  artifact_sha256, artifact_byte_size, artifact_format, created_at,
  retention_partition_key
on public.lca_results
for each row execute function private.lca_result_gc_prepare_identity();

create trigger lca_results_gc_write_fence
before update or delete on public.lca_results
for each row execute function private.lca_result_gc_guard_result_write();

create trigger lca_result_cache_gc_reference_fence
before insert or update of result_id, status on public.lca_result_cache
for each row execute function private.lca_result_gc_guard_cache_reference();

create trigger lca_latest_all_unit_results_gc_reference_fence
before insert or update of result_id on public.lca_latest_all_unit_results
for each row execute function private.lca_result_gc_guard_latest_reference();

create trigger lcia_result_packages_gc_reference_fence
before insert or update of result_id, latest_all_unit_result_id
on public.lcia_result_packages
for each row execute function private.lca_result_gc_guard_package_reference();

create function private.worker_lca_result_gc_preview_v1(
  p_limit integer default 100
) returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_enabled boolean;
  v_items jsonb;
  v_null_partition_count bigint;
  v_duplicate_locator_count bigint;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;
  if p_limit is null or p_limit not between 1 and 500 then
    return private.lca_result_gc_error(
      'invalid_result_gc_limit', 400,
      'result GC limit must be between 1 and 500'
    );
  end if;

  select control.claims_enabled into v_enabled
  from private.lca_result_gc_control control
  where control.singleton;

  select count(*) into v_null_partition_count
  from public.lca_results result_row
  where result_row.retention_partition_key is null;

  select count(*) into v_duplicate_locator_count
  from (
    select result_row.artifact_url
    from public.lca_results result_row
    where nullif(btrim(result_row.artifact_url), '') is not null
    group by result_row.artifact_url
    having count(*) > 1
  ) duplicate_locator;

  select coalesce(jsonb_agg(jsonb_build_object(
    'resultId', candidate.id,
    'artifactUrl', candidate.artifact_url,
    'artifactSha256', candidate.artifact_sha256,
    'artifactByteSize', candidate.artifact_byte_size,
    'artifactFormat', candidate.artifact_format,
    'retentionPartitionKey', candidate.retention_partition_key,
    'expiresAt', candidate.expires_at,
    'createdAt', candidate.created_at
  ) order by candidate.expires_at, candidate.created_at, candidate.id), '[]'::jsonb)
  into v_items
  from (
    select result_row.*
    from public.lca_results result_row
    where private.lca_result_gc_ineligibility_reason(result_row.id) is null
      and not exists (
        select 1
        from private.lca_result_gc_operations operation
        where operation.target_result_id = result_row.id
          and operation.state in ('claimed', 'deleting')
      )
    order by result_row.expires_at, result_row.created_at, result_row.id
    limit p_limit
  ) candidate;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'claimsEnabled', coalesce(v_enabled, false),
      'legacyOrIneligiblePartitionCount', v_null_partition_count,
      'duplicateLocatorGroupCount', v_duplicate_locator_count,
      'items', v_items
    )
  );
end
$function$;

create function private.worker_lca_result_gc_claim_v1(
  p_worker_id text,
  p_limit integer default 100,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_now timestamptz := clock_timestamp();
  v_lease_expires_at timestamptz;
  v_recovered_count integer := 0;
  v_recovered_operation_ids uuid[] := array[]::uuid[];
  v_items jsonb;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;
  if nullif(btrim(p_worker_id), '') is null then
    return private.lca_result_gc_error(
      'invalid_result_gc_worker', 400, 'worker id is required'
    );
  end if;
  if p_limit is null or p_limit not between 1 and 500
     or p_lease_seconds is null or p_lease_seconds not between 30 and 3600 then
    return private.lca_result_gc_error(
      'invalid_result_gc_claim_bounds', 400,
      'limit must be 1..500 and lease seconds must be 30..3600'
    );
  end if;
  if not coalesce((
    select control.claims_enabled
    from private.lca_result_gc_control control
    where control.singleton
  ), false) then
    return private.lca_result_gc_error(
      'result_gc_disabled', 409,
      'result GC claims are disabled by rollout control'
    );
  end if;

  v_lease_expires_at := v_now + make_interval(secs => p_lease_seconds);

  with recovery_candidates as (
    select operation.operation_id
    from private.lca_result_gc_operations operation
    where operation.state in ('claimed', 'deleting')
      and operation.lease_expires_at <= v_now
    order by operation.lease_expires_at, operation.claimed_at,
      operation.operation_id
    for update skip locked
    limit p_limit
  ), recovered as (
    update private.lca_result_gc_operations operation
    set generation = operation.generation + 1,
        claim_token = gen_random_uuid(),
        claimed_by = btrim(p_worker_id),
        lease_expires_at = v_lease_expires_at,
        last_error_code = null,
        updated_at = v_now
    from recovery_candidates candidate
    where operation.operation_id = candidate.operation_id
    returning operation.*
  )
  select count(*), coalesce(array_agg(recovered.operation_id), array[]::uuid[])
    into v_recovered_count, v_recovered_operation_ids
  from recovered;

  with recovery_items as (
    select operation.*
    from private.lca_result_gc_operations operation
    where operation.operation_id = any(v_recovered_operation_ids)
  ), candidate_rows as (
    select result_row.id
    from public.lca_results result_row
    where v_recovered_count < p_limit
      and private.lca_result_gc_ineligibility_reason(result_row.id) is null
      and not exists (
        select 1
        from private.lca_result_gc_operations operation
        where operation.target_result_id = result_row.id
          and operation.state in ('claimed', 'deleting')
      )
    order by result_row.expires_at, result_row.created_at, result_row.id
    for update skip locked
    limit greatest(p_limit - v_recovered_count, 0)
  ), inserted as (
    insert into private.lca_result_gc_operations (
      target_result_id,
      live_result_id,
      state,
      generation,
      claim_token,
      claimed_by,
      lease_expires_at,
      retention_partition_key,
      artifact_url,
      artifact_sha256,
      artifact_byte_size,
      artifact_format,
      claimed_at,
      updated_at
    )
    select result_row.id,
           result_row.id,
           'claimed',
           1,
           gen_random_uuid(),
           btrim(p_worker_id),
           v_lease_expires_at,
           result_row.retention_partition_key,
           result_row.artifact_url,
           result_row.artifact_sha256,
           result_row.artifact_byte_size,
           result_row.artifact_format,
           v_now,
           v_now
    from candidate_rows candidate
    join public.lca_results result_row on result_row.id = candidate.id
    returning *
  ), all_items as (
    select recovery.*,
           case when recovery.state = 'deleting'
             then 'delete_recovery' else 'claim_recovery' end as phase
    from recovery_items recovery
    union all
    select inserted.*, 'claimed'::text as phase from inserted
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'operationId', item.operation_id,
    'resultId', item.target_result_id,
    'claimToken', item.claim_token,
    'generation', item.generation,
    'leaseExpiresAt', item.lease_expires_at,
    'phase', item.phase,
    'objectDeleteRequired', item.state = 'deleting',
    'artifactUrl', item.artifact_url,
    'artifactSha256', item.artifact_sha256,
    'artifactByteSize', item.artifact_byte_size,
    'artifactFormat', item.artifact_format,
    'retentionPartitionKey', item.retention_partition_key
  ) order by
    case when item.phase in ('delete_recovery', 'claim_recovery')
      then 0 else 1 end,
    item.claimed_at,
    item.operation_id), '[]'::jsonb)
  into v_items
  from all_items item;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object('items', v_items)
  );
end
$function$;

create function private.worker_lca_result_gc_renew_v1(
  p_operation_id uuid,
  p_claim_token uuid,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_operation private.lca_result_gc_operations%rowtype;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;
  if p_operation_id is null or p_claim_token is null
     or p_lease_seconds is null or p_lease_seconds not between 30 and 3600 then
    return private.lca_result_gc_error(
      'invalid_result_gc_renewal', 400,
      'operation, token and lease seconds 30..3600 are required'
    );
  end if;

  update private.lca_result_gc_operations operation
  set lease_expires_at = clock_timestamp() +
        make_interval(secs => p_lease_seconds),
      updated_at = clock_timestamp()
  where operation.operation_id = p_operation_id
    and operation.claim_token = p_claim_token
    and operation.state in ('claimed', 'deleting')
    and operation.lease_expires_at > clock_timestamp()
  returning operation.* into v_operation;

  if v_operation.operation_id is null then
    return private.lca_result_gc_error(
      'result_gc_claim_invalid', 409,
      'result GC claim is missing, stale or expired'
    );
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'operationId', v_operation.operation_id,
    'claimToken', v_operation.claim_token,
    'generation', v_operation.generation,
    'state', v_operation.state,
    'leaseExpiresAt', v_operation.lease_expires_at
  ));
end
$function$;

create function private.worker_lca_result_gc_fence_v1(
  p_operation_id uuid,
  p_claim_token uuid
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_operation private.lca_result_gc_operations%rowtype;
  v_result public.lca_results%rowtype;
  v_reason text;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;

  select operation.* into v_operation
  from private.lca_result_gc_operations operation
  where operation.operation_id = p_operation_id;

  if v_operation.operation_id is null
     or v_operation.claim_token is distinct from p_claim_token
     or v_operation.state <> 'claimed'
     or v_operation.lease_expires_at <= clock_timestamp() then
    return private.lca_result_gc_error(
      'result_gc_claim_invalid', 409,
      'result GC claim is missing, stale, expired or not claimable'
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(v_operation.retention_partition_key, 398)
  );

  select operation.* into v_operation
  from private.lca_result_gc_operations operation
  where operation.operation_id = p_operation_id
  for update;

  if v_operation.claim_token is distinct from p_claim_token
     or v_operation.state <> 'claimed'
     or v_operation.lease_expires_at <= clock_timestamp() then
    return private.lca_result_gc_error(
      'result_gc_claim_invalid', 409,
      'result GC claim changed while acquiring its partition fence'
    );
  end if;

  perform 1
  from public.lca_results partition_row
  where partition_row.retention_partition_key =
        v_operation.retention_partition_key
  order by partition_row.created_at, partition_row.id
  for update;

  select result_row.* into v_result
  from public.lca_results result_row
  where result_row.id = v_operation.live_result_id
  for update;

  v_reason := private.lca_result_gc_ineligibility_reason(
    v_operation.target_result_id
  );
  if v_reason is null and (
    v_result.id is null
    or v_result.retention_partition_key is distinct from
       v_operation.retention_partition_key
    or v_result.artifact_url is distinct from v_operation.artifact_url
    or v_result.artifact_sha256 is distinct from v_operation.artifact_sha256
    or v_result.artifact_byte_size is distinct from
       v_operation.artifact_byte_size
    or v_result.artifact_format is distinct from v_operation.artifact_format
  ) then
    v_reason := 'artifact_identity_drift';
  end if;

  if v_reason is not null then
    update private.lca_result_gc_operations operation
    set state = 'ineligible',
        finalized_at = clock_timestamp(),
        lease_expires_at = clock_timestamp(),
        last_error_code = v_reason,
        updated_at = clock_timestamp()
    where operation.operation_id = v_operation.operation_id;
    return jsonb_build_object(
      'ok', true,
      'outcome', 'ineligible',
      'data', jsonb_build_object(
        'operationId', v_operation.operation_id,
        'resultId', v_operation.target_result_id,
        'reason', v_reason,
        'objectDeleteRequired', false
      )
    );
  end if;

  update private.lca_result_gc_operations operation
  set state = 'deleting',
      fenced_at = clock_timestamp(),
      updated_at = clock_timestamp()
  where operation.operation_id = v_operation.operation_id
    and operation.state = 'claimed'
    and operation.claim_token = p_claim_token
  returning operation.* into v_operation;

  return jsonb_build_object(
    'ok', true,
    'outcome', 'delete_ready',
    'data', jsonb_build_object(
      'operationId', v_operation.operation_id,
      'resultId', v_operation.target_result_id,
      'claimToken', v_operation.claim_token,
      'generation', v_operation.generation,
      'leaseExpiresAt', v_operation.lease_expires_at,
      'objectDeleteRequired', true,
      'artifactUrl', v_operation.artifact_url,
      'artifactSha256', v_operation.artifact_sha256,
      'artifactByteSize', v_operation.artifact_byte_size,
      'artifactFormat', v_operation.artifact_format
    )
  );
end
$function$;

create function private.worker_lca_result_gc_finalize_v1(
  p_operation_id uuid,
  p_claim_token uuid,
  p_object_outcome text
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_operation private.lca_result_gc_operations%rowtype;
  v_result public.lca_results%rowtype;
  v_reason text;
  v_deleted_id uuid;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;
  if p_object_outcome is null
     or p_object_outcome not in ('deleted', 'missing') then
    return private.lca_result_gc_error(
      'invalid_result_gc_object_outcome', 400,
      'object outcome must be deleted or missing'
    );
  end if;

  select operation.* into v_operation
  from private.lca_result_gc_operations operation
  where operation.operation_id = p_operation_id
  for update;

  if v_operation.state = 'finalized'
     and v_operation.claim_token = p_claim_token then
    return jsonb_build_object(
      'ok', true,
      'outcome', 'replayed',
      'data', jsonb_build_object(
        'operationId', v_operation.operation_id,
        'resultId', v_operation.target_result_id,
        'objectOutcome', v_operation.object_outcome,
        'finalizedAt', v_operation.finalized_at
      )
    );
  end if;

  if v_operation.operation_id is null
     or v_operation.claim_token is distinct from p_claim_token
     or v_operation.state <> 'deleting'
     or v_operation.lease_expires_at <= clock_timestamp() then
    return private.lca_result_gc_error(
      'result_gc_claim_invalid', 409,
      'result GC deleting claim is missing, stale or expired'
    );
  end if;

  select result_row.* into v_result
  from public.lca_results result_row
  where result_row.id = v_operation.live_result_id
  for update;

  v_reason := private.lca_result_gc_ineligibility_reason(
    v_operation.target_result_id
  );
  if v_reason is not null
     or v_result.id is null
     or v_result.retention_partition_key is distinct from
        v_operation.retention_partition_key
     or v_result.artifact_url is distinct from v_operation.artifact_url
     or v_result.artifact_sha256 is distinct from v_operation.artifact_sha256
     or v_result.artifact_byte_size is distinct from
        v_operation.artifact_byte_size
     or v_result.artifact_format is distinct from v_operation.artifact_format then
    update private.lca_result_gc_operations operation
    set last_error_code = coalesce(v_reason, 'artifact_identity_drift'),
        error_count = operation.error_count + 1,
        updated_at = clock_timestamp()
    where operation.operation_id = v_operation.operation_id;
    return private.lca_result_gc_error(
      'result_gc_fence_violated', 409,
      'the fenced result no longer satisfies its frozen eligibility contract'
    );
  end if;

  update private.lca_result_gc_operations operation
  set state = 'finalizing',
      updated_at = clock_timestamp()
  where operation.operation_id = v_operation.operation_id
    and operation.claim_token = p_claim_token
    and operation.state = 'deleting';

  insert into private.lca_result_gc_finalize_context (
    backend_pid, transaction_id, operation_id, claim_token
  ) values (
    pg_catalog.pg_backend_pid(), pg_catalog.txid_current(),
    v_operation.operation_id, p_claim_token
  );

  delete from public.lca_results result_row
  where result_row.id = v_operation.live_result_id
    and result_row.retention_partition_key =
        v_operation.retention_partition_key
    and result_row.artifact_url = v_operation.artifact_url
    and result_row.artifact_sha256 = v_operation.artifact_sha256
    and result_row.artifact_byte_size = v_operation.artifact_byte_size
    and result_row.artifact_format = v_operation.artifact_format
  returning result_row.id into v_deleted_id;

  if v_deleted_id is null then
    raise exception using
      errcode = '40001',
      message = 'lca_result_gc_finalize_compare_and_swap_failed';
  end if;

  update private.lca_result_gc_operations operation
  set state = 'finalized',
      finalized_at = clock_timestamp(),
      object_outcome = p_object_outcome,
      last_error_code = null,
      updated_at = clock_timestamp()
  where operation.operation_id = v_operation.operation_id
    and operation.claim_token = p_claim_token
    and operation.state = 'finalizing';

  delete from private.lca_result_gc_finalize_context context_row
  where context_row.backend_pid = pg_catalog.pg_backend_pid()
    and context_row.transaction_id = pg_catalog.txid_current()
    and context_row.operation_id = v_operation.operation_id;

  return jsonb_build_object(
    'ok', true,
    'outcome', 'finalized',
    'data', jsonb_build_object(
      'operationId', v_operation.operation_id,
      'resultId', v_operation.target_result_id,
      'objectOutcome', p_object_outcome
    )
  );
end
$function$;

create function private.worker_lca_result_gc_fail_v1(
  p_operation_id uuid,
  p_claim_token uuid,
  p_error_code text
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, pg_temp
as $function$
declare
  v_operation private.lca_result_gc_operations%rowtype;
begin
  if not private.lca_result_gc_caller_allowed() then
    return private.lca_result_gc_error(
      'result_gc_worker_role_required', 403,
      'lca_worker_runtime membership is required'
    );
  end if;
  if nullif(btrim(p_error_code), '') is null then
    return private.lca_result_gc_error(
      'invalid_result_gc_failure', 400, 'error code is required'
    );
  end if;

  select operation.* into v_operation
  from private.lca_result_gc_operations operation
  where operation.operation_id = p_operation_id
  for update;

  if v_operation.operation_id is null
     or v_operation.claim_token is distinct from p_claim_token
     or v_operation.state not in ('claimed', 'deleting')
     or v_operation.lease_expires_at <= clock_timestamp() then
    return private.lca_result_gc_error(
      'result_gc_claim_invalid', 409,
      'result GC claim is missing, stale or expired'
    );
  end if;

  if v_operation.state = 'claimed' then
    update private.lca_result_gc_operations operation
    set state = 'ineligible',
        finalized_at = clock_timestamp(),
        lease_expires_at = clock_timestamp(),
        last_error_code = btrim(p_error_code),
        error_count = operation.error_count + 1,
        updated_at = clock_timestamp()
    where operation.operation_id = v_operation.operation_id;
    return jsonb_build_object(
      'ok', true,
      'outcome', 'released',
      'data', jsonb_build_object(
        'operationId', v_operation.operation_id,
        'objectDeleteRequired', false
      )
    );
  end if;

  update private.lca_result_gc_operations operation
  set last_error_code = btrim(p_error_code),
      error_count = operation.error_count + 1,
      updated_at = clock_timestamp()
  where operation.operation_id = v_operation.operation_id;

  return jsonb_build_object(
    'ok', true,
    'outcome', 'retry_required',
    'data', jsonb_build_object(
      'operationId', v_operation.operation_id,
      'objectDeleteRequired', true,
      'leaseExpiresAt', v_operation.lease_expires_at
    )
  );
end
$function$;

revoke all on function private.lca_result_gc_error(text, integer, text),
  private.lca_result_gc_caller_allowed(),
  private.lca_result_gc_ineligibility_reason(uuid),
  private.lca_result_gc_prepare_identity(),
  private.worker_lca_result_gc_attest_v1(uuid),
  private.lca_result_gc_guard_result_write(),
  private.lca_result_gc_assert_reference_allowed(uuid),
  private.lca_result_gc_guard_cache_reference(),
  private.lca_result_gc_guard_latest_reference(),
  private.lca_result_gc_guard_package_reference(),
  private.worker_lca_result_gc_preview_v1(integer),
  private.worker_lca_result_gc_claim_v1(text, integer, integer),
  private.worker_lca_result_gc_renew_v1(uuid, uuid, integer),
  private.worker_lca_result_gc_fence_v1(uuid, uuid),
  private.worker_lca_result_gc_finalize_v1(uuid, uuid, text),
  private.worker_lca_result_gc_fail_v1(uuid, uuid, text)
from public, anon, authenticated, service_role, api_internal_executor,
  lca_worker_runtime;

grant create on schema private to lca_result_gc_executor;
grant lca_result_gc_executor to postgres;

alter function private.lca_result_gc_error(text, integer, text)
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_caller_allowed()
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_ineligibility_reason(uuid)
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_prepare_identity()
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_attest_v1(uuid)
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_guard_result_write()
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_assert_reference_allowed(uuid)
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_guard_cache_reference()
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_guard_latest_reference()
  owner to lca_result_gc_executor;
alter function private.lca_result_gc_guard_package_reference()
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_preview_v1(integer)
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_claim_v1(text, integer, integer)
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_renew_v1(uuid, uuid, integer)
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_fence_v1(uuid, uuid)
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_finalize_v1(uuid, uuid, text)
  owner to lca_result_gc_executor;
alter function private.worker_lca_result_gc_fail_v1(uuid, uuid, text)
  owner to lca_result_gc_executor;

grant execute on function private.worker_lca_result_gc_preview_v1(integer),
  private.worker_lca_result_gc_attest_v1(uuid),
  private.worker_lca_result_gc_claim_v1(text, integer, integer),
  private.worker_lca_result_gc_renew_v1(uuid, uuid, integer),
  private.worker_lca_result_gc_fence_v1(uuid, uuid),
  private.worker_lca_result_gc_finalize_v1(uuid, uuid, text),
  private.worker_lca_result_gc_fail_v1(uuid, uuid, text)
to lca_worker_runtime;

comment on table private.lca_result_gc_control is
  'Issue #398 rollout control. Claims are disabled by default; enabling is a separately recorded operator action after exact Worker/DB qualification.';
comment on table private.lca_result_gc_operations is
  'Issue #398 persistent per-result claim/fence/deleting/finalize state. Deleting is never released; an expired lease rotates generation/token and retries the frozen locator.';
comment on table private.lca_result_gc_attest_context is
  'Issue #398 transaction-local unforgeable capability used only while explicitly attesting a result for GC.';
comment on table private.lca_result_gc_finalize_context is
  'Issue #398 transaction-local unforgeable capability used only during exact result finalization.';
comment on function private.worker_lca_result_gc_attest_v1(uuid) is
  'Issue #398 explicit opt-in attestation. Ordinary inserts and updates never make legacy rows GC eligible.';
comment on function private.worker_lca_result_gc_preview_v1(integer) is
  'Issue #398 non-mutating preview and legacy/duplicate locator audit.';
comment on function private.worker_lca_result_gc_claim_v1(text, integer, integer) is
  'Issue #398 per-item token claim. Recovers expired deleting operations before claiming fresh eligible rows with FOR UPDATE SKIP LOCKED.';
comment on function private.worker_lca_result_gc_renew_v1(uuid, uuid, integer) is
  'Issue #398 current unexpired token lease renewal.';
comment on function private.worker_lca_result_gc_fence_v1(uuid, uuid) is
  'Issue #398 final eligibility recheck and claimed-to-deleting CAS. S3 deletion begins only after this transaction commits.';
comment on function private.worker_lca_result_gc_finalize_v1(uuid, uuid, text) is
  'Issue #398 current-token exact-row finalize; deleted and missing S3 outcomes are equivalent and same-token replay is idempotent.';
comment on function private.worker_lca_result_gc_fail_v1(uuid, uuid, text) is
  'Issue #398 claimed failure releases terminally; deleting failure remains fenced and requires idempotent recovery.';

revoke lca_result_gc_executor from postgres;
revoke create on schema private from lca_result_gc_executor;

do $role_postflight$
declare
  v_creator_edge_count integer;
  v_unsafe_edge_count integer;
begin
  select count(*) into v_creator_edge_count
  from pg_auth_members auth_members
  join pg_roles member_role on member_role.oid = auth_members.member
  join pg_roles granted_role on granted_role.oid = auth_members.roleid
  where member_role.rolname = 'postgres'
    and granted_role.rolname in (
      'lca_worker_runtime', 'lca_result_gc_executor'
    )
    and auth_members.grantor = 'supabase_admin'::regrole
    and auth_members.admin_option
    and not auth_members.inherit_option
    and not auth_members.set_option;

  select count(*) into v_unsafe_edge_count
  from pg_auth_members auth_members
  join pg_roles member_role on member_role.oid = auth_members.member
  join pg_roles granted_role on granted_role.oid = auth_members.roleid
  where (
    member_role.rolname in (
      'lca_worker_runtime', 'lca_result_gc_executor'
    )
    or granted_role.rolname in (
      'lca_worker_runtime', 'lca_result_gc_executor'
    )
  )
    and not (
      member_role.rolname = 'postgres'
      and granted_role.rolname in (
        'lca_worker_runtime', 'lca_result_gc_executor'
      )
      and auth_members.grantor = 'supabase_admin'::regrole
      and auth_members.admin_option
      and not auth_members.inherit_option
      and not auth_members.set_option
    );

  if v_creator_edge_count <> 2 or v_unsafe_edge_count <> 0 then
    raise exception
      'issue 398 role owner transfer did not restore the exact creator-edge baseline';
  end if;
end
$role_postflight$;

notify pgrst, 'reload schema';

commit;
