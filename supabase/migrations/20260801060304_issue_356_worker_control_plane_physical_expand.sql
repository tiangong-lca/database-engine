-- Issue #356: move the Worker control plane to a private physical boundary.
--
-- This is an Expand migration.  The four public relations become explicit-
-- column, security-invoker compatibility views over the same physical OIDs;
-- there is no copy and no dual write.  The twelve exact Worker routines move
-- to private and public/api adapters delegate by fully-qualified name.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
  v_public_physical integer;
  v_private_physical integer;
  v_public_compat integer;
  v_private_pilot integer;
  v_ref_count integer;
  v_ref_definer_count integer;
  v_ref_invoker_count integer;
  v_ref_hash text;
  v_view_count integer;
  v_view_hash text;
begin
  select count(*) filter (where n.nspname = 'public' and c.relkind = 'r'),
         count(*) filter (where n.nspname = 'private' and c.relkind = 'r'),
         count(*) filter (where n.nspname = 'public' and c.relkind = 'v'),
         count(*) filter (where n.nspname = 'private' and c.relkind = 'v')
    into v_public_physical, v_private_physical, v_public_compat, v_private_pilot
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where c.relname in (
    'worker_job_kinds', 'worker_jobs', 'worker_job_events', 'worker_job_artifacts'
  ) and n.nspname in ('public', 'private');

  if v_public_physical = 4 and v_private_pilot = 4
     and v_private_physical = 0 and v_public_compat = 0 then
    with refs as (
      select p.oid::regprocedure::text as signature,
             p.prosecdef,
             pg_get_functiondef(p.oid) as definition
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      where p.prokind = 'f'
        and n.nspname in ('public', 'private', 'api', 'util', 'archive')
        and pg_get_functiondef(p.oid) ~
          'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
    )
    select count(*),
           count(*) filter (where prosecdef),
           count(*) filter (where not prosecdef),
           md5(string_agg(signature || ':' || md5(definition), E'\n' order by signature))
      into v_ref_count, v_ref_definer_count, v_ref_invoker_count, v_ref_hash
    from refs;

    if (v_ref_count, v_ref_definer_count, v_ref_invoker_count, v_ref_hash)
       is distinct from (66, 58, 8, 'd08f58f1f45e2ddbd487ecfef3a9dc25') then
      raise exception using
        errcode = '55000',
        message = format(
          'Issue 356 source routine closure drift: count=%s definer=%s invoker=%s hash=%s',
          v_ref_count, v_ref_definer_count, v_ref_invoker_count, v_ref_hash
        );
    end if;
  elsif not (
    v_public_physical = 0 and v_private_pilot = 0
    and v_private_physical = 4 and v_public_compat = 4
  ) then
    raise exception using
      errcode = '55000',
      message = format(
        'Issue 356 relation phase drift: publicPhysical=%s privatePhysical=%s publicCompat=%s privatePilot=%s',
        v_public_physical, v_private_physical, v_public_compat, v_private_pilot
      );
  end if;

  with boundary_views as (
    select c.relname,
           md5(pg_get_viewdef(c.oid, true)) as definition_hash,
           coalesce(c.relacl::text, '') as acl
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname in (
        'worker_domain_traceability_cutoffs',
        'worker_domain_traceability_violations',
        'worker_job_domain_refs',
        'worker_legacy_lifecycle_audit',
        'worker_legacy_table_retirement_blockers'
      )
  )
  select count(*),
         md5(string_agg(relname || ':' || definition_hash || ':' || acl, E'\n' order by relname))
    into v_view_count, v_view_hash
  from boundary_views;

  if (v_view_count, v_view_hash)
     is distinct from (5, '194b385d637524ee30d124ae9280b1ea') then
    raise exception using
      errcode = '55000',
      message = format(
        'Issue 356 #354 view dependency drift: count=%s hash=%s',
        v_view_count, v_view_hash
      );
  end if;
end
$preflight$;

-- Preserve immutable identity evidence for the physical objects and the five
-- #354 views.  The migration checks this again after every DDL operation.
create temporary table issue_356_relation_before on commit drop as
select c.oid, c.reltype, n.nspname, c.relname, c.relkind, c.relowner,
       c.relrowsecurity, c.relforcerowsecurity, c.relreplident,
       coalesce(c.relacl::text, '') as acl
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and c.relname in (
    'worker_job_kinds', 'worker_jobs', 'worker_job_events', 'worker_job_artifacts'
  ) and c.relkind = 'r';

create temporary table issue_356_view_before on commit drop as
select c.oid, c.relname, md5(pg_get_viewdef(c.oid, true)) as definition_hash,
       coalesce(c.relacl::text, '') as acl
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and c.relname in (
    'worker_domain_traceability_cutoffs',
    'worker_domain_traceability_violations',
    'worker_job_domain_refs',
    'worker_legacy_lifecycle_audit',
    'worker_legacy_table_retirement_blockers'
  );

do $move$
begin
  if to_regclass('public.worker_jobs') is not null
     and exists (
       select 1 from pg_class
       where oid = 'public.worker_jobs'::regclass and relkind = 'r'
     ) then
    drop view private.worker_job_artifacts;
    drop view private.worker_job_events;
    drop view private.worker_jobs;
    drop view private.worker_job_kinds;

    alter table public.worker_job_kinds set schema private;
    alter table public.worker_jobs set schema private;
    alter table public.worker_job_events set schema private;
    alter table public.worker_job_artifacts set schema private;

    alter function public.worker_cancel_job(uuid, uuid, text) set schema private;
    alter function public.worker_claim_jobs(text, text, integer, integer) set schema private;
    alter function public.worker_enqueue_job(
      text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text,
      text, integer, text, timestamp with time zone, text, integer,
      timestamp with time zone, jsonb, uuid, uuid
    ) set schema private;
    alter function public.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer)
      set schema private;
    -- The table move changes the composite argument's namespace without
    -- changing the routine OID.
    alter function public.worker_job_payload(private.worker_jobs, boolean)
      set schema private;
    alter function public.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean)
      set schema private;
    alter function public.worker_list_jobs_by_concurrency_key(text, text, text[], integer, boolean)
      set schema private;
    alter function public.worker_read_job(uuid, boolean) set schema private;
    alter function public.worker_read_jobs_by_ids(uuid[], boolean) set schema private;
    alter function public.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean)
      set schema private;
    alter function public.worker_record_job_result(
      uuid, uuid, text, jsonb, text, jsonb, jsonb, text, text, jsonb,
      text[], text, boolean
    ) set schema private;
    alter function public.worker_retry_job(uuid, timestamp with time zone, integer, text)
      set schema private;
  end if;
end
$move$;

-- Explicit-column compatibility views preserve the old direct-SQL surface for
-- the migration window.  They are security-invoker views: service_role must
-- possess both the view grant and the retained private physical-table grant.
create or replace view public.worker_job_kinds
with (security_invoker = true) as
select
  job_kind, worker_runtime, worker_queue, default_visibility,
  default_priority, default_max_attempts, default_lease_seconds,
  payload_schema_version, result_schema_version, user_visible, description,
  created_at, updated_at, task_center_category, task_center_surface, presenter_key
from private.worker_job_kinds;

create or replace view public.worker_jobs
with (security_invoker = true) as
select
  id, job_kind, worker_runtime, worker_queue, priority, queue_key, root_job_id,
  parent_job_id, subject_type, subject_id, subject_version, requester_type,
  requested_by, team_id, idempotency_key, request_hash, concurrency_key,
  status, phase, progress, visibility, run_after, attempt_count, max_attempts,
  leased_by, lease_token, lease_expires_at, heartbeat_at, timeout_at,
  payload_schema_version, payload_json, payload_ref, result_schema_version,
  result_json, result_ref, diagnostics, error_code, error_message,
  error_details, blocker_codes, resolution_scope, retryable, created_at,
  updated_at, started_at, finished_at, expires_at, cancelled_at, cancelled_by
from private.worker_jobs;

create or replace view public.worker_job_events
with (security_invoker = true) as
select
  id, job_id, event_type, status, phase, progress, worker_id, lease_token,
  message, details, created_at
from private.worker_job_events;

create or replace view public.worker_job_artifacts
with (security_invoker = true) as
select
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata, visibility, created_at, expires_at,
  artifact_role, lifecycle_state, gc_claim_token, gc_claimed_at,
  gc_claim_expires_at, gc_failure_count, gc_last_error, deleted_at,
  gc_cleanup_state
from private.worker_job_artifacts;

revoke all on public.worker_job_kinds, public.worker_jobs,
  public.worker_job_events, public.worker_job_artifacts
from public, anon, authenticated, service_role, api_internal_executor;
-- Spell out the compatibility DML contract.  GRANT ALL on PostgreSQL 17 also
-- includes MAINTAIN, which was never part of the Data API compatibility
-- surface and must not be acquired accidentally by these views.
grant select on public.worker_jobs, public.worker_job_artifacts to service_role;
grant update (phase, progress, diagnostics, heartbeat_at, lease_expires_at, updated_at)
  on public.worker_jobs to service_role;
grant insert (job_id, artifact_type, content_type, metadata, visibility)
  on public.worker_job_artifacts to service_role;

comment on view public.worker_job_kinds is
  'Issue #356 Expand compatibility view; private.worker_job_kinds is the single physical source.';
comment on view public.worker_jobs is
  'Issue #356 Expand compatibility view; private.worker_jobs is the single physical source.';
comment on view public.worker_job_events is
  'Issue #356 Expand compatibility view; private.worker_job_events is the single physical source.';
comment on view public.worker_job_artifacts is
  'Issue #356 Expand compatibility view; private.worker_job_artifacts is the single physical source.';

-- Rewrite every SECURITY DEFINER database body that still names one of the
-- moved Worker objects.  The exact 62/58/4 closure hash above makes this
-- migration-time dynamic rewrite deterministic.  The four util invokers are
-- direct-DB/operator surfaces and are rewritten too.  Four public SECURITY
-- INVOKER functions deliberately remain on compatibility views because
-- granting browser roles private USAGE would violate the schema boundary.
create temporary table issue_356_rewrite_before on commit drop as
select p.oid, p.proowner, p.prolang, p.prorettype, p.prokind, p.provolatile,
       p.proisstrict, p.prosecdef, p.proleakproof, p.proparallel,
       coalesce(p.proconfig::text, '') as config,
       coalesce(p.proacl::text, '') as acl,
       pg_get_functiondef(p.oid) as definition
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where p.prokind = 'f'
  and (p.prosecdef or n.nspname = 'util')
  and n.nspname in ('public', 'private', 'api', 'util', 'archive')
  and pg_get_functiondef(p.oid) ~
    'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)';

do $rewrite$
declare
  r record;
  v_definition text;
begin
  for r in
    select p.oid, p.oid::regprocedure::text as signature,
           pg_get_functiondef(p.oid) as definition
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where p.prokind = 'f'
      and (p.prosecdef or n.nspname = 'util')
      and n.nspname in ('public', 'private', 'api', 'util', 'archive')
      and pg_get_functiondef(p.oid) ~
        'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
    order by p.oid::regprocedure::text
  loop
    v_definition := r.definition;
    v_definition := replace(v_definition, 'public.worker_job_artifacts', 'private.worker_job_artifacts');
    v_definition := replace(v_definition, 'public.worker_job_events', 'private.worker_job_events');
    v_definition := replace(v_definition, 'public.worker_job_kinds', 'private.worker_job_kinds');
    v_definition := replace(v_definition, 'public.worker_job_payload', 'private.worker_job_payload');
    v_definition := replace(v_definition, 'public.worker_jobs', 'private.worker_jobs');
    execute v_definition;
  end loop;
end
$rewrite$;

-- CREATE OR REPLACE must change only the five reviewed schema-qualified
-- references.  OID, owner, ACL, language, return type and execution properties
-- are all part of the fail-closed rewrite contract.
do $rewrite_postflight$
declare
  v_count integer;
  v_bad integer;
begin
  select count(*) into v_count from issue_356_rewrite_before;
  -- A controlled retry sees zero remaining rewritable bodies; the initial
  -- Expand path must see 58 DEFINER + four util invoker routines.
  if v_count not in (0, 62) then
    raise exception using errcode = '55000',
      message = format('Issue 356 rewrite set drift: %s', v_count);
  end if;

  select count(*) into v_bad
  from issue_356_rewrite_before b
  left join pg_proc p on p.oid = b.oid
  where p.oid is null
     or p.proowner is distinct from b.proowner
     or p.prolang is distinct from b.prolang
     or p.prorettype is distinct from b.prorettype
     or p.prokind is distinct from b.prokind
     or p.provolatile is distinct from b.provolatile
     or p.proisstrict is distinct from b.proisstrict
     or p.prosecdef is distinct from b.prosecdef
     or p.proleakproof is distinct from b.proleakproof
     or p.proparallel is distinct from b.proparallel
     or coalesce(p.proconfig::text, '') is distinct from b.config
     or coalesce(p.proacl::text, '') is distinct from b.acl
     or pg_get_functiondef(p.oid) is distinct from
       replace(
         replace(
           replace(
             replace(
               replace(b.definition,
                 'public.worker_job_artifacts', 'private.worker_job_artifacts'),
               'public.worker_job_events', 'private.worker_job_events'),
             'public.worker_job_kinds', 'private.worker_job_kinds'),
           'public.worker_job_payload', 'private.worker_job_payload'),
         'public.worker_jobs', 'private.worker_jobs');
  if v_bad <> 0 then
    raise exception using errcode = '55000',
      message = format('Issue 356 rewritten routine property/definition drift: %s', v_bad);
  end if;
end
$rewrite_postflight$;

-- None of the moved core routines may resolve an unqualified caller-controlled
-- object.  public remains last only for explicitly retained shared helpers;
-- pg_catalog, private, and util are resolved first and pg_temp is absent.
alter function private.worker_cancel_job(uuid, uuid, text)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_claim_jobs(text, text, integer, integer)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_enqueue_job(
  text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text,
  text, integer, text, timestamp with time zone, text, integer,
  timestamp with time zone, jsonb, uuid, uuid
) set search_path = pg_catalog, private, util, public;
alter function private.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_job_payload(private.worker_jobs, boolean)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_list_jobs_by_concurrency_key(text, text, text[], integer, boolean)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_read_job(uuid, boolean)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_read_jobs_by_ids(uuid[], boolean)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean)
  set search_path = pg_catalog, private, util, public;
alter function private.worker_record_job_result(
  uuid, uuid, text, jsonb, text, jsonb, jsonb, text, text, jsonb,
  text[], text, boolean
) set search_path = pg_catalog, private, util, public;
alter function private.worker_retry_job(uuid, timestamp with time zone, integer, text)
  set search_path = pg_catalog, private, util, public;

-- Recreate eleven exact SECURITY INVOKER public compatibility signatures and
-- eleven stable API v1 adapters from the private routines' catalog arguments.
-- The invoker must hold the reviewed private EXECUTE edge; wrappers never hide
-- a privileged implementation from the #333 audit.  $n references avoid any
-- dependency on caller-provided names or search_path.
do $adapters$
declare
  r record;
  v_call_args text;
begin
  for r in
    select p.oid, p.proname, p.pronargs,
           pg_get_function_arguments(p.oid) as arguments
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'private'
      and p.proname in (
        'worker_cancel_job', 'worker_claim_jobs', 'worker_enqueue_job',
        'worker_heartbeat_job', 'worker_list_jobs',
        'worker_list_jobs_by_concurrency_key', 'worker_read_job',
        'worker_read_jobs_by_ids', 'worker_read_latest_job',
        'worker_record_job_result', 'worker_retry_job'
      )
    order by p.proname
  loop
    select string_agg('$' || i::text, ', ' order by i)
      into v_call_args
    from generate_series(1, r.pronargs) as i;

    execute format(
      'create or replace function public.%I(%s) returns jsonb '
      'language sql security invoker set search_path = '''' '
      'as $wrapper$ select private.%I(%s) $wrapper$',
      r.proname, r.arguments, r.proname, v_call_args
    );
    execute format(
      'revoke all on function public.%I(%s) from public, anon, authenticated, service_role, api_internal_executor',
      r.proname, pg_get_function_identity_arguments(r.oid)
    );
    execute format(
      'grant execute on function public.%I(%s) to service_role, api_internal_executor',
      r.proname, pg_get_function_identity_arguments(r.oid)
    );

    execute format(
      'create or replace function api.%I(%s) returns jsonb '
      'language sql security invoker set search_path = '''' '
      'as $adapter$ select private.%I(%s) $adapter$',
      r.proname || '_v1', r.arguments, r.proname, v_call_args
    );
    execute format(
      'revoke all on function api.%I(%s) from public, anon, authenticated, service_role, api_internal_executor',
      r.proname || '_v1', pg_get_function_identity_arguments(r.oid)
    );
    execute format(
      'grant execute on function api.%I(%s) to service_role',
      r.proname || '_v1', pg_get_function_identity_arguments(r.oid)
    );
  end loop;
end
$adapters$;

-- The public compatibility view has a distinct composite type.  Convert it
-- explicitly without leaking internal fields or relying on an implicit cast.
create or replace function public.worker_job_payload(
  p_job public.worker_jobs,
  p_include_internal boolean default false
) returns jsonb
language sql
stable
security invoker
set search_path = ''
as $wrapper$
  select private.worker_job_payload(
    pg_catalog.jsonb_populate_record(
      null::private.worker_jobs,
      pg_catalog.to_jsonb(p_job)
    ),
    p_include_internal
  )
$wrapper$;

revoke all on function public.worker_job_payload(public.worker_jobs, boolean)
  from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function public.worker_job_payload(public.worker_jobs, boolean)
  to service_role, api_internal_executor;

-- Preserve the one non-target composite signature whose argument OID followed
-- worker_job_artifacts into private.  The original routine remains the private-
-- composite implementation; this overload is the public-view bridge.
create or replace function public.lcia_scope_closure_artifact_lineage_eligible(
  p_check public.lcia_scope_closure_checks,
  p_artifact public.worker_job_artifacts,
  p_public_artifact_role text
) returns boolean
language sql
stable
security invoker
set search_path = ''
as $compat$
  select public.lcia_scope_closure_artifact_lineage_eligible(
    p_check,
    pg_catalog.jsonb_populate_record(
      null::private.worker_job_artifacts,
      pg_catalog.to_jsonb(p_artifact)
    ),
    p_public_artifact_role
  )
$compat$;

revoke all on function public.lcia_scope_closure_artifact_lineage_eligible(
  public.lcia_scope_closure_checks, public.worker_job_artifacts, text
) from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function public.lcia_scope_closure_artifact_lineage_eligible(
  public.lcia_scope_closure_checks, public.worker_job_artifacts, text
) to api_internal_executor;

-- Replace the legacy GRANT ALL ACL with the reviewed direct-consumer minimum.
-- State it explicitly so retries converge and no internal executor acquires a
-- relation capability merely because it can execute a bounded adapter.
revoke all on private.worker_job_kinds, private.worker_jobs,
  private.worker_job_events, private.worker_job_artifacts
from public, anon, authenticated, service_role, api_internal_executor;
grant select on private.worker_jobs, private.worker_job_artifacts to service_role;
grant update (phase, progress, diagnostics, heartbeat_at, lease_expires_at, updated_at)
  on private.worker_jobs to service_role;
grant insert (job_id, artifact_type, content_type, metadata, visibility)
  on private.worker_job_artifacts to service_role;

revoke all on function private.worker_cancel_job(uuid, uuid, text),
  private.worker_claim_jobs(text, text, integer, integer),
  private.worker_enqueue_job(
    text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text,
    text, integer, text, timestamp with time zone, text, integer,
    timestamp with time zone, jsonb, uuid, uuid
  ),
  private.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer),
  private.worker_job_payload(private.worker_jobs, boolean),
  private.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean),
  private.worker_list_jobs_by_concurrency_key(text, text, text[], integer, boolean),
  private.worker_read_job(uuid, boolean),
  private.worker_read_jobs_by_ids(uuid[], boolean),
  private.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean),
  private.worker_record_job_result(
    uuid, uuid, text, jsonb, text, jsonb, jsonb, text, text, jsonb,
    text[], text, boolean
  ),
  private.worker_retry_job(uuid, timestamp with time zone, integer, text)
from public, anon, authenticated;
grant execute on function private.worker_cancel_job(uuid, uuid, text),
  private.worker_claim_jobs(text, text, integer, integer),
  private.worker_enqueue_job(
    text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text,
    text, integer, text, timestamp with time zone, text, integer,
    timestamp with time zone, jsonb, uuid, uuid
  ),
  private.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer),
  private.worker_job_payload(private.worker_jobs, boolean),
  private.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean),
  private.worker_list_jobs_by_concurrency_key(text, text, text[], integer, boolean),
  private.worker_read_job(uuid, boolean),
  private.worker_read_jobs_by_ids(uuid[], boolean),
  private.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean),
  private.worker_record_job_result(
    uuid, uuid, text, jsonb, text, jsonb, jsonb, text, text, jsonb,
    text[], text, boolean
  ),
  private.worker_retry_job(uuid, timestamp with time zone, integer, text)
to service_role;
grant execute on function private.worker_job_payload(private.worker_jobs, boolean)
  to api_internal_executor;
grant execute on function private.worker_cancel_job(uuid, uuid, text),
  private.worker_claim_jobs(text, text, integer, integer),
  private.worker_enqueue_job(
    text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text,
    text, integer, text, timestamp with time zone, text, integer,
    timestamp with time zone, jsonb, uuid, uuid
  ),
  private.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer),
  private.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean),
  private.worker_list_jobs_by_concurrency_key(text, text, text[], integer, boolean),
  private.worker_read_job(uuid, boolean),
  private.worker_read_jobs_by_ids(uuid[], boolean),
  private.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean),
  private.worker_record_job_result(
    uuid, uuid, text, jsonb, text, jsonb, jsonb, text, text, jsonb,
    text[], text, boolean
  ),
  private.worker_retry_job(uuid, timestamp with time zone, integer, text)
to api_internal_executor;

-- Rewrite the pilot residue projection to describe the actual physical phase.
create or replace view private.worker_control_plane_contract_residue
with (security_invoker = true) as
with invoker_body_residue as (
  select p.oid::regprocedure::text as signature
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where p.prokind = 'f'
    and not p.prosecdef
    and n.nspname in ('public', 'private', 'api', 'util', 'archive')
    and pg_get_functiondef(p.oid) ~
      'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
), public_compatibility_relations as (
  select c.relname
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'v'
    and c.relname in (
      'worker_job_kinds', 'worker_jobs', 'worker_job_events', 'worker_job_artifacts'
    )
), private_physical_relations as (
  select c.relname
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'private'
    and c.relkind = 'r'
    and c.relname in (
      'worker_job_kinds', 'worker_jobs', 'worker_job_events', 'worker_job_artifacts'
    )
)
select jsonb_build_object(
  'contractVersion', 'worker-control-plane.private-physical-expand.v1',
  'migrationVersion', '20260801060304',
  'contractReady', false,
  'reason', 'public compatibility and owner runtime confirmation remain during Expand',
  'serviceRoleRelationAclContract', 'worker-control-plane.private-minimum.v1',
  'serviceRoleRelationAclEvidence', jsonb_build_array(
    'linancn/tiangong-lca-worker#192',
    'linancn/tiangong-lca-edge-functions#249',
    'linancn/tiangong-lca-edge-functions#250',
    'chukeaa/tiangong-lca-release#9',
    'tiangong-lca/utilities#6',
    'tiangong-lca/database-engine#358'
  ),
  'privatePhysicalRelations',
    (select coalesce(jsonb_agg(relname order by relname), '[]'::jsonb)
       from private_physical_relations),
  'publicCompatibilityRelations',
    (select coalesce(jsonb_agg(relname order by relname), '[]'::jsonb)
       from public_compatibility_relations),
  'securityInvokerCompatibilityBodyResidue',
    (select coalesce(jsonb_agg(signature order by signature), '[]'::jsonb)
       from invoker_body_residue),
  'securityInvokerCompatibilityAllowlist', jsonb_build_array(
    jsonb_build_object(
      'signature', 'public.cmd_dataset_review_submit_gate_payload(dataset_review_submit_gate_runs,text)',
      'reason', 'browser-capable invoker must use the public security-invoker compatibility relation',
      'removalGate', 'tiangong-lca/database-engine#358'
    ),
    jsonb_build_object(
      'signature', 'public.cmd_dataset_review_submit_job_payload(anyelement)',
      'reason', 'browser-capable polymorphic invoker must use the public security-invoker compatibility relation',
      'removalGate', 'tiangong-lca/database-engine#358'
    ),
    jsonb_build_object(
      'signature', 'public.lcia_result_package_bind_closure_certificate()',
      'reason', 'browser-capable invoker must use the public security-invoker compatibility relation',
      'removalGate', 'tiangong-lca/database-engine#358'
    ),
    jsonb_build_object(
      'signature', 'public.worker_job_payload(public.worker_jobs,boolean)',
      'reason', 'public composite bridge must accept the compatibility-view row type',
      'removalGate', 'tiangong-lca/database-engine#358'
    )
  ),
  'ownerRuntimeConfirmationRequired', jsonb_build_array(
    'private.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean)',
    'private.worker_read_jobs_by_ids(uuid[],boolean)',
    'private.worker_read_latest_job(uuid,text,uuid,text,text,text[],boolean)',
    'private.worker_retry_job(uuid,timestamp with time zone,integer,text)'
  )
) as residue;

revoke all on private.worker_control_plane_contract_residue
  from public, anon, authenticated;
grant select on private.worker_control_plane_contract_residue to service_role;

-- Fail the transaction if OIDs, row types, RLS, owner, ACL, or the five #354
-- view contracts moved.  Indexes, constraints and triggers are OID-attached and
-- are asserted in the dedicated pgTAP suite.
do $postflight$
declare
  v_bad integer;
  v_residue_count integer;
  v_residue_hash text;
begin
  select count(*) into v_bad
  from issue_356_relation_before b
  left join pg_class c on c.oid = b.oid
  left join pg_namespace n on n.oid = c.relnamespace
  where n.nspname is distinct from 'private'
     or c.relname is distinct from b.relname
     or c.relkind is distinct from 'r'
     or c.reltype is distinct from b.reltype
     or c.relowner is distinct from b.relowner
     or c.relrowsecurity is distinct from b.relrowsecurity
     or c.relforcerowsecurity is distinct from b.relforcerowsecurity
     or c.relreplident is distinct from b.relreplident;
  if v_bad <> 0 then
    raise exception using errcode = '55000',
      message = format('Issue 356 physical relation identity drift: %s', v_bad);
  end if;

  if not (
    has_table_privilege('service_role', 'private.worker_jobs', 'SELECT')
    and has_column_privilege('service_role', 'private.worker_jobs', 'phase', 'UPDATE')
    and has_column_privilege('service_role', 'private.worker_jobs', 'progress', 'UPDATE')
    and has_column_privilege('service_role', 'private.worker_jobs', 'diagnostics', 'UPDATE')
    and has_column_privilege('service_role', 'private.worker_jobs', 'heartbeat_at', 'UPDATE')
    and has_column_privilege('service_role', 'private.worker_jobs', 'lease_expires_at', 'UPDATE')
    and has_column_privilege('service_role', 'private.worker_jobs', 'updated_at', 'UPDATE')
    and not has_column_privilege('service_role', 'private.worker_jobs', 'status', 'UPDATE')
    and not has_column_privilege('service_role', 'private.worker_jobs', 'lease_token', 'UPDATE')
    and not has_table_privilege('service_role', 'private.worker_jobs', 'INSERT')
    and not has_table_privilege('service_role', 'private.worker_jobs', 'DELETE')
    and has_table_privilege('service_role', 'private.worker_job_artifacts', 'SELECT')
    and has_column_privilege('service_role', 'private.worker_job_artifacts', 'job_id', 'INSERT')
    and has_column_privilege('service_role', 'private.worker_job_artifacts', 'artifact_type', 'INSERT')
    and has_column_privilege('service_role', 'private.worker_job_artifacts', 'content_type', 'INSERT')
    and has_column_privilege('service_role', 'private.worker_job_artifacts', 'metadata', 'INSERT')
    and has_column_privilege('service_role', 'private.worker_job_artifacts', 'visibility', 'INSERT')
    and not has_column_privilege('service_role', 'private.worker_job_artifacts', 'storage_path', 'INSERT')
    and not has_table_privilege('service_role', 'private.worker_job_artifacts', 'UPDATE')
    and not has_table_privilege('service_role', 'private.worker_job_artifacts', 'DELETE')
    and not has_table_privilege('service_role', 'private.worker_job_events', 'SELECT')
    and not has_table_privilege('service_role', 'private.worker_job_kinds', 'SELECT')
    and not has_table_privilege('api_internal_executor', 'private.worker_jobs', 'SELECT')
    and not has_table_privilege('api_internal_executor', 'private.worker_job_artifacts', 'SELECT')
    and not has_table_privilege('api_internal_executor', 'private.worker_job_events', 'SELECT')
    and not has_table_privilege('api_internal_executor', 'private.worker_job_kinds', 'SELECT')
  ) then
    raise exception using errcode = '42501',
      message = 'Issue 356 service role minimum relation ACL drift';
  end if;

  select count(*) into v_bad
  from issue_356_view_before b
  left join pg_class c on c.oid = b.oid
  where md5(pg_get_viewdef(c.oid, true)) is distinct from b.definition_hash
     or coalesce(c.relacl::text, '') is distinct from b.acl;
  if v_bad <> 0 then
    raise exception using errcode = '55000',
      message = format('Issue 356 #354 view OID/definition/ACL drift: %s', v_bad);
  end if;

  with residue as (
    select p.oid::regprocedure::text as signature
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where p.prokind = 'f'
      and not p.prosecdef
      and n.nspname in ('public', 'api', 'private', 'util', 'archive')
      and pg_get_functiondef(p.oid) ~
        'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
  )
  select count(*), md5(string_agg(signature, E'\n' order by signature))
    into v_residue_count, v_residue_hash
  from residue;
  if (v_residue_count, v_residue_hash)
     is distinct from (4, 'cf80b294145deed788b94d0ea6d7c868') then
    raise exception using errcode = '55000',
      message = format(
        'Issue 356 invoker compatibility residue drift: count=%s hash=%s',
        v_residue_count, v_residue_hash
      );
  end if;

  if exists (
    select 1 from pg_roles r
    where r.rolname in ('anon', 'authenticated')
      and (
        has_schema_privilege(r.rolname, 'private', 'USAGE')
        or has_table_privilege(r.rolname, 'private.worker_jobs', 'SELECT')
        or has_table_privilege(r.rolname, 'public.worker_jobs', 'SELECT')
      )
  ) then
    raise exception using errcode = '42501',
      message = 'Issue 356 browser role boundary drift';
  end if;
end
$postflight$;

notify pgrst, 'reload schema';

commit;
