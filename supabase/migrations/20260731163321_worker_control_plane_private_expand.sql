-- Additive Worker control-plane Expand contract.
--
-- The public relations remain the single physical source of truth during the
-- compatibility window.  Moving their composite types would silently rewrite
-- public.worker_job_payload(public.worker_jobs, boolean) to a private argument
-- type and would invalidate SQL/PLpgSQL bodies that still name public.*.
-- Private security-invoker views therefore provide the schema-qualified Worker
-- contract without dual writes.  A later Contract migration may physically
-- move the tables only after the residue query and exact consumer SHAs prove
-- that the public relation contract has no remaining callers.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

-- Support the bounded concurrency snapshot without filtering and sorting all
-- rows for a job kind. The partial predicate matches the RPC's required key.
create index if not exists worker_jobs_job_kind_concurrency_created_idx
  on public.worker_jobs (job_kind, concurrency_key, created_at desc, id desc)
  where concurrency_key is not null;

create schema if not exists private;
grant usage on schema private to service_role;

create or replace view private.worker_job_kinds
with (security_invoker = true)
as select * from public.worker_job_kinds;

create or replace view private.worker_jobs
with (security_invoker = true)
as select * from public.worker_jobs;

create or replace view private.worker_job_events
with (security_invoker = true)
as select * from public.worker_job_events;

create or replace view private.worker_job_artifacts
with (security_invoker = true)
as select * from public.worker_job_artifacts;

revoke all on private.worker_job_kinds from public, anon, authenticated;
revoke all on private.worker_jobs from public, anon, authenticated;
revoke all on private.worker_job_events from public, anon, authenticated;
revoke all on private.worker_job_artifacts from public, anon, authenticated;

grant select on private.worker_jobs to service_role;
grant update (phase, progress, diagnostics, heartbeat_at, lease_expires_at, updated_at)
  on private.worker_jobs to service_role;
grant select on private.worker_job_artifacts to service_role;
grant insert (job_id, artifact_type, content_type, metadata, visibility)
  on private.worker_job_artifacts to service_role;
-- The artifact-role CHECK constraint evaluates this classifier as the caller.
-- Grant only the transitive execution edge required by the bounded INSERT.
grant execute on function public.lcia_scope_closure_artifact_role(text) to service_role;

comment on view private.worker_jobs is
  'Expand-phase schema-qualified Worker contract over the single public.worker_jobs source of truth; remove the public compatibility relation only in a separately gated Contract migration.';
comment on view private.worker_job_events is
  'Expand-phase schema-qualified Worker event contract over the single public source of truth.';
comment on view private.worker_job_artifacts is
  'Expand-phase schema-qualified Worker artifact contract over the single public source of truth.';
comment on view private.worker_job_kinds is
  'Expand-phase schema-qualified Worker job-kind contract over the single public source of truth.';
-- worker_job_domain_refs is a stable cross-domain UNION ALL projection rather
-- than Worker control-plane storage.  Keep it public and preserve its ACL.

create or replace function public.worker_list_jobs_by_concurrency_key(
  p_job_kind text,
  p_concurrency_key text,
  p_statuses text[],
  p_limit integer default 20,
  p_include_internal boolean default true
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job_kind text := nullif(lower(trim(p_job_kind)), '');
  v_concurrency_key text := nullif(trim(p_concurrency_key), '');
  v_limit integer := coalesce(p_limit, 20);
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object('ok',false,'code','SERVICE_ROLE_REQUIRED','status',403,
      'message','Service role is required to list worker jobs');
  end if;
  if v_job_kind is null then
    return jsonb_build_object('ok',false,'code','INVALID_WORKER_JOB_KIND','status',400,
      'message','job kind is required');
  end if;
  if v_concurrency_key is null then
    return jsonb_build_object('ok',false,'code','INVALID_WORKER_JOB_CONCURRENCY_KEY','status',400,
      'message','concurrency key is required');
  end if;
  if v_limit < 1 or v_limit > 20 then
    return jsonb_build_object('ok',false,'code','INVALID_WORKER_JOB_LIMIT','status',400,
      'message','limit must be between 1 and 20');
  end if;
  if p_statuses is not null and exists (
    select 1 from unnest(p_statuses) status_value
    where status_value not in ('queued','running','waiting','completed','blocked','stale','failed','cancelled')
  ) then
    return jsonb_build_object('ok',false,'code','INVALID_WORKER_JOB_STATUS','status',400,
      'message','statuses contains an unsupported worker job status');
  end if;
  select coalesce(jsonb_agg(public.worker_job_payload(
      jsonb_populate_record(null::public.worker_jobs,to_jsonb(j)),p_include_internal)
    order by j.created_at desc,j.id desc),'[]'::jsonb)
  into v_jobs
  from (
    select *
    from private.worker_jobs
    where job_kind = v_job_kind
      and concurrency_key = v_concurrency_key
      and (p_statuses is null or status = any(p_statuses))
    order by created_at desc,id desc
    limit v_limit
  ) j;
  return jsonb_build_object('ok',true,'data',v_jobs);
end;
$$;

revoke all on function public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean)
  from public, anon, authenticated;
grant execute on function public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean)
  to service_role;
comment on function public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean) is
  'Service-only bounded concurrency snapshot preserving created-at order so consumers can apply expiry policy.';

create or replace function public.worker_read_jobs_by_ids(
  p_job_ids uuid[],
  p_include_internal boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_count integer := coalesce(cardinality(p_job_ids), 0);
  v_jobs jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object('ok',false,'code','SERVICE_ROLE_REQUIRED','status',403,
      'message','Service role is required to read worker jobs');
  end if;
  if v_count > 200 then
    return jsonb_build_object('ok',false,'code','WORKER_JOB_BATCH_LIMIT_EXCEEDED','status',400,
      'message','worker job batch is limited to 200 ids');
  end if;
  with requested as (
    select requested_id, min(ordinality) as first_ordinality
    from unnest(coalesce(p_job_ids,'{}'::uuid[])) with ordinality ids(requested_id, ordinality)
    group by requested_id
  )
  select coalesce(jsonb_agg(public.worker_job_payload(
      jsonb_populate_record(null::public.worker_jobs,to_jsonb(j)),p_include_internal)
    order by requested.first_ordinality),'[]'::jsonb)
  into v_jobs
  from requested
  join private.worker_jobs j on j.id=requested.requested_id;
  return jsonb_build_object('ok',true,'data',v_jobs);
end;
$$;
revoke all on function public.worker_read_jobs_by_ids(uuid[],boolean)
  from public, anon, authenticated;
grant execute on function public.worker_read_jobs_by_ids(uuid[],boolean) to service_role;

-- Stop public schema expansion for objects created in the future.  Existing
-- ACLs are intentionally untouched and every new public API must grant access
-- explicitly in its own migration.
alter default privileges for role postgres in schema public
  revoke all on tables from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public
  revoke all on sequences from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public
  revoke execute on functions from public, anon, authenticated, service_role;
-- The platform-owned supabase_admin defaults cannot be changed by the
-- migration role (Postgres rejects ALTER DEFAULT PRIVILEGES FOR ROLE with
-- SQLSTATE 42501).  They remain an explicit hosted-operator blocker in the
-- residue report; repo-authored migrations create objects as postgres, whose
-- future defaults are closed here.

-- This read-only residue view is the Contract-stage exit gate.  It deliberately
-- exposes catalog names and counts only; it reads no job or business rows.
create or replace view private.worker_control_plane_contract_residue
with (security_invoker = true)
as
with target_functions as (
  select
    n.nspname as function_schema,
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where p.prokind = 'f'
    and n.nspname in ('public', 'private')
    and pg_get_functiondef(p.oid) ~ '\\mpublic\\.worker_(jobs|job_events|job_artifacts|job_kinds|job_domain_refs)\\M'
), public_compatibility_relations as (
  select n.nspname as relation_schema, c.relname as relation_name
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relname in (
      'worker_jobs', 'worker_job_events', 'worker_job_artifacts',
      'worker_job_domain_refs', 'worker_job_kinds'
    )
), platform_owner_defaults as (
  select distinct owner_role.rolname as owner
  from pg_default_acl d
  join pg_namespace n on n.oid = d.defaclnamespace
  join pg_roles owner_role on owner_role.oid = d.defaclrole
  where n.nspname = 'public'
    and owner_role.rolname <> 'postgres'
    and exists (
      select 1
      from aclexplode(d.defaclacl) a
      left join pg_roles grantee_role on grantee_role.oid = a.grantee
      where a.grantee = 0
        or grantee_role.rolname in ('anon', 'authenticated', 'service_role')
    )
)
select jsonb_build_object(
  'contractVersion', 'worker-control-plane.private-expand.v1',
  'migrationVersion', '20260731163321',
  'contractReady', false,
  'reason', 'public compatibility consumers and platform-owner default privileges remain during Expand',
  'platformOwnerDefaultPrivilegesReady', not exists(select 1 from platform_owner_defaults),
  'platformOwnerDefaultPrivilegeOwners',
    (select coalesce(jsonb_agg(owner order by owner),'[]'::jsonb) from platform_owner_defaults),
  'publicCompatibilityRelations',
    (select coalesce(jsonb_agg(to_jsonb(r) order by r.relation_name), '[]'::jsonb)
       from public_compatibility_relations r),
  'publicQualifiedFunctionConsumers',
    (select coalesce(jsonb_agg(to_jsonb(f) order by f.function_schema, f.function_name, f.identity_arguments), '[]'::jsonb)
       from target_functions f)
) as residue;

revoke all on private.worker_control_plane_contract_residue from public, anon, authenticated;
grant select on private.worker_control_plane_contract_residue to service_role;

comment on view private.worker_control_plane_contract_residue is
  'Contract exit-gate inventory. Contract is forbidden until exact consumer SHAs are migrated, compatibility calls are zero, burn-in and rollback windows close, and this catalog residue is explicitly reviewed.';

-- Publish the two additive RPCs to PostgREST immediately after commit.
notify pgrst, 'reload schema';

commit;
