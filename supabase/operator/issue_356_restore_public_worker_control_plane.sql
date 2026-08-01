-- psql-only emergency rollback for Issue #356 Expand.
-- Preconditions are fail closed; this operator action does not mutate the
-- Supabase migration ledger.  Roll forward by reapplying migration 20260801060304.

\set ON_ERROR_STOP on

begin;
set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
  v_physical_count integer;
  v_physical_hash text;
  v_moved_count integer;
  v_moved_definer_count integer;
  v_moved_invoker_count integer;
  v_moved_hash text;
  v_adapter_count integer;
  v_adapter_definer_count integer;
  v_adapter_hash text;
  v_ref_count integer;
  v_ref_definer_count integer;
  v_ref_hash text;
  v_compat_count integer;
  v_compat_hash text;
begin
  if not (
    (select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname='public' and c.relkind='v' and c.relname in
        ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')) = 4
  ) then
    raise exception using errcode='55000', message='Issue 356 rollback requires the exact physical Expand phase';
  end if;

  -- Bind the complete mutable catalog surface of the four physical relations.
  -- OIDs are deliberately excluded because they differ across independent
  -- stacks; every OID-attached property is represented by its canonical name,
  -- definition, ACL, or state instead.
  with targets as (
    select c.oid,c.relname,pg_get_userbyid(c.relowner) owner,c.relkind,
      c.relrowsecurity,c.relforcerowsecurity,c.relreplident,
      coalesce(c.relacl::text,'') acl
    from pg_class c join pg_namespace n on n.oid=c.relnamespace
    where n.nspname='private' and c.relkind='r' and c.relname in
      ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
  ), entries as (
    select jsonb_build_object(
      'name',t.relname,'owner',t.owner,'kind',t.relkind,
      'rls',t.relrowsecurity,'forceRls',t.relforcerowsecurity,
      'replicaIdentity',t.relreplident,'acl',t.acl,
      'columns',(select coalesce(jsonb_agg(jsonb_build_object(
        'num',a.attnum,'name',a.attname,
        'type',format_type(a.atttypid,a.atttypmod),'notNull',a.attnotnull,
        'identity',a.attidentity,'generated',a.attgenerated,
        'default',pg_get_expr(d.adbin,d.adrelid),
        'acl',coalesce(a.attacl::text,'')) order by a.attnum),'[]')
        from pg_attribute a left join pg_attrdef d
          on d.adrelid=a.attrelid and d.adnum=a.attnum
        where a.attrelid=t.oid and a.attnum>0 and not a.attisdropped),
      'constraints',(select coalesce(jsonb_agg(jsonb_build_object(
        'name',x.conname,'type',x.contype,
        'definition',pg_get_constraintdef(x.oid,true),
        'validated',x.convalidated,'deferrable',x.condeferrable,
        'deferred',x.condeferred) order by x.conname),'[]')
        from pg_constraint x where x.conrelid=t.oid),
      'indexes',(select coalesce(jsonb_agg(jsonb_build_object(
        'name',ic.relname,'definition',pg_get_indexdef(i.indexrelid),
        'unique',i.indisunique,'valid',i.indisvalid,'ready',i.indisready,
        'replicaIdentity',i.indisreplident,'clustered',i.indisclustered)
        order by ic.relname),'[]')
        from pg_index i join pg_class ic on ic.oid=i.indexrelid
        where i.indrelid=t.oid),
      'triggers',(select coalesce(jsonb_agg(jsonb_build_object(
        'name',g.tgname,'definition',pg_get_triggerdef(g.oid,true),
        'enabled',g.tgenabled) order by g.tgname),'[]')
        from pg_trigger g where g.tgrelid=t.oid and not g.tgisinternal),
      'policies',(select coalesce(jsonb_agg(jsonb_build_object(
        'name',p.polname,'command',p.polcmd,'permissive',p.polpermissive,
        'roles',p.polroles,'qual',pg_get_expr(p.polqual,p.polrelid),
        'check',pg_get_expr(p.polwithcheck,p.polrelid)) order by p.polname),'[]')
        from pg_policy p where p.polrelid=t.oid),
      'publications',(select coalesce(jsonb_agg(pub.pubname order by pub.pubname),'[]')
        from pg_publication_rel pr join pg_publication pub on pub.oid=pr.prpubid
        where pr.prrelid=t.oid)
    ) entry from targets t
  )
  select count(*),md5(jsonb_agg(entry order by entry->>'name')::text)
    into v_physical_count,v_physical_hash from entries;
  if (v_physical_count,v_physical_hash)
     is distinct from (4,'7c3d286ec5acfe907cf7718ded0727c9') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback physical-relation fingerprint drift: count=%s hash=%s',
      v_physical_count,v_physical_hash
    );
  end if;

  with moved as (
    select jsonb_build_object(
      'signature',p.oid::regprocedure::text,
      'owner',pg_get_userbyid(p.proowner),'acl',coalesce(p.proacl::text,''),
      'config',coalesce(p.proconfig::text,''),'securityDefiner',p.prosecdef,
      'language',l.lanname,'volatility',p.provolatile,'strict',p.proisstrict,
      'parallel',p.proparallel,'definition',pg_get_functiondef(p.oid)
    ) entry, p.prosecdef
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    join pg_language l on l.oid=p.prolang
    where n.nspname='private' and p.proname in (
      'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
      'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
      'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
      'worker_record_job_result','worker_retry_job'
    )
  )
  select count(*), count(*) filter (where prosecdef),
         count(*) filter (where not prosecdef),
         md5(jsonb_agg(entry order by entry->>'signature')::text)
    into v_moved_count, v_moved_definer_count, v_moved_invoker_count, v_moved_hash
  from moved;
  if (v_moved_count,v_moved_definer_count,v_moved_invoker_count,v_moved_hash)
     is distinct from (12,11,1,'8f7ee9db66241c84495b66ce991640ce') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback moved-routine fingerprint drift: count=%s definer=%s invoker=%s hash=%s',
      v_moved_count,v_moved_definer_count,v_moved_invoker_count,v_moved_hash
    );
  end if;

  with adapters as (
    select n.nspname, p.oid::regprocedure::text as signature, p.prosecdef,
      jsonb_build_object(
        'schema',n.nspname,'signature',p.oid::regprocedure::text,
        'owner',pg_get_userbyid(p.proowner),'acl',coalesce(p.proacl::text,''),
        'config',coalesce(p.proconfig::text,''),'securityDefiner',p.prosecdef,
        'definition',pg_get_functiondef(p.oid)
      ) entry
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where (n.nspname='public' and p.proname in (
      'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
      'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
      'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
      'worker_record_job_result','worker_retry_job'
    )) or (n.nspname='api' and p.proname in (
      'worker_cancel_job_v1','worker_claim_jobs_v1','worker_enqueue_job_v1',
      'worker_heartbeat_job_v1','worker_list_jobs_v1',
      'worker_list_jobs_by_concurrency_key_v1','worker_read_job_v1',
      'worker_read_jobs_by_ids_v1','worker_read_latest_job_v1',
      'worker_record_job_result_v1','worker_retry_job_v1'
    ))
  )
  select count(*), count(*) filter (where prosecdef),
         md5(jsonb_agg(entry order by nspname,signature)::text)
    into v_adapter_count,v_adapter_definer_count,v_adapter_hash
  from adapters;
  if (v_adapter_count,v_adapter_definer_count,v_adapter_hash)
     is distinct from (23,0,'22576b4a61cec24ff864a7a793fc7e45') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback adapter fingerprint drift: count=%s definer=%s hash=%s',
      v_adapter_count,v_adapter_definer_count,v_adapter_hash
    );
  end if;

  with refs as (
    select p.oid::regprocedure::text as signature,p.prosecdef,
           pg_get_functiondef(p.oid) as definition
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where p.prokind='f' and n.nspname in ('public','private','api','util','archive')
      and pg_get_functiondef(p.oid) ~
        'private\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
  )
  select count(*),count(*) filter(where prosecdef),
         md5(string_agg(signature||':'||md5(definition),E'\n' order by signature))
    into v_ref_count,v_ref_definer_count,v_ref_hash
  from refs;
  if (v_ref_count,v_ref_definer_count,v_ref_hash)
     is distinct from (66,59,'271afd812e119e19c2e943877c6a2fdd') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback reference fingerprint drift: count=%s definer=%s hash=%s',
      v_ref_count,v_ref_definer_count,v_ref_hash
    );
  end if;

  with views as (
    select jsonb_build_object(
      'name',c.relname,'owner',pg_get_userbyid(c.relowner),
      'acl',coalesce(c.relacl::text,''),
      'columnAcl',(select coalesce(jsonb_agg(jsonb_build_object(
        'num',a.attnum,'name',a.attname,'acl',coalesce(a.attacl::text,''))
        order by a.attnum),'[]') from pg_attribute a
        where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped),
      'options',coalesce(c.reloptions::text,''),
      'definition',pg_get_viewdef(c.oid,true)
    ) entry
    from pg_class c join pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='v' and c.relname in
      ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
  )
  select count(*),md5(jsonb_agg(entry order by entry->>'name')::text)
    into v_compat_count,v_compat_hash from views;
  if (v_compat_count,v_compat_hash)
     is distinct from (4,'a6bf84cca731bf107b98ecc74e226c9d') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback compatibility-view fingerprint drift: count=%s hash=%s',
      v_compat_count,v_compat_hash
    );
  end if;
end
$preflight$;

-- Drop the two public-composite bridges before their view row types.
drop function public.worker_job_payload(public.worker_jobs, boolean);
drop function public.lcia_scope_closure_artifact_lineage_eligible(
  public.lcia_scope_closure_checks, public.worker_job_artifacts, text
);

-- Drop the eleven public and API invoker adapters by the exact identity
-- arguments of the private implementation routines.
do $drop_adapters$
declare r record;
begin
  for r in
    select p.proname, pg_get_function_identity_arguments(p.oid) args
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname in (
      'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
      'worker_list_jobs','worker_list_jobs_by_concurrency_key','worker_read_job',
      'worker_read_jobs_by_ids','worker_read_latest_job','worker_record_job_result','worker_retry_job'
    ) order by p.proname
  loop
    execute format('drop function public.%I(%s)',r.proname,r.args);
    execute format('drop function api.%I(%s)',r.proname||'_v1',r.args);
  end loop;
end
$drop_adapters$;

drop view public.worker_job_artifacts;
drop view public.worker_job_events;
drop view public.worker_jobs;
drop view public.worker_job_kinds;

alter table private.worker_job_kinds set schema public;
alter table private.worker_jobs set schema public;
alter table private.worker_job_events set schema public;
alter table private.worker_job_artifacts set schema public;

-- Restore the predecessor's legacy public-table ACL exactly. The Expand
-- migration deliberately narrows this ACL; rollback must reverse that policy
-- change as well as the namespace move before recreating private pilot views.
-- REVOKE ALL also clears the Expand-only pg_attribute.attacl entries; a table-
-- level GRANT alone would leave those column ACL rows behind and would not be
-- an exact catalog rollback.
revoke all on table public.worker_job_kinds, public.worker_jobs,
  public.worker_job_events, public.worker_job_artifacts
  from public, anon, authenticated, service_role, api_internal_executor;
grant all on table public.worker_job_kinds, public.worker_jobs,
  public.worker_job_events, public.worker_job_artifacts to service_role;
grant select on table public.worker_job_kinds, public.worker_jobs,
  public.worker_job_events, public.worker_job_artifacts to api_internal_executor;

alter function private.worker_cancel_job(uuid,uuid,text) set schema public;
alter function private.worker_claim_jobs(text,text,integer,integer) set schema public;
alter function private.worker_enqueue_job(
  text,jsonb,text,text,uuid,text,uuid,text,uuid,text,text,text,integer,text,
  timestamp with time zone,text,integer,timestamp with time zone,jsonb,uuid,uuid
) set schema public;
alter function private.worker_heartbeat_job(uuid,uuid,text,numeric,jsonb,integer) set schema public;
alter function private.worker_job_payload(public.worker_jobs,boolean) set schema public;
alter function private.worker_list_jobs(uuid,text,uuid,text[],text,integer,boolean) set schema public;
alter function private.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean) set schema public;
alter function private.worker_read_job(uuid,boolean) set schema public;
alter function private.worker_read_jobs_by_ids(uuid[],boolean) set schema public;
alter function private.worker_read_latest_job(uuid,text,uuid,text,text,text[],boolean) set schema public;
alter function private.worker_record_job_result(
  uuid,uuid,text,jsonb,text,jsonb,jsonb,text,text,jsonb,text[],text,boolean
) set schema public;
alter function private.worker_retry_job(uuid,timestamp with time zone,integer,text) set schema public;

-- Restore the exact 62 rewritten bodies symmetrically.  The four allowlisted
-- public invokers were never rewritten and therefore are not in this set.
do $restore_bodies$
declare r record; v_definition text; v_count integer := 0;
begin
  for r in
    select p.oid,pg_get_functiondef(p.oid) definition
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where p.prokind='f' and n.nspname in ('public','api','private','util','archive')
      and pg_get_functiondef(p.oid) ~
        'private\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
    order by p.oid::regprocedure::text
  loop
    v_definition := r.definition;
    v_definition := replace(v_definition,'private.worker_job_artifacts','public.worker_job_artifacts');
    v_definition := replace(v_definition,'private.worker_job_events','public.worker_job_events');
    v_definition := replace(v_definition,'private.worker_job_kinds','public.worker_job_kinds');
    v_definition := replace(v_definition,'private.worker_job_payload','public.worker_job_payload');
    v_definition := replace(v_definition,'private.worker_jobs','public.worker_jobs');
    execute v_definition;
    v_count := v_count + 1;
  end loop;
  if v_count <> 62 then
    raise exception using errcode='55000',
      message=format('Issue 356 rollback routine closure drift: %s',v_count);
  end if;
end
$restore_bodies$;

-- Restore the exact pre-#356 routine configuration.
alter function public.worker_cancel_job(uuid,uuid,text) set search_path=public,pg_temp;
alter function public.worker_claim_jobs(text,text,integer,integer) set search_path=public,pg_temp;
alter function public.worker_enqueue_job(
  text,jsonb,text,text,uuid,text,uuid,text,uuid,text,text,text,integer,text,
  timestamp with time zone,text,integer,timestamp with time zone,jsonb,uuid,uuid
) set search_path=public,pg_temp;
alter function public.worker_heartbeat_job(uuid,uuid,text,numeric,jsonb,integer) set search_path=public,pg_temp;
alter function public.worker_job_payload(public.worker_jobs,boolean) set search_path=public,pg_temp;
alter function public.worker_list_jobs(uuid,text,uuid,text[],text,integer,boolean) set search_path=public,pg_temp;
alter function public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean) set search_path=public,pg_temp;
alter function public.worker_read_job(uuid,boolean) set search_path=public,pg_temp;
alter function public.worker_read_jobs_by_ids(uuid[],boolean) set search_path=public,pg_temp;
alter function public.worker_read_latest_job(uuid,text,uuid,text,text,text[],boolean) set search_path=public,pg_temp;
alter function public.worker_record_job_result(
  uuid,uuid,text,jsonb,text,jsonb,jsonb,text,text,jsonb,text[],text,boolean
) set search_path=public,pg_temp;
alter function public.worker_retry_job(uuid,timestamp with time zone,integer,text) set search_path=public,pg_temp;

-- Re-run the idempotent predecessor Expand to restore its four private views,
-- exact bounded RPC definitions/default ACLs, and residue contract.  Its BEGIN
-- joins this transaction and its COMMIT is the single atomic commit point for
-- the complete rollback.  With ON_ERROR_STOP, any predecessor failure leaves
-- this transaction uncommitted and psql rolls it back on exit.
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
