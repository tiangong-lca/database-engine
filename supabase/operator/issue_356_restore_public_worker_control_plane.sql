-- psql-only emergency rollback for Issue #356 Expand.
-- Preconditions are fail closed; this operator action does not mutate the
-- Supabase migration ledger.  Roll forward by reapplying migration 20260801060304.

\set ON_ERROR_STOP on

begin;
set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
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
      where n.nspname='private' and c.relkind='r' and c.relname in
        ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')) = 4
    and
    (select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname='public' and c.relkind='v' and c.relname in
        ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')) = 4
  ) then
    raise exception using errcode='55000', message='Issue 356 rollback requires the exact physical Expand phase';
  end if;

  with moved as (
    select p.oid::regprocedure::text as signature, p.prosecdef,
           pg_get_functiondef(p.oid) as definition
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname in (
      'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
      'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
      'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
      'worker_record_job_result','worker_retry_job'
    )
  )
  select count(*), count(*) filter (where prosecdef),
         count(*) filter (where not prosecdef),
         md5(string_agg(signature||':'||md5(definition),E'\n' order by signature))
    into v_moved_count, v_moved_definer_count, v_moved_invoker_count, v_moved_hash
  from moved;
  if (v_moved_count,v_moved_definer_count,v_moved_invoker_count,v_moved_hash)
     is distinct from (12,11,1,'9fde6463206ad32158657d78c2b60f6b') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback moved-routine fingerprint drift: count=%s definer=%s invoker=%s hash=%s',
      v_moved_count,v_moved_definer_count,v_moved_invoker_count,v_moved_hash
    );
  end if;

  with adapters as (
    select n.nspname, p.oid::regprocedure::text as signature, p.prosecdef,
           pg_get_functiondef(p.oid) as definition
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
         md5(string_agg(nspname||':'||signature||':'||md5(definition),E'\n'
                        order by nspname,signature))
    into v_adapter_count,v_adapter_definer_count,v_adapter_hash
  from adapters;
  if (v_adapter_count,v_adapter_definer_count,v_adapter_hash)
     is distinct from (23,0,'bd814e9fbb1f502a70ff2853d0e392bb') then
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
     is distinct from (66,59,'5ea2663d4315fe53f6af184bb6c6cf26') then
    raise exception using errcode='55000', message=format(
      'Issue 356 rollback reference fingerprint drift: count=%s definer=%s hash=%s',
      v_ref_count,v_ref_definer_count,v_ref_hash
    );
  end if;

  select count(*),md5(string_agg(c.relname||':'||md5(pg_get_viewdef(c.oid,true))||':'||
    coalesce(c.relacl::text,'')||':'||coalesce(c.reloptions::text,''),E'\n' order by c.relname))
    into v_compat_count,v_compat_hash
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts');
  if (v_compat_count,v_compat_hash)
     is distinct from (4,'a9450eff0ba5ecfa7b42da563a62e459') then
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
\ir ../migrations/20260731163321_worker_control_plane_private_expand.sql
