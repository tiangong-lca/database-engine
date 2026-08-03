begin;
create extension if not exists pgtap with schema extensions;
select extensions.plan(16);

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prokind='f' and p.prosecdef),
  233::bigint, 'public SECURITY DEFINER inventory includes the grouped root-review facades and excludes moved Worker canonicals');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and pg_get_userbyid(p.proowner)='postgres'),
  217::bigint, '217 public SECURITY DEFINER signatures retain postgres ownership');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and pg_get_userbyid(p.proowner)='api_internal_executor'),
  16::bigint, '#339 retains sixteen RLS-bound facade owners');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef
     and not exists (select 1 from unnest(coalesce(p.proconfig,'{}')) c where c like 'search_path=%')),
  0::bigint, 'every SECURITY DEFINER signature fixes search_path');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('anon',p.oid,'EXECUTE')),
  93::bigint, 'current anon effective EXECUTE count reflects #354 convergence');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('authenticated',p.oid,'EXECUTE')),
  143::bigint, 'current authenticated effective EXECUTE count includes the grouped root-review facades');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('public',p.oid,'EXECUTE')),
  17::bigint, 'current PUBLIC effective EXECUTE count is explicit');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('service_role',p.oid,'EXECUTE')),
  163::bigint, 'public service_role effective EXECUTE count includes grouped root-review facades and excludes private Worker canonicals');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname in ('public','api','private','util','archive') and p.prosecdef),
  333::bigint, 'the governed SECURITY DEFINER inventory includes grouped root-review facades, result GC, and document-validation evidence contracts');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='private' and p.prosecdef and pg_get_userbyid(p.proowner)='postgres'
     and p.proname in (
       'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
       'worker_list_jobs','worker_list_jobs_by_concurrency_key','worker_read_job',
       'worker_read_jobs_by_ids','worker_read_latest_job','worker_record_job_result','worker_retry_job'
     )),
  11::bigint, 'all eleven moved Worker canonicals remain postgres-owned SECURITY DEFINER routines');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='private' and p.prosecdef
     and p.proname in (
       'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
       'worker_list_jobs','worker_list_jobs_by_concurrency_key','worker_read_job',
       'worker_read_jobs_by_ids','worker_read_latest_job','worker_record_job_result','worker_retry_job'
     )
     and p.proconfig @> array['search_path=pg_catalog, private, util, public, pg_temp']),
  11::bigint, 'all moved Worker canonicals use the reviewed trusted search_path with pg_temp last');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='private' and p.prosecdef
     and p.proname in (
       'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
       'worker_list_jobs','worker_list_jobs_by_concurrency_key','worker_read_job',
       'worker_read_jobs_by_ids','worker_read_latest_job','worker_record_job_result','worker_retry_job'
     )
     and (has_function_privilege('anon',p.oid,'EXECUTE')
       or has_function_privilege('authenticated',p.oid,'EXECUTE')
       or has_function_privilege('public',p.oid,'EXECUTE'))),
  0::bigint, 'browser roles and PUBLIC cannot execute moved private Worker canonicals');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and not p.prosecdef
     and p.proname in (
       'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
       'worker_list_jobs','worker_list_jobs_by_concurrency_key','worker_read_job',
       'worker_read_jobs_by_ids','worker_read_latest_job','worker_record_job_result','worker_retry_job'
     )
     and p.proconfig @> array['search_path=pg_catalog, pg_temp']
     and has_function_privilege('service_role',p.oid,'EXECUTE')
     and has_function_privilege('api_internal_executor',p.oid,'EXECUTE')
     and not has_function_privilege('anon',p.oid,'EXECUTE')
     and not has_function_privilege('authenticated',p.oid,'EXECUTE')
     and not has_function_privilege('public',p.oid,'EXECUTE')),
  11::bigint, 'all eleven public Worker wrappers are safe invokers with the exact service-only execution edge');

select extensions.is(
  (select count(*)::bigint
   from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where (
     n.nspname='public' and p.prosecdef and has_function_privilege('service_role',p.oid,'EXECUTE')
   ) or (
     n.nspname='private' and p.prosecdef and has_function_privilege('service_role',p.oid,'EXECUTE')
     and p.proname in (
       'worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
       'worker_list_jobs','worker_list_jobs_by_concurrency_key','worker_read_job',
       'worker_read_jobs_by_ids','worker_read_latest_job','worker_record_job_result','worker_retry_job'
     )
   )),
  174::bigint, 'service_role effective EXECUTE includes grouped root-review facades and is conserved across Worker moves');

select extensions.is(
  (select jsonb_array_length(posture->'forbiddenInternalExecute') from util.security_acl_expand_posture),
  0, '#339 internal-schema API-role EXECUTE drift remains closed');

select extensions.is(
  (select jsonb_array_length(posture->'repoOwnerDefaultPrivilegeResidue') from util.security_acl_expand_posture),
  0, '#339 repo-owned default privilege residue remains closed');

select * from extensions.finish();
rollback;
