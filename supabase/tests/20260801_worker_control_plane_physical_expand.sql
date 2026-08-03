begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, pg_catalog, public;
select no_plan();

select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relkind='r' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')), 4,
  'four Worker relations are private physical tables');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v' and coalesce(c.reloptions,'{}') @> array['security_invoker=true']
    and c.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')), 4,
  'four public Worker compatibility relations are security-invoker views');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind in ('r','p') and c.relname like 'worker_job%'), 0,
  'public contains no Worker control-plane physical table');

select is((select count(*)::integer from (
  select c.relname from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relkind='r' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
  except
  select c.relname from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
) d), 0, 'public/private relation names have exact parity');
select is((select count(*)::integer from (
  select c.relname,a.attname,a.attnum,format_type(a.atttypid,a.atttypmod) type_name
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
  where n.nspname='private' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
  except
  select c.relname,a.attname,a.attnum,format_type(a.atttypid,a.atttypmod)
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
  where n.nspname='public' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
) d), 0, 'public compatibility columns preserve private order and type');

select is((select count(*)::integer from pg_constraint con
  join pg_class c on c.oid=con.conrelid join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')), 42,
  'all Worker constraints remain attached to private physical OIDs');
select is((select count(*)::integer from pg_index i join pg_class c on c.oid=i.indrelid
  join pg_namespace n on n.oid=c.relnamespace where n.nspname='private' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')), 19,
  'all Worker indexes remain attached to private physical OIDs');
select is((select count(*)::integer from pg_trigger t join pg_class c on c.oid=t.tgrelid
  join pg_namespace n on n.oid=c.relnamespace where not t.tgisinternal and n.nspname='private'
    and c.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')), 2,
  'all user Worker triggers remain attached to private physical OIDs');
select ok((select count(*)=1 and bool_and(
    c.relname='worker_jobs'
    and p.polname='worker_jobs_result_gc_executor_select'
    and p.polcmd='r'
    and p.polroles=array['lca_result_gc_executor'::regrole]::oid[]
    and pg_get_expr(p.polqual,p.polrelid)='true'
  ) from pg_policy p join pg_class c on c.oid=p.polrelid
  join pg_namespace n on n.oid=c.relnamespace where n.nspname='private' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')),
  'Worker physical tables expose only the exact result-GC executor SELECT policy');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relname in
    ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
    and c.relrowsecurity and not c.relforcerowsecurity), 4,
  'Worker physical tables preserve enabled non-forced RLS flags');

select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private' and p.proname in
    ('worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
     'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
     'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
     'worker_record_job_result','worker_retry_job')), 12,
  'twelve exact Worker implementations are private');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private' and p.proname in
    ('worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
     'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
     'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
     'worker_record_job_result','worker_retry_job') and p.prosecdef), 11,
  'eleven of twelve private Worker implementations remain SECURITY DEFINER');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public' and p.proname in
    ('worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
     'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
     'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
     'worker_record_job_result','worker_retry_job') and p.prosecdef), 0,
  'public Worker compatibility wrappers never hide privileged implementations');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='api' and p.proname like 'worker_%_v1' and not p.prosecdef), 11,
  'eleven API v1 adapters are security-invoker functions');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private' and p.proname in
    ('worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
     'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
     'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
     'worker_record_job_result','worker_retry_job')
    and not coalesce(p.proconfig,'{}') @> array['search_path=pg_catalog, private, util, public, pg_temp']
), 0, 'all twelve private Worker routines have the reviewed fixed search_path');

select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where p.prokind='f' and p.prosecdef and n.nspname in ('public','api','private','util','archive')
    and pg_get_functiondef(p.oid) ~ 'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'), 0,
  'no SECURITY DEFINER body retains a public Worker dependency');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where p.prokind='f' and not p.prosecdef and n.nspname in ('public','api','private','util','archive')
    and pg_get_functiondef(p.oid) ~ 'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'), 4,
  'four reviewed security-invoker compatibility-body residues remain');

select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where p.prosecdef and n.nspname='public'), 233,
  'logical SECURITY DEFINER distribution has 233 physical public routines after grouped root-review facades');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where p.prosecdef and n.nspname='private'), 62,
  'logical SECURITY DEFINER distribution has 62 physical private routines including result GC');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where p.prosecdef and n.nspname='util'), 36,
  'global SECURITY DEFINER coverage includes 36 util routines');

select ok(not has_schema_privilege('anon','private','USAGE')
  and not has_schema_privilege('authenticated','private','USAGE'),
  'browser roles cannot resolve private');
select ok(not has_table_privilege('anon','public.worker_jobs','SELECT')
  and not has_table_privilege('authenticated','public.worker_jobs','SELECT'),
  'browser roles cannot read the public compatibility view');
select ok(not has_table_privilege('service_role','public.worker_jobs','MAINTAIN'),
  'public compatibility views do not accidentally grant PostgreSQL 17 MAINTAIN');
select ok(has_table_privilege('service_role','public.worker_jobs','SELECT')
  and has_column_privilege('service_role','public.worker_jobs','phase','UPDATE')
  and has_column_privilege('service_role','public.worker_jobs','progress','UPDATE')
  and not has_column_privilege('service_role','public.worker_jobs','status','UPDATE')
  and not has_table_privilege('service_role','public.worker_jobs','INSERT')
  and not has_table_privilege('service_role','public.worker_jobs','DELETE'),
  'service_role public compatibility DML is column bounded');
select ok(not has_table_privilege('api_internal_executor','public.worker_jobs','SELECT')
  and not has_table_privilege('api_internal_executor','private.worker_jobs','SELECT'),
  'api_internal_executor uses bounded Worker routines rather than relation ACL');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private' and p.proname in
    ('worker_cancel_job','worker_claim_jobs','worker_enqueue_job','worker_heartbeat_job',
     'worker_job_payload','worker_list_jobs','worker_list_jobs_by_concurrency_key',
     'worker_read_job','worker_read_jobs_by_ids','worker_read_latest_job',
     'worker_record_job_result','worker_retry_job')
    and has_function_privilege('api_internal_executor',p.oid,'EXECUTE')), 12,
  'api_internal_executor retains the exact 12-of-12 baseline routine EXECUTE set');

select ok(to_regprocedure('private.worker_job_payload(private.worker_jobs,boolean)') is not null
  and to_regprocedure('public.worker_job_payload(public.worker_jobs,boolean)') is not null,
  'worker_jobs composite implementation and public bridge are both exact');
select ok(to_regprocedure('public.lcia_scope_closure_artifact_lineage_eligible(public.lcia_scope_closure_checks,private.worker_job_artifacts,text)') is not null
  and to_regprocedure('public.lcia_scope_closure_artifact_lineage_eligible(public.lcia_scope_closure_checks,public.worker_job_artifacts,text)') is not null,
  'artifact composite implementation and public bridge are both exact');
select is((select count(*)::integer from pg_depend d join pg_proc p on p.oid=d.objid
  join pg_namespace n on n.oid=p.pronamespace join pg_type t on t.oid=d.refobjid
  join pg_namespace tn on tn.oid=t.typnamespace
  where d.classid='pg_proc'::regclass and d.refclassid='pg_type'::regclass
    and tn.nspname='private' and t.typname in ('worker_jobs','worker_job_artifacts')), 2,
  'two reviewed routines hold private Worker composite type dependencies');

select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname in
    ('worker_domain_traceability_cutoffs','worker_domain_traceability_violations',
     'worker_job_domain_refs','worker_legacy_lifecycle_audit',
     'worker_legacy_table_retirement_blockers') and c.relkind='v'), 5,
  '#354 five-view compatibility contract remains present');
select is((select md5(string_agg(c.relname || ':' || md5(pg_get_viewdef(c.oid,true)) || ':' ||
  coalesce(c.relacl::text,''), E'\n' order by c.relname)) from pg_class c
  join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname in
    ('worker_domain_traceability_cutoffs','worker_domain_traceability_violations',
     'worker_job_domain_refs','worker_legacy_lifecycle_audit',
     'worker_legacy_table_retirement_blockers')),
  '194b385d637524ee30d124ae9280b1ea', '#354 five-view definition and ACL hash is exact');

select is((select residue->>'contractVersion' from private.worker_control_plane_contract_residue),
  'worker-control-plane.private-physical-expand.v1', 'residue reports the physical Expand version');
select is((select residue->>'contractReady' from private.worker_control_plane_contract_residue),
  'false', 'Contract remains fail closed while compatibility surfaces exist');
select is((select residue->>'serviceRoleRelationAclContract'
  from private.worker_control_plane_contract_residue),
  'worker-control-plane.private-minimum.v1',
  'Expand reports the exact minimum service relation ACL contract');
select is((select jsonb_array_length(residue->'serviceRoleRelationAclEvidence')
  from private.worker_control_plane_contract_residue), 6,
  'minimum ACL contract names all five consumer issues and Contract gate');

select * from finish();
rollback;
