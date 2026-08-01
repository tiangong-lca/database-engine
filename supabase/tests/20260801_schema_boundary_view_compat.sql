begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select no_plan();

select has_schema('api', 'api schema exists');
select has_schema('private', 'private schema exists');
select has_schema('util', 'util schema exists');
select has_schema('archive', 'archive schema exists');

with expected(schema_name, usage_roles) as (values
  ('api', array['anon','authenticated','service_role']::text[]),
  ('private', array['service_role']::text[]),
  ('util', array['service_role']::text[]),
  ('archive', array['service_role']::text[])
)
select ok(
  (select owner.rolname='postgres'
   from pg_namespace n join pg_roles owner on owner.oid=n.nspowner
   where n.nspname=expected.schema_name),
  format('%s schema is postgres-owned', schema_name)
) from expected;

with expected(schema_name, role_name, can_use) as (values
  ('api','anon',true), ('api','authenticated',true), ('api','service_role',true),
  ('private','anon',false), ('private','authenticated',false), ('private','service_role',true),
  ('util','anon',false), ('util','authenticated',false), ('util','service_role',true),
  ('archive','anon',false), ('archive','authenticated',false), ('archive','service_role',true)
)
select ok(
  has_schema_privilege(role_name,schema_name,'USAGE')=can_use
  and not has_schema_privilege(role_name,schema_name,'CREATE'),
  format('%s has exact USAGE/no-CREATE contract on %s',role_name,schema_name)
) from expected;

with expected(name,target_schema,column_names) as (values
  ('worker_domain_traceability_cutoffs','private',array['domain_source','required_worker_column','cutover_at','traceability_required','contract_note']::text[]),
  ('worker_domain_traceability_violations','util',array['domain_source','domain_id','domain_role','created_at','updated_at','cutover_at','violation_code','details']::text[]),
  ('worker_job_domain_refs','api',array['worker_job_id','domain_source','domain_id','domain_role','legacy_job_id','status','created_at','updated_at']::text[]),
  ('worker_legacy_lifecycle_audit','util',array['legacy_source','task_family','legacy_status','row_count','active_count','oldest_created_at','newest_created_at','latest_updated_at']::text[]),
  ('worker_legacy_table_retirement_blockers','util',array['legacy_table','blocker_type','blocker_schema','blocker_name','blocker_identity','is_drop_restrict_blocker','details']::text[])
)
select ok(
  to_regclass(format('%I.%I',target_schema,name)) is not null
  and to_regclass(format('public.%I',name)) is not null,
  format('%s has canonical target and public compatibility views',name)
) from expected;

with expected(name,target_schema,column_names) as (values
  ('worker_domain_traceability_cutoffs','private',array['domain_source','required_worker_column','cutover_at','traceability_required','contract_note']::text[]),
  ('worker_domain_traceability_violations','util',array['domain_source','domain_id','domain_role','created_at','updated_at','cutover_at','violation_code','details']::text[]),
  ('worker_job_domain_refs','api',array['worker_job_id','domain_source','domain_id','domain_role','legacy_job_id','status','created_at','updated_at']::text[]),
  ('worker_legacy_lifecycle_audit','util',array['legacy_source','task_family','legacy_status','row_count','active_count','oldest_created_at','newest_created_at','latest_updated_at']::text[]),
  ('worker_legacy_table_retirement_blockers','util',array['legacy_table','blocker_type','blocker_schema','blocker_name','blocker_identity','is_drop_restrict_blocker','details']::text[])
)
select is(
  (select array_agg(a.attname::text order by a.attnum)
   from pg_attribute a
   where a.attrelid=format('%I.%I',target_schema,name)::regclass and a.attnum>0 and not a.attisdropped),
  column_names,
  format('%s canonical columns are frozen',name)
) from expected;

with expected(name,target_schema,column_names) as (values
  ('worker_domain_traceability_cutoffs','private',array['domain_source','required_worker_column','cutover_at','traceability_required','contract_note']::text[]),
  ('worker_domain_traceability_violations','util',array['domain_source','domain_id','domain_role','created_at','updated_at','cutover_at','violation_code','details']::text[]),
  ('worker_job_domain_refs','api',array['worker_job_id','domain_source','domain_id','domain_role','legacy_job_id','status','created_at','updated_at']::text[]),
  ('worker_legacy_lifecycle_audit','util',array['legacy_source','task_family','legacy_status','row_count','active_count','oldest_created_at','newest_created_at','latest_updated_at']::text[]),
  ('worker_legacy_table_retirement_blockers','util',array['legacy_table','blocker_type','blocker_schema','blocker_name','blocker_identity','is_drop_restrict_blocker','details']::text[])
)
select is(
  (select array_agg(a.attname::text order by a.attnum)
   from pg_attribute a
   where a.attrelid=format('public.%I',name)::regclass and a.attnum>0 and not a.attisdropped),
  column_names,
  format('%s compatibility columns are explicit and frozen',name)
) from expected;

with expected(name,target_schema) as (values
  ('worker_domain_traceability_cutoffs','private'),
  ('worker_domain_traceability_violations','util'),
  ('worker_job_domain_refs','api'),
  ('worker_legacy_lifecycle_audit','util'),
  ('worker_legacy_table_retirement_blockers','util')
), objects as (
  select name,target_schema,format('%I.%I',target_schema,name)::regclass oid,'canonical' label from expected
  union all select name,target_schema,format('public.%I',name)::regclass,'compatibility' from expected
)
select ok(
  (select c.relkind='v' and c.reloptions=array['security_invoker=true'] and owner.rolname='postgres'
   from pg_class c join pg_roles owner on owner.oid=c.relowner where c.oid=objects.oid),
  format('%s %s view is postgres-owned security_invoker',name,label)
) from objects;

with expected(name,target_schema) as (values
  ('worker_domain_traceability_cutoffs','private'),
  ('worker_domain_traceability_violations','util'),
  ('worker_job_domain_refs','api'),
  ('worker_legacy_lifecycle_audit','util'),
  ('worker_legacy_table_retirement_blockers','util')
), objects as (
  select name,format('%I.%I',target_schema,name)::regclass oid,'canonical' label from expected
  union all select name,format('public.%I',name)::regclass,'compatibility' from expected
)
select ok(
  has_table_privilege('service_role',oid,'SELECT')
  and not has_table_privilege('anon',oid,'SELECT')
  and not has_table_privilege('authenticated',oid,'SELECT')
  and not has_table_privilege('api_internal_executor',oid,'SELECT'),
  format('%s %s view has exact service-only read contract',name,label)
) from objects;

with expected(name,target_schema) as (values
  ('worker_domain_traceability_cutoffs','private'),
  ('worker_domain_traceability_violations','util'),
  ('worker_job_domain_refs','api'),
  ('worker_legacy_lifecycle_audit','util'),
  ('worker_legacy_table_retirement_blockers','util')
)
select ok(
  obj_description(format('public.%I',name)::regclass,'pg_class') =
    format('Compatibility view for %s.%s; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.',target_schema,name),
  format('%s compatibility comment has exact removal metadata',name)
) from expected;

with expected(name,target_schema) as (values
  ('worker_domain_traceability_cutoffs','private'),
  ('worker_domain_traceability_violations','util'),
  ('worker_job_domain_refs','api'),
  ('worker_legacy_lifecycle_audit','util'),
  ('worker_legacy_table_retirement_blockers','util')
)
select ok(exists(
  select 1 from pg_rewrite r join pg_depend d
    on d.classid='pg_rewrite'::regclass and d.objid=r.oid
  where r.ev_class=format('public.%I',name)::regclass
    and d.refobjid=format('%I.%I',target_schema,name)::regclass
),format('%s compatibility view depends on its canonical target',name)) from expected;

select ok(exists(
  select 1 from pg_rewrite r join pg_depend d
    on d.classid='pg_rewrite'::regclass and d.objid=r.oid
  where r.ev_class='util.worker_domain_traceability_violations'::regclass
    and d.refobjid='private.worker_domain_traceability_cutoffs'::regclass
),'violation view follows the OID-preserved private cutoff dependency');

select is((select count(*)::integer from (
  (select domain_source,required_worker_column,cutover_at,traceability_required,contract_note from public.worker_domain_traceability_cutoffs
   except all select domain_source,required_worker_column,cutover_at,traceability_required,contract_note from private.worker_domain_traceability_cutoffs)
  union all
  (select domain_source,required_worker_column,cutover_at,traceability_required,contract_note from private.worker_domain_traceability_cutoffs
   except all select domain_source,required_worker_column,cutover_at,traceability_required,contract_note from public.worker_domain_traceability_cutoffs)
) parity),0,'cutoff compatibility parity is exact');

select is((select count(*)::integer from (
  (select domain_source,domain_id,domain_role,created_at,updated_at,cutover_at,violation_code,details from public.worker_domain_traceability_violations
   except all select domain_source,domain_id,domain_role,created_at,updated_at,cutover_at,violation_code,details from util.worker_domain_traceability_violations)
  union all
  (select domain_source,domain_id,domain_role,created_at,updated_at,cutover_at,violation_code,details from util.worker_domain_traceability_violations
   except all select domain_source,domain_id,domain_role,created_at,updated_at,cutover_at,violation_code,details from public.worker_domain_traceability_violations)
) parity),0,'violation compatibility parity is exact');

select is((select count(*)::integer from (
  (select worker_job_id,domain_source,domain_id,domain_role,legacy_job_id,status,created_at,updated_at from public.worker_job_domain_refs
   except all select worker_job_id,domain_source,domain_id,domain_role,legacy_job_id,status,created_at,updated_at from api.worker_job_domain_refs)
  union all
  (select worker_job_id,domain_source,domain_id,domain_role,legacy_job_id,status,created_at,updated_at from api.worker_job_domain_refs
   except all select worker_job_id,domain_source,domain_id,domain_role,legacy_job_id,status,created_at,updated_at from public.worker_job_domain_refs)
) parity),0,'domain-ref compatibility parity is exact');

select is((select count(*)::integer from (
  (select legacy_source,task_family,legacy_status,row_count,active_count,oldest_created_at,newest_created_at,latest_updated_at from public.worker_legacy_lifecycle_audit
   except all select legacy_source,task_family,legacy_status,row_count,active_count,oldest_created_at,newest_created_at,latest_updated_at from util.worker_legacy_lifecycle_audit)
  union all
  (select legacy_source,task_family,legacy_status,row_count,active_count,oldest_created_at,newest_created_at,latest_updated_at from util.worker_legacy_lifecycle_audit
   except all select legacy_source,task_family,legacy_status,row_count,active_count,oldest_created_at,newest_created_at,latest_updated_at from public.worker_legacy_lifecycle_audit)
) parity),0,'lifecycle compatibility parity is exact');

select is((select count(*)::integer from (
  (select legacy_table,blocker_type,blocker_schema,blocker_name,blocker_identity,is_drop_restrict_blocker,details from public.worker_legacy_table_retirement_blockers
   except all select legacy_table,blocker_type,blocker_schema,blocker_name,blocker_identity,is_drop_restrict_blocker,details from util.worker_legacy_table_retirement_blockers)
  union all
  (select legacy_table,blocker_type,blocker_schema,blocker_name,blocker_identity,is_drop_restrict_blocker,details from util.worker_legacy_table_retirement_blockers
   except all select legacy_table,blocker_type,blocker_schema,blocker_name,blocker_identity,is_drop_restrict_blocker,details from public.worker_legacy_table_retirement_blockers)
) parity),0,'retirement-blocker compatibility parity is exact');

select throws_ok('drop view private.worker_domain_traceability_cutoffs restrict','2BP01',null,
  'canonical cutoff cannot be dropped while dependents remain');
select throws_ok('drop view util.worker_domain_traceability_violations restrict','2BP01',null,
  'canonical violation view cannot be dropped while compatibility remains');
select throws_ok('drop view api.worker_job_domain_refs restrict','2BP01',null,
  'canonical domain-ref view cannot be dropped while compatibility remains');
select throws_ok('drop view util.worker_legacy_lifecycle_audit restrict','2BP01',null,
  'canonical lifecycle view cannot be dropped while compatibility remains');
select throws_ok('drop view util.worker_legacy_table_retirement_blockers restrict','2BP01',null,
  'canonical blocker view cannot be dropped while compatibility remains');

select ok((select (posture->>'expandReady')::boolean and not (posture->>'contractReady')::boolean
  from util.schema_boundary_phase),'phase readback is Expand-ready and Contract-false');

select * from finish();
rollback;
