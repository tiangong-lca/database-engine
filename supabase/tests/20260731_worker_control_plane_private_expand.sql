begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select no_plan();

select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relname in ('worker_jobs','worker_job_events','worker_job_artifacts','worker_job_kinds') and c.relkind='v'), 4,
  'all four private Worker storage Expand relations are views');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname in ('worker_jobs','worker_job_events','worker_job_artifacts','worker_job_kinds') and c.relkind='r'), 4,
  'public physical relations remain the single source of truth during Expand');
select ok(to_regprocedure('public.worker_job_payload(public.worker_jobs,boolean)') is not null,
  'public worker_job_payload composite signature does not drift');
select ok(to_regprocedure('public.worker_job_payload(private.worker_jobs,boolean)') is null,
  'no accidental private composite overload replaces the public signature');
select is(
  (select indexdef from pg_indexes where schemaname='public'
    and indexname='worker_jobs_job_kind_concurrency_created_idx'),
  'CREATE INDEX worker_jobs_job_kind_concurrency_created_idx ON public.worker_jobs USING btree (job_kind, concurrency_key, created_at DESC, id DESC) WHERE (concurrency_key IS NOT NULL)',
  'bounded concurrency lookup has the exact partial composite index'
);

select is((select count(*)::integer from private.worker_job_kinds),
          (select count(*)::integer from public.worker_job_kinds), 'job-kind view parity');
select is((select count(*)::integer from private.worker_jobs),
          (select count(*)::integer from public.worker_jobs), 'job view parity');
select is((select count(*)::integer from private.worker_job_events),
          (select count(*)::integer from public.worker_job_events), 'event view parity');
select is((select count(*)::integer from private.worker_job_artifacts),
          (select count(*)::integer from public.worker_job_artifacts), 'artifact view parity');
select ok((select c.relkind='v' and coalesce(c.reloptions,'{}') @> array['security_invoker=true']
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname='worker_job_domain_refs'),
  'cross-domain worker_job_domain_refs remains a public security-invoker projection');

select ok(has_schema_privilege('service_role','private','USAGE'), 'service role can resolve private contract');
select ok(has_table_privilege('service_role','private.worker_jobs','SELECT'), 'service role can read private jobs');
select ok(has_column_privilege('service_role','private.worker_jobs','phase','UPDATE')
  and has_column_privilege('service_role','private.worker_jobs','progress','UPDATE')
  and has_column_privilege('service_role','private.worker_jobs','diagnostics','UPDATE')
  and has_column_privilege('service_role','private.worker_jobs','heartbeat_at','UPDATE')
  and has_column_privilege('service_role','private.worker_jobs','lease_expires_at','UPDATE')
  and has_column_privilege('service_role','private.worker_jobs','updated_at','UPDATE'), 'service role can update only runtime heartbeat columns');
select ok(not has_column_privilege('service_role','private.worker_jobs','status','UPDATE')
  and not has_column_privilege('service_role','private.worker_jobs','lease_token','UPDATE')
  and not has_table_privilege('service_role','private.worker_jobs','INSERT,DELETE'), 'service role cannot bypass job lifecycle fences');
select ok(has_table_privilege('service_role','private.worker_job_artifacts','SELECT'), 'service role can read private artifacts');
select ok(has_column_privilege('service_role','private.worker_job_artifacts','job_id','INSERT')
  and has_column_privilege('service_role','private.worker_job_artifacts','artifact_type','INSERT')
  and has_column_privilege('service_role','private.worker_job_artifacts','content_type','INSERT')
  and has_column_privilege('service_role','private.worker_job_artifacts','metadata','INSERT')
  and has_column_privilege('service_role','private.worker_job_artifacts','visibility','INSERT'), 'service role can insert the bounded artifact columns');
select ok(not has_column_privilege('service_role','private.worker_job_artifacts','storage_bucket','INSERT')
  and not has_column_privilege('service_role','private.worker_job_artifacts','storage_path','INSERT')
  and not has_table_privilege('service_role','private.worker_job_artifacts','UPDATE,DELETE'), 'service role cannot write artifact locators or mutate artifacts');
select ok(has_function_privilege('service_role','public.lcia_scope_closure_artifact_role(text)','EXECUTE'),
  'service role can execute the artifact check-constraint classifier');
select ok(not has_table_privilege('service_role','private.worker_job_events','SELECT,INSERT,UPDATE,DELETE'), 'event writes remain RPC lifecycle-owned');
select ok(not has_table_privilege('service_role','private.worker_job_kinds','SELECT,INSERT,UPDATE,DELETE'), 'job kinds remain migration-owned');
select ok(has_table_privilege('service_role','public.worker_job_domain_refs','SELECT'), 'service role retains public domain-ref projection access');
select ok(not has_table_privilege('authenticated','public.worker_job_domain_refs','SELECT'), 'authenticated remains unable to read domain refs');

select ok(not has_table_privilege('anon','private.worker_jobs','SELECT'), 'anon cannot read private jobs');
select ok(not has_table_privilege('authenticated','private.worker_jobs','SELECT'), 'authenticated cannot read private jobs');
select ok(not has_function_privilege('authenticated','public.worker_read_job(uuid,boolean)','EXECUTE'), 'worker_read_job remains service-only');
select ok(not has_function_privilege('authenticated','public.worker_list_jobs(uuid,text,uuid,text[],text,integer,boolean)','EXECUTE'), 'worker_list_jobs remains service-only');
select ok(not has_function_privilege('authenticated','public.worker_cancel_job(uuid,uuid,text)','EXECUTE'), 'worker_cancel_job remains service-only');
select ok(to_regprocedure('public.worker_read_latest_job(uuid,text,uuid,text,text,text[],boolean)') is not null,
  'seven-argument latest-job signature remains');
select ok(to_regprocedure('public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean)') is not null,
  'bounded concurrency snapshot signature exists');
select ok(not has_function_privilege('authenticated','public.worker_list_jobs_by_concurrency_key(text,text,text[],integer,boolean)','EXECUTE'),
  'bounded concurrency snapshot remains service-only');
select ok(has_function_privilege('service_role','public.worker_read_jobs_by_ids(uuid[],boolean)','EXECUTE'), 'service role can batch-read jobs');
select ok(not has_function_privilege('authenticated','public.worker_read_jobs_by_ids(uuid[],boolean)','EXECUTE'), 'batch read remains service-only');

select is((select count(*)::integer from pg_default_acl d join pg_namespace n on n.oid=d.defaclnamespace,
  lateral aclexplode(coalesce(d.defaclacl, acldefault(d.defaclobjtype,d.defaclrole))) a
  where n.nspname='public' and d.defaclrole='postgres'::regrole
    and (a.grantee=0 or a.grantee in ('anon'::regrole,'authenticated'::regrole,'service_role'::regrole))), 0,
  'repo migration owner future public defaults require explicit API and service grants');

select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where p.prokind='f' and n.nspname in ('public','private') and p.prosecdef
    and pg_get_functiondef(p.oid) ilike '%worker_jobs%' and p.proconfig is null), 0,
  'Worker-related SECURITY DEFINER functions all have fixed configuration');

create temporary table pg_temp.expand_job (id uuid primary key) on commit drop;
with inserted as (
  insert into private.worker_jobs (job_kind,worker_runtime,worker_queue,requester_type,status,payload_schema_version,payload_json)
  values ('lca.snapshot_gc','calculator','maintenance','service','queued','lca.snapshot_gc.request.v1','{"expandTest":true}'::jsonb)
  returning id
)
insert into pg_temp.expand_job select id from inserted;
select is((select count(*)::integer from public.worker_jobs where payload_json @> '{"expandTest":true}'), 1,
  'private insert writes the one public source of truth');
update private.worker_jobs set phase='private-expand' where id=(select id from pg_temp.expand_job);
select is((select phase from public.worker_jobs where id=(select id from pg_temp.expand_job)), 'private-expand',
  'private update is visible through public compatibility');
delete from private.worker_jobs where id=(select id from pg_temp.expand_job);
select is((select count(*)::integer from public.worker_jobs where payload_json @> '{"expandTest":true}'), 0,
  'private delete removes the one source row');

insert into public.worker_jobs (
  id,job_kind,worker_runtime,worker_queue,requester_type,status,concurrency_key,
  payload_schema_version,payload_json,created_at,updated_at
) values
  ('00000000-0000-0000-0000-000000000101','lca.snapshot_gc','calculator','maintenance','service','completed',
   'expand:concurrency','lca.snapshot_gc.request.v1','{"sequence":1}',clock_timestamp()-interval '2 minutes',clock_timestamp()),
  ('00000000-0000-0000-0000-000000000102','lca.snapshot_gc','calculator','maintenance','service','completed',
   'expand:concurrency','lca.snapshot_gc.request.v1','{"sequence":2}',clock_timestamp()-interval '1 minute',clock_timestamp());
insert into public.worker_jobs (
  job_kind,worker_runtime,worker_queue,requester_type,status,concurrency_key,
  payload_schema_version,payload_json,created_at,updated_at
)
select 'lca.snapshot_gc','calculator','maintenance','service','queued',
  'expand:noise:'||g,'lca.snapshot_gc.request.v1','{}',
  clock_timestamp()-g*interval '1 second',clock_timestamp()
from generate_series(1,2000) g;
analyze public.worker_jobs;
create function pg_temp.worker_concurrency_plan_uses_contract_index() returns boolean
language plpgsql as $$
declare v_plan json;
begin
  execute $plan$
    explain (format json, costs off)
    select * from private.worker_jobs
    where job_kind='lca.snapshot_gc'
      and concurrency_key='nonexistent'
      and status=any(array['queued','running'])
    order by created_at desc,id desc
    limit 20
  $plan$ into v_plan;
  return v_plan::text like '%worker_jobs_job_kind_concurrency_created_idx%';
end;
$$;
select ok(pg_temp.worker_concurrency_plan_uses_contract_index(),
  'representative planner uses the bounded concurrency composite index');
set local role service_role;
select set_config('request.jwt.claim.role','service_role',true);
insert into private.worker_job_artifacts(job_id,artifact_type,content_type,metadata,visibility)
values ('00000000-0000-0000-0000-000000000101','ordinary_worker_log','text/plain','{}','operator');
select is((select count(*)::integer from private.worker_job_artifacts
  where job_id='00000000-0000-0000-0000-000000000101'),1,
  'service role can insert a production-shaped artifact through the private contract');
select is(
  public.worker_list_jobs_by_concurrency_key(
    'lca.snapshot_gc','expand:concurrency',array['completed'],20,true
  ) #>> '{data,0,id}',
  '00000000-0000-0000-0000-000000000102',
  'bounded concurrency snapshot preserves created-at descending order'
);
select is(
  public.worker_list_jobs_by_concurrency_key(
    'lca.snapshot_gc','expand:concurrency',array['completed'],1,true
  ) #>> '{data,0,payload,sequence}',
  '2',
  'bounded concurrency snapshot includes the internal camel-case DTO payload'
);
select is(
  public.worker_list_jobs_by_concurrency_key(
    'lca.snapshot_gc','expand:concurrency',array['completed'],0,true
  ) ->> 'code',
  'INVALID_WORKER_JOB_LIMIT',
  'bounded concurrency snapshot rejects a zero limit'
);
select is(
  public.worker_list_jobs_by_concurrency_key(
    'lca.snapshot_gc','expand:concurrency',array['completed'],21,true
  ) ->> 'code',
  'INVALID_WORKER_JOB_LIMIT',
  'bounded concurrency snapshot rejects a limit above twenty'
);
reset role;

select ok((select residue->>'contractReady' from private.worker_control_plane_contract_residue) = 'false',
  'Contract exit gate remains fail closed while public compatibility exists');
select ok((select residue->>'platformOwnerDefaultPrivilegesReady' from private.worker_control_plane_contract_residue) = 'false',
  'Contract gate exposes unresolved platform-owner default privileges');
select ok((select residue->'platformOwnerDefaultPrivilegeOwners' from private.worker_control_plane_contract_residue) ? 'supabase_admin',
  'Contract residue names the platform owner requiring operator action');

select * from finish();
rollback;
