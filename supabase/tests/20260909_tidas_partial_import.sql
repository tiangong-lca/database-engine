begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, private, api;
select no_plan();
select set_config('request.jwt.claim.role','service_role',true);
select set_config('request.jwt.claims','{"role":"service_role"}',true);
-- Keep triggers active while replacing only the external network boundary.
create temporary table import_webhooks(name text, body jsonb);
create or replace function util.invoke_edge_function(name text, body jsonb, timeout_milliseconds integer default 300000)
returns void language plpgsql security definer set search_path = '' as $$
begin insert into pg_temp.import_webhooks values(name,body); end $$;


insert into private.worker_job_kinds(job_kind,worker_queue,payload_schema_version)
values ('tidas.import_package','package','tidas.import_package.request.v1') on conflict do nothing;
insert into auth.users(id) values ('10770000-0000-4000-8000-000000000001');
insert into private.worker_jobs(id,job_kind,worker_queue,requested_by,status,payload_schema_version,payload_json,lease_token,lease_expires_at,subject_type,subject_id)
values ('10770000-0000-4000-8000-000000000002','tidas.import_package','package','10770000-0000-4000-8000-000000000001','running','tidas.import_package.request.v2',
 '{"type":"import_package","import_policy":"root_closure_v2","source_artifact_id":"10770000-0000-4000-8000-000000000003"}',
 '10770000-0000-4000-8000-000000000004',clock_timestamp()+interval '5 minutes','lca_package_job','10770000-0000-4000-8000-000000000005');
insert into private.lca_package_artifacts(id,job_id,worker_job_id,artifact_kind,status,artifact_url,artifact_sha256,artifact_format,content_type,metadata)
values ('10770000-0000-4000-8000-000000000003','10770000-0000-4000-8000-000000000005','10770000-0000-4000-8000-000000000002','import_source','ready','fixture://package',repeat('a',64),'tidas-package-zip:v1','application/zip','{"requested_by":"10770000-0000-4000-8000-000000000001"}');

create function pg_temp.apply_group(root_id text, entries jsonb) returns jsonb language sql as $$
 select private.tidas_import_group_apply_v2('10770000-0000-4000-8000-000000000002','10770000-0000-4000-8000-000000000004','10770000-0000-4000-8000-000000000003',repeat('b',64),jsonb_build_object('table','processes','id',root_id,'version','01.00.000'),entries)
$$;
create temporary table test_groups(name text, entries jsonb, receipt jsonb);
insert into test_groups values ('first','[{"table":"sources","id":"10770000-0000-4000-8000-000000000010","version":"01.00.000","json_ordered":{}},{"table":"processes","id":"10770000-0000-4000-8000-000000000011","version":"01.00.000","json_ordered":{}}]',null);
update test_groups set receipt=pg_temp.apply_group('10770000-0000-4000-8000-000000000011',entries);
select is((select receipt->>'inserted_count' from test_groups),'2','root and dependency commit together');
select is((select pg_temp.apply_group('10770000-0000-4000-8000-000000000011',entries) from test_groups),(select receipt from test_groups),'replay returns original commit receipt');
select is((select count(*) from private.tidas_import_groups_v2 where worker_job_id='10770000-0000-4000-8000-000000000002'),1::bigint,'replay does not duplicate receipt');

update public.sources set state_code=100,json_ordered='{"unchanged":true}' where id='10770000-0000-4000-8000-000000000010';
insert into test_groups values ('shared','[{"table":"sources","id":"10770000-0000-4000-8000-000000000010","version":"01.00.000","json_ordered":{"incoming":"different"}},{"table":"processes","id":"10770000-0000-4000-8000-000000000012","version":"01.00.000","json_ordered":{}}]',null);
update test_groups set receipt=pg_temp.apply_group('10770000-0000-4000-8000-000000000012',entries) where name='shared';
select is((select receipt->>'existing_count' from test_groups where name='shared'),'1','existing shared dependency skipped');
select is((select json_ordered::jsonb from public.sources where id='10770000-0000-4000-8000-000000000010'),'{"unchanged":true}'::jsonb,'different incoming content never overwrites existing state 100 data');
select is((select state_code from public.sources where id='10770000-0000-4000-8000-000000000010'),100,'skip preserves state code');

select throws_ok($q$select pg_temp.apply_group('10770000-0000-4000-8000-000000000013','[{"table":"sources","id":"10770000-0000-4000-8000-000000000014","version":"01.00.000","json_ordered":{}},{"table":"processes","id":"10770000-0000-4000-8000-000000000013","version":"01.00.000","json_ordered":null}]')$q$,'22023','TIDAS_IMPORT_ENTRY_INVALID','late invalid entry fails group');
select is((select count(*) from public.sources where id='10770000-0000-4000-8000-000000000014'),0::bigint,'late failure rolls back earlier dependency insert');
select is((select count(*) from public.processes where id in ('10770000-0000-4000-8000-000000000011','10770000-0000-4000-8000-000000000012')),2::bigint,'prior successful groups survive later failure');
select is((select count(*) from private.tidas_import_groups_v2 where worker_job_id='10770000-0000-4000-8000-000000000002'),2::bigint,'failed group leaves no receipt');
select is(api.svc_tidas_package_read_v2('10770000-0000-4000-8000-000000000001','10770000-0000-4000-8000-000000000002')#>>'{data,importProgress,imported_count}','3','readback reports distinct committed records without report artifact');
select is(api.svc_tidas_package_read_v2('10770000-0000-4000-8000-000000000099','10770000-0000-4000-8000-000000000002')->'data','null'::jsonb,'foreign user cannot read receipt summary');
select ok(not has_function_privilege('authenticated','private.tidas_import_group_apply_v2(uuid,uuid,uuid,text,jsonb,jsonb)','EXECUTE'),'browser cannot write groups');
select ok(not has_table_privilege('service_role','private.tidas_import_groups_v2','SELECT'),'receipts require reviewed capability');
update private.worker_jobs set lease_expires_at=clock_timestamp()-interval '1 second' where id='10770000-0000-4000-8000-000000000002';
select throws_ok($q$select pg_temp.apply_group('10770000-0000-4000-8000-000000000011',(select entries from test_groups where name='first'))$q$,'55000','TIDAS_IMPORT_LEASE_OR_SOURCE_INVALID','expired lease cannot replay or write');
insert into private.lca_package_artifacts(id,job_id,artifact_kind,status,artifact_url,artifact_format,content_type,metadata)
values ('10770000-0000-4000-8000-000000000020','10770000-0000-4000-8000-000000000021','import_source','pending','fixture://v2','tidas-package-zip:v1','application/zip','{"requested_by":"10770000-0000-4000-8000-000000000001"}');
select is(api.svc_tidas_package_import_enqueue_v2('10770000-0000-4000-8000-000000000001','10770000-0000-4000-8000-000000000021','10770000-0000-4000-8000-000000000020',repeat('c',64),100,'test.zip','application/zip')->>'ok','true','v2 admission reuses existing source/queue checks');
select is((select j.payload_schema_version from private.worker_jobs j join private.lca_package_artifacts a on a.worker_job_id=j.id where a.id='10770000-0000-4000-8000-000000000020'),'tidas.import_package.request.v2','new job pins v2 before it becomes claimable');
select is((select j.payload_json->>'import_policy' from private.worker_jobs j join private.lca_package_artifacts a on a.worker_job_id=j.id where a.id='10770000-0000-4000-8000-000000000020'),'root_closure_v2','new job pins partial policy');
select is(api.svc_tidas_package_import_enqueue_v2('10770000-0000-4000-8000-000000000001','10770000-0000-4000-8000-000000000021','10770000-0000-4000-8000-000000000020',repeat('c',64),100,'test.zip','application/zip')->>'mode','in_progress','v2 re-enqueue reuses active job');
select * from finish();
rollback;
