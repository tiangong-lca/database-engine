begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

-- A minimal immutable current release.  The closure request must use this
-- manifest, rather than any live state-code rows, as its identity universe.
insert into auth.users (instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous)
values ('00000000-0000-0000-0000-000000000000','c7220000-0000-4000-8000-000000000001','authenticated','authenticated','closure-e2e@example.com','x',now(),'{}','{}',now(),now(),false,false);
insert into public.users(id,raw_user_meta_data,contact) values ('c7220000-0000-4000-8000-000000000001','{}',null);
insert into public.teams(id,json,rank,is_public) values ('00000000-0000-0000-0000-000000000000','{"name":"System"}',0,false) on conflict(id) do nothing;
insert into public.roles(user_id,team_id,role) values ('c7220000-0000-4000-8000-000000000001','00000000-0000-0000-0000-000000000000','data_product_manager');

insert into public.lca_release_runs(id,release_version,selection_manifest_hash,input_manifest_hash,calculation_bundle_hash,calculation_bundle_ref,profile_lock_hash,publish_plan_hash,publish_plan,artifact_set_hash,release_manifest_hash,release_manifest,status,idempotency_key,request_hash,created_by)
values ('c7220000-0000-4000-8000-000000000010','77.00.001',repeat('a',64),repeat('b',64),repeat('c',64),'{}',repeat('d',64),repeat('e',64),'{}',repeat('f',64),repeat('9',64),'{}','published','closure-e2e-release',repeat('1',64),'c7220000-0000-4000-8000-000000000001');
insert into public.lca_release_approvals(id,release_run_id,publish_plan_hash,approval_hash,approved_by,approved_at,expires_at)
values ('c7220000-0000-4000-8000-000000000011','c7220000-0000-4000-8000-000000000010',repeat('e',64),repeat('2',64),'c7220000-0000-4000-8000-000000000001',now(),now()+interval '1 day');
insert into public.lca_release_publications(id,release_run_id,release_version,approval_id,approval_hash,publish_plan_hash,release_manifest_hash,artifact_set_hash,approved_by,executed_by,credential_fingerprint,idempotency_key,published_at)
values ('c7220000-0000-4000-8000-000000000012','c7220000-0000-4000-8000-000000000010','77.00.001','c7220000-0000-4000-8000-000000000011',repeat('2',64),repeat('e',64),repeat('9',64),repeat('f',64),'c7220000-0000-4000-8000-000000000001','c7220000-0000-4000-8000-000000000001',repeat('3',64),'closure-e2e-publication',now());
insert into public.lca_release_dataset_versions(release_run_id,dataset_type,dataset_role,dataset_uuid,dataset_version,source_process_uuid,source_process_version,version_significant_hash,semantic_hash,canonical_content_hash,artifact_ref)
values
('c7220000-0000-4000-8000-000000000010','process','unit_process','c7220000-0000-4000-8000-000000000020','01.00.000','c7220000-0000-4000-8000-000000000020','01.00.000',repeat('4',64),repeat('5',64),repeat('6',64),'{}'),
('c7220000-0000-4000-8000-000000000010','lciamethod','support','c7220000-0000-4000-8000-000000000021','01.00.000',null,null,repeat('7',64),repeat('8',64),repeat('9',64),'{}');

set local role authenticated;
select set_config('request.jwt.claim.role','authenticated',true);
select set_config('request.jwt.claim.sub','c7220000-0000-4000-8000-000000000001',true);

create temporary table closure_e2e_responses(result jsonb);
insert into closure_e2e_responses values
(public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000020","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-a','{}')),
(public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000020","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-b','{}'));

reset role;

select is((select count(*) from closure_e2e_responses where result->>'ok'='true'),2::bigint,'two explicit runs are accepted against the release');
select is((select count(distinct scan_execution_id) from public.lcia_scope_closure_checks where request_idempotency_token in ('closure-e2e-a','closure-e2e-b')),1::bigint,'two runs share one release-bound scan execution');
select is((select count(distinct data_snapshot_token) from public.lcia_scope_closure_checks where request_idempotency_token in ('closure-e2e-a','closure-e2e-b')),1::bigint,'two runs freeze the same exact release snapshot');
select is(public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000099","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-live-only','{}')->>'code','invalid_closure_scope','live-only process identity is rejected outside the current release');

-- Run A owns the shared execution, records a complete scan, then run B turns
-- that completed scan into a distinct report/certificate without copying A's
-- report identity.
update public.worker_jobs set status='running', lease_token='c7220000-0000-4000-8000-000000000101', lease_expires_at=now()+interval '10 minutes', started_at=now()
where id=(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a');
select set_config('request.jwt.claim.role','service_role',true);
select is(public.svc_lcia_scope_closure_claim_scan_execution(
  (select scan_execution_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  (select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000101'
)->'data'->>'acquired','true','first worker acquires the shared scan execution lease');
insert into public.worker_job_artifacts(id,job_id,artifact_type,storage_bucket,storage_path,content_type,byte_size,checksum_sha256)
values ('c7220000-0000-4000-8000-000000000201',(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'closure_report','test','reports/a.xlsx','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',10,repeat('a',64));
select is(public.svc_lcia_scope_closure_check_record_result_v2(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  (select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000101','passed','complete',
  (select requested_scope_manifest from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  jsonb_build_object('schemaVersion','lcia.scope-closure-evidence.v1','sourceFingerprint','source-a','resolutionMapHash','resolution-a','closureBundleHash','bundle-a','snapshotId','c7220000-0000-4000-8000-000000000301','snapshotHash','snapshot-a','reportArtifactManifestHash',public.lcia_scope_closure_sha256(jsonb_build_object('artifactId','c7220000-0000-4000-8000-000000000201'::uuid,'bucket','test','objectPath','reports/a.xlsx','mediaType','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','byteSize',10,'checksumSha256',repeat('a',64))),'evidenceHash','evidence-a'),
  jsonb_build_object('scan','first'),'[]'::jsonb,'{}'::text[],'c7220000-0000-4000-8000-000000000201'
)->>'ok','true','first run records a lease-fenced complete certificate');
select is((select status from public.lcia_scope_closure_scan_executions limit 1),'completed','first completion makes the shared execution reusable');

update public.worker_jobs set status='running', lease_token='c7220000-0000-4000-8000-000000000102', lease_expires_at=now()+interval '10 minutes', started_at=now()
where id=(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b');
insert into public.worker_job_artifacts(id,job_id,artifact_type,storage_bucket,storage_path,content_type,byte_size,checksum_sha256)
values ('c7220000-0000-4000-8000-000000000202',(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),'closure_report','test','reports/b.xlsx','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',11,repeat('b',64));
select is(public.svc_lcia_scope_closure_reuse_completed_scan(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),
  (select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),
  'c7220000-0000-4000-8000-000000000102',
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
)->'data'->>'reuseAvailable','true','second run can reuse completed scan facts');
select is(public.svc_lcia_scope_closure_finalize_reused_scan(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),
  (select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),
  'c7220000-0000-4000-8000-000000000102',
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000202',jsonb_build_object('scan','reused-target')
)->>'ok','true','second run finalizes with a target-owned report');
select ok((select a.certificate_hash<>b.certificate_hash and b.reused_from_check_id=a.id and b.report_artifact_id='c7220000-0000-4000-8000-000000000202'::uuid and b.result_summary->>'scan'='reused-target' from public.lcia_scope_closure_checks a join public.lcia_scope_closure_checks b on true where a.request_idempotency_token='closure-e2e-a' and b.request_idempotency_token='closure-e2e-b'),'reuse creates a distinct certificate, report and target summary with source-run linkage');

select set_config('request.jwt.claim.role','authenticated',true);
select public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000020","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-fail','{}');
update public.worker_jobs set status='running', lease_token='c7220000-0000-4000-8000-000000000103', lease_expires_at=now()+interval '10 minutes', started_at=now()
where id=(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-fail');
select set_config('request.jwt.claim.role','service_role',true);
select is(public.svc_lcia_scope_closure_fail_before_scan((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-fail'),(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-fail'),'c7220000-0000-4000-8000-000000000104','bootstrap_failed')->>'code','worker_job_lease_invalid','early failure rejects a stale lease token');
select is(public.svc_lcia_scope_closure_fail_before_scan((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-fail'),(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-fail'),'c7220000-0000-4000-8000-000000000103','bootstrap_failed')->>'ok','true','early failure is lease fenced and records the run');
select is((select status from public.lcia_scope_closure_scan_executions limit 1),'completed','a later worker bootstrap failure does not corrupt the completed shared execution');

-- A content change with the same exact process identity is a new public
-- release, hence a new snapshot/fingerprint. Frozen evidence remains valid;
-- only a current-membership policy is evaluated against the new binding.
alter table public.lca_release_publications disable trigger user;
update public.lca_release_publications set is_current=false,status='superseded' where id='c7220000-0000-4000-8000-000000000012';
insert into public.lca_release_runs(id,release_version,selection_manifest_hash,input_manifest_hash,calculation_bundle_hash,calculation_bundle_ref,profile_lock_hash,publish_plan_hash,publish_plan,artifact_set_hash,release_manifest_hash,release_manifest,status,idempotency_key,request_hash,created_by)
values ('c7220000-0000-4000-8000-000000000030','77.00.002',repeat('a',64),repeat('b',64),repeat('c',64),'{}',repeat('d',64),repeat('e',64),'{}',repeat('f',64),repeat('0',64),'{}','published','closure-e2e-release-2',repeat('1',64),'c7220000-0000-4000-8000-000000000001');
insert into public.lca_release_approvals(id,release_run_id,publish_plan_hash,approval_hash,approved_by,approved_at,expires_at) values ('c7220000-0000-4000-8000-000000000031','c7220000-0000-4000-8000-000000000030',repeat('e',64),repeat('2',64),'c7220000-0000-4000-8000-000000000001',now(),now()+interval '1 day');
insert into public.lca_release_publications(id,release_run_id,release_version,approval_id,approval_hash,publish_plan_hash,release_manifest_hash,artifact_set_hash,approved_by,executed_by,credential_fingerprint,idempotency_key,published_at) values ('c7220000-0000-4000-8000-000000000032','c7220000-0000-4000-8000-000000000030','77.00.002','c7220000-0000-4000-8000-000000000031',repeat('2',64),repeat('e',64),repeat('0',64),repeat('f',64),'c7220000-0000-4000-8000-000000000001','c7220000-0000-4000-8000-000000000001',repeat('3',64),'closure-e2e-publication-2',now());
insert into public.lca_release_dataset_versions(release_run_id,dataset_type,dataset_role,dataset_uuid,dataset_version,source_process_uuid,source_process_version,version_significant_hash,semantic_hash,canonical_content_hash,artifact_ref) values ('c7220000-0000-4000-8000-000000000030','process','unit_process','c7220000-0000-4000-8000-000000000020','01.00.000','c7220000-0000-4000-8000-000000000020','01.00.000',repeat('4',64),repeat('5',64),repeat('0',64),'{}'),('c7220000-0000-4000-8000-000000000030','lciamethod','support','c7220000-0000-4000-8000-000000000021','01.00.000',null,null,repeat('7',64),repeat('8',64),repeat('9',64),'{}');
alter table public.lca_release_publications enable trigger user;
select set_config('request.jwt.claim.role','authenticated',true);
select public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000020","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-after-release','{}');
select ok((select a.data_snapshot_token<>n.data_snapshot_token and a.request_fingerprint<>n.request_fingerprint and a.certificate_status='valid' from public.lcia_scope_closure_checks a join public.lcia_scope_closure_checks n on true where a.request_idempotency_token='closure-e2e-a' and n.request_idempotency_token='closure-e2e-after-release'),'same identity with changed released content creates a new snapshot/fingerprint while frozen certificate remains valid');

alter table public.processes disable trigger user;
insert into public.processes(id,version,json,user_id,state_code) values ('c7220000-0000-4000-8000-000000000020','01.00.000','{"processDataSet":{"name":"release process"}}','c7220000-0000-4000-8000-000000000001',100);
alter table public.processes enable trigger user;
create temporary table closure_build_ids(label text primary key,id uuid) on commit drop;
insert into closure_build_ids select 'build-a',(r->'data'->>'buildId')::uuid from (select public.cmd_lcia_result_build_request_v2('frozen build',null,'subset',null,'[]'::jsonb,'closure-e2e-build-a',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select requested_scope_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select policy_fingerprint from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}') r) q;
select ok((select count(*)=1 from public.worker_jobs j join closure_build_ids b on j.subject_id=b.id where j.job_kind='lcia_result.package_build' and j.payload_json->>'closure_check_id'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')),'Build V2 atomically persists a certificate-bound worker payload');
insert into closure_build_ids select 'build-b',(r->'data'->>'buildId')::uuid from (select public.cmd_lcia_result_build_request_v2('frozen build two',null,'subset',null,'[]'::jsonb,'closure-e2e-build-b',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select requested_scope_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select policy_fingerprint from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}') r) q;
select ok((select count(*)=2 and count(distinct subject_id)=2 from public.worker_jobs where job_kind='lcia_result.package_build' and payload_json->>'closure_check_id'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')),'two explicit Build V2 requests create independent jobs bound to the same certificate');


select * from finish();
rollback;
