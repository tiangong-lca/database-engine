begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

-- A minimal immutable current release.  The closure request must use this
-- manifest, rather than any live state-code rows, as its identity universe.
insert into auth.users (instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous)
values ('00000000-0000-0000-0000-000000000000','c7220000-0000-4000-8000-000000000001','authenticated','authenticated','closure-e2e@example.com','x',now(),'{}','{}',now(),now(),false,false);
insert into public.users(id,raw_user_meta_data,contact) values ('c7220000-0000-4000-8000-000000000001','{}',null);
insert into auth.users (instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous)
values ('00000000-0000-0000-0000-000000000000','c7220000-0000-4000-8000-000000000002','authenticated','authenticated','closure-e2e-other-manager@example.com','x',now(),'{}','{}',now(),now(),false,false);
insert into public.users(id,raw_user_meta_data,contact) values ('c7220000-0000-4000-8000-000000000002','{}',null);
insert into public.teams(id,json,rank,is_public) values ('00000000-0000-0000-0000-000000000000','{"name":"System"}',0,false) on conflict(id) do nothing;
insert into public.roles(user_id,team_id,role) values ('c7220000-0000-4000-8000-000000000001','00000000-0000-0000-0000-000000000000','data_product_manager');
insert into public.roles(user_id,team_id,role) values ('c7220000-0000-4000-8000-000000000002','00000000-0000-0000-0000-000000000000','data_product_manager');

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
values ('c7220000-0000-4000-8000-000000000201',(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'closure_report_xlsx','test','reports/a.xlsx','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',10,repeat('a',64));
insert into public.worker_job_artifacts(id,job_id,artifact_type,storage_bucket,storage_path,content_type,byte_size,checksum_sha256,metadata)
values ('c7220000-0000-4000-8000-000000000203',(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'closure_bundle','test','bundles/a.json','application/json',20,repeat('b',64),jsonb_build_object('closureCheckId',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')));
update public.lca_network_snapshots set status='ready',source_hash='source-a'
where id=(select numerical_snapshot_id from public.lcia_scope_closure_scan_executions limit 1);
insert into public.lca_snapshot_artifacts(
  id,snapshot_id,artifact_url,artifact_sha256,artifact_byte_size,artifact_format,
  process_count,flow_count,impact_count,a_nnz,b_nnz,c_nnz,status,
  snapshot_index_sha256,snapshot_build_contract_hash,effective_scope_hash,
  data_snapshot_token,closure_bundle_hash
)
select
  'c7220000-0000-4000-8000-000000000204',e.numerical_snapshot_id,
  's3://test/snapshots/a.h5',repeat('c',64),100,'snapshot-hdf5:v1',
  1,1,1,1,1,1,'ready',repeat('d',64),
  public.lcia_scope_closure_sha256_text(
    'lcia.numerical-snapshot-build-contract.v1'||chr(10)
    ||public.lcia_scope_closure_sha256(c.requested_scope_manifest)||chr(10)
    ||c.data_snapshot_token||chr(10)||repeat('b',64)||chr(10)
    ||e.numerical_snapshot_id::text||chr(10)||'snapshot-hdf5:v1'
  ),
  public.lcia_scope_closure_sha256(c.requested_scope_manifest),
  c.data_snapshot_token,repeat('b',64)
from public.lcia_scope_closure_checks c
join public.lcia_scope_closure_scan_executions e on e.id=c.scan_execution_id
where c.request_idempotency_token='closure-e2e-a';
select is(public.svc_lcia_scope_closure_check_record_result_v2(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  (select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000101','passed','complete',
  (select requested_scope_manifest from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  jsonb_build_object('snapshotId',gen_random_uuid(),'snapshotHash',repeat('f',64)),
  '{}'::jsonb,'[]'::jsonb,'{}'::text[],'c7220000-0000-4000-8000-000000000201'
)->>'code','closure_snapshot_evidence_v3_required','JSON-only fake snapshot evidence cannot sign a passed certificate');
select is(public.svc_lcia_scope_closure_check_record_result_v3(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  (select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000101','passed','complete',
  (select requested_scope_manifest from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  jsonb_build_object(
    'schemaVersion','lcia.scope-closure-evidence.v2',
    'sourceFingerprint','source-a','resolutionMapHash',repeat('e',64),
    'closureBundleArtifactId','c7220000-0000-4000-8000-000000000203',
    'closureBundleHash',repeat('b',64),
    'snapshotId',(select numerical_snapshot_id from public.lcia_scope_closure_scan_executions limit 1),
    'snapshotHash',repeat('c',64),
    'snapshotArtifactId','c7220000-0000-4000-8000-000000000204',
    'snapshotIndexSha256',repeat('d',64),
    'snapshotBuildContractHash',(select snapshot_build_contract_hash from public.lca_snapshot_artifacts where id='c7220000-0000-4000-8000-000000000204'),
    'reportArtifactManifestHash',public.lcia_scope_closure_sha256(jsonb_build_object('artifactId','c7220000-0000-4000-8000-000000000201'::uuid,'bucket','test','objectPath','reports/a.xlsx','mediaType','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','byteSize',10,'checksumSha256',repeat('a',64))),
    'evidenceHash',public.lcia_scope_closure_sha256_text(
      'lcia.scope-closure-evidence.v2'||chr(10)||'source-a'||chr(10)||repeat('e',64)||chr(10)
      ||repeat('b',64)||chr(10)||'c7220000-0000-4000-8000-000000000203'||chr(10)
      ||(select numerical_snapshot_id::text from public.lcia_scope_closure_scan_executions limit 1)||chr(10)
      ||repeat('c',64)||chr(10)||'c7220000-0000-4000-8000-000000000204'||chr(10)||repeat('d',64)||chr(10)
      ||(select snapshot_build_contract_hash from public.lca_snapshot_artifacts where id='c7220000-0000-4000-8000-000000000204')
    )
  ),
  jsonb_build_object(
    'schemaVersion','lcia.scope-closure-summary.v1',
    'scan','first',
    'evidenceHash',public.lcia_scope_closure_sha256_text(
      'lcia.scope-closure-evidence.v2'||chr(10)||'source-a'||chr(10)||repeat('e',64)||chr(10)
      ||repeat('b',64)||chr(10)||'c7220000-0000-4000-8000-000000000203'||chr(10)
      ||(select numerical_snapshot_id::text from public.lcia_scope_closure_scan_executions limit 1)||chr(10)
      ||repeat('c',64)||chr(10)||'c7220000-0000-4000-8000-000000000204'||chr(10)||repeat('d',64)||chr(10)
      ||(select snapshot_build_contract_hash from public.lca_snapshot_artifacts where id='c7220000-0000-4000-8000-000000000204')
    )
  ),
  '[]'::jsonb,'{}'::text[],'c7220000-0000-4000-8000-000000000201',
  'c7220000-0000-4000-8000-000000000203','c7220000-0000-4000-8000-000000000204'
)->>'ok','true','first run records a lease-fenced complete certificate');
select isnt(
  (select evidence_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  public.lcia_scope_closure_sha256_text(
    'lcia.scope-closure-evidence.v2'||chr(10)||'source-a'||chr(10)||repeat('e',64)||chr(10)
    ||repeat('b',64)||chr(10)||'c7220000-0000-4000-8000-000000000205'||chr(10)
    ||(select numerical_snapshot_id::text from public.lcia_scope_closure_scan_executions limit 1)||chr(10)
    ||repeat('c',64)||chr(10)||'c7220000-0000-4000-8000-000000000204'||chr(10)||repeat('d',64)||chr(10)
    ||(select snapshot_build_contract_hash from public.lca_snapshot_artifacts where id='c7220000-0000-4000-8000-000000000204')
  ),
  'tampering the closure bundle artifact identity changes the canonical evidence hash'
);
select is((select status from public.lcia_scope_closure_scan_executions limit 1),'completed','first completion makes the shared execution reusable');
select throws_ok(
  $$delete from public.lca_snapshot_artifacts
    where id='c7220000-0000-4000-8000-000000000204'$$,
  '23503','lca_snapshot_artifact_has_valid_closure_certificate',
  'a valid certificate prevents direct deletion of its HDF5 artifact'
);
select throws_ok(
  $$delete from public.lca_network_snapshots
    where id=(select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')$$,
  '23503','lca_snapshot_has_valid_closure_certificate',
  'a valid certificate prevents deletion of its numerical snapshot'
);

update public.worker_jobs set status='running', lease_token='c7220000-0000-4000-8000-000000000102', lease_expires_at=now()+interval '10 minutes', started_at=now()
where id=(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b');
insert into public.worker_job_artifacts(id,job_id,artifact_type,storage_bucket,storage_path,content_type,byte_size,checksum_sha256)
values ('c7220000-0000-4000-8000-000000000202',(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),'closure_report_xlsx','test','reports/b.xlsx','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',11,repeat('b',64));
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
  'c7220000-0000-4000-8000-000000000202',jsonb_build_object('schemaVersion','lcia.scope-closure-summary.v1','scan','reused-target')
)->>'ok','true','second run finalizes with a target-owned report');
select ok((select a.certificate_hash<>b.certificate_hash and b.reused_from_check_id=a.id and b.report_artifact_id='c7220000-0000-4000-8000-000000000202'::uuid and b.result_summary->>'scan'='reused-target' from public.lcia_scope_closure_checks a join public.lcia_scope_closure_checks b on true where a.request_idempotency_token='closure-e2e-a' and b.request_idempotency_token='closure-e2e-b'),'reuse creates a distinct certificate, report and target summary with source-run linkage');

-- Operator visibility is strictly owner-scoped.  A second valid DPM cannot
-- use SECURITY DEFINER reads, report lookup, or the task feed to enumerate
-- the first manager's closure run.
select set_config('request.jwt.claim.role','authenticated',true);
select set_config('request.jwt.claim.sub','c7220000-0000-4000-8000-000000000001',true);
select is(public.get_lcia_scope_closure_check((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))->>'ok','true','closure owner can read its check');
select is(public.list_lcia_scope_closure_issues((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))->>'ok','true','closure owner can read its issue page');
select is((public.list_lcia_scope_closure_issues((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))#>>'{data,totalCount}')::integer,0,'closure owner issue page reports an accurate total count');
select is(public.get_lcia_scope_closure_report_download((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))->>'ok','true','closure owner can read its report download metadata');
select set_config('request.jwt.claim.sub','c7220000-0000-4000-8000-000000000002',true);
select is(public.get_lcia_scope_closure_check((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))->>'code','closure_check_not_found','second manager cannot read another manager closure check');
select is(public.list_lcia_scope_closure_issues((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))->>'code','closure_check_not_found','second manager cannot read another manager closure issues');
select is(public.get_lcia_scope_closure_report_download((select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'))->>'code','closure_check_not_found','second manager cannot read another manager closure report');
select is(jsonb_array_length(public.get_task_summary_v2_feed('data_product',array['lcia.scope_closure_check','lcia_result.package_build']::text[],null,null,null,null,50,false)->'data'->'items'),0,'second manager task feed excludes another manager closure work');
select set_config('request.jwt.claim.sub','c7220000-0000-4000-8000-000000000001',true);

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
update public.lcia_scope_closure_checks set requested_scope_manifest=jsonb_set(requested_scope_manifest,'{certificateFreshnessPolicy}','"current-membership-required-v1"') where request_idempotency_token='closure-e2e-a';
select is(public.cmd_lcia_result_build_request_v2('current build',null,'subset',null,'[]'::jsonb,'closure-e2e-current-stale',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select requested_scope_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select policy_fingerprint from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}')->>'code','closure_check_stale','current-membership certificate is rejected after its release binding changes');
update public.lcia_scope_closure_checks set requested_scope_manifest=jsonb_set(requested_scope_manifest,'{certificateFreshnessPolicy}','"frozen-artifact-reusable-v1"') where request_idempotency_token='closure-e2e-a';

alter table public.processes disable trigger user;
insert into public.processes(id,version,json,user_id,state_code) values ('c7220000-0000-4000-8000-000000000020','01.00.000','{"processDataSet":{"name":"release process"}}','c7220000-0000-4000-8000-000000000001',100);
alter table public.processes enable trigger user;
create temporary table closure_build_ids(label text primary key,id uuid) on commit drop;
insert into closure_build_ids select 'build-a',(r->'data'->>'buildId')::uuid from (select public.cmd_lcia_result_build_request_v2('frozen build',null,'subset',null,'[]'::jsonb,'closure-e2e-build-a',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select requested_scope_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select policy_fingerprint from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}') r) q;
select ok((select count(*)=1 from public.worker_jobs j join closure_build_ids b on j.subject_id=b.id where j.job_kind='lcia_result.package_build' and j.payload_json->>'closure_check_id'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')),'Build V2 atomically persists a certificate-bound worker payload');
insert into closure_build_ids select 'build-b',(r->'data'->>'buildId')::uuid from (select public.cmd_lcia_result_build_request_v2('frozen build two',null,'subset',null,'[]'::jsonb,'closure-e2e-build-b',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select requested_scope_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select policy_fingerprint from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}') r) q;
select ok((select count(*)=2 and count(distinct subject_id)=2 from public.worker_jobs where job_kind='lcia_result.package_build' and payload_json->>'closure_check_id'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')),'two explicit Build V2 requests create independent jobs bound to the same certificate');
insert into closure_build_ids select 'build-revoked',(r->'data'->>'buildId')::uuid from (select public.cmd_lcia_result_build_request_v2('revocation fence build',null,'subset',null,'[]'::jsonb,'closure-e2e-build-revoked',(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select requested_scope_hash from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select policy_fingerprint from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}') r) q;

-- Exercise the real package-ready boundary using the certificate's pinned
-- snapshot and output rows.  This is intentionally not a trigger-only unit
-- test: the package is created through the service command a Worker uses.
insert into public.lca_network_snapshots (id,scope,process_filter,source_hash,status,created_by)
values ('c7220000-0000-4000-8000-000000000302','data_product','{"release":"77.00.001","wrong":true}'::jsonb,'snapshot-wrong','ready','c7220000-0000-4000-8000-000000000001');
insert into public.lca_results (id,job_id,snapshot_id,payload,diagnostics,artifact_url,artifact_sha256,artifact_byte_size,artifact_format,worker_job_id,is_pinned)
values ('c7220000-0000-4000-8000-000000000401',(select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-a')),(select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),'{}'::jsonb,'{}'::jsonb,'s3://test/closure-e2e-result.json',repeat('c',64),128,'application/json',(select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-a')),false);
insert into public.lca_latest_all_unit_results (id,snapshot_id,job_id,result_id,query_artifact_url,query_artifact_sha256,query_artifact_byte_size,query_artifact_format,status,worker_job_id)
values ('c7220000-0000-4000-8000-000000000402',(select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),(select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-a')),'c7220000-0000-4000-8000-000000000401','s3://test/closure-e2e-query.json',repeat('d',64),64,'application/json','ready',(select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-a')));

update public.worker_jobs
set status='running',lease_token=gen_random_uuid(),lease_expires_at=now()+interval '10 minutes',started_at=coalesce(started_at,now())
where subject_id in (select id from closure_build_ids);
select set_config('request.jwt.claim.role','service_role',true);

select ok((
  select payload_json->>'closure_bundle_artifact_id'=(
           select closure_bundle_artifact_id::text
           from public.lcia_scope_closure_checks
           where request_idempotency_token='closure-e2e-a'
         )
     and payload_json->'input_manifest'=jsonb_build_object(
           'predicateVersion',(select effective_scope_manifest->>'eligibilityPredicateVersion' from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
           'selectionMode','closure_certificate',
           'processes',(select effective_scope_manifest->'processes' from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
         )
  from public.worker_jobs
  where subject_id=(select id from closure_build_ids where label='build-a')
),'Build V2 persists the closure bundle artifact and exact certificate process axis');
select ok((
  select result->>'ok'='true'
     and result#>>'{data,closureBundleArtifactId}'=(
           select closure_bundle_artifact_id::text
           from public.lcia_scope_closure_checks
           where request_idempotency_token='closure-e2e-a'
         )
     and result#>>'{data,snapshotArtifactId}'=(
           select snapshot_artifact_id::text
           from public.lcia_scope_closure_checks
           where request_idempotency_token='closure-e2e-a'
         )
     and result#>'{data,inputManifest}'=(
           select payload_json->'input_manifest'
           from public.worker_jobs
           where subject_id=(select id from closure_build_ids where label='build-a')
         )
     and not (result->'data' ? 'reportArtifactManifestHash')
  from (select public.svc_lcia_scope_closure_build_binding(
    (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-a'))
  ) result) binding
),'after-lease binding returns the authoritative snapshot, bundle, and exact process axis without extending the solver contract');

update public.worker_jobs
set payload_json=jsonb_set(payload_json,'{lcia_method_set}','[]'::jsonb)
where subject_id=(select id from closure_build_ids where label='build-b');
select is(public.svc_lcia_scope_closure_build_binding(
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-b'))
)->>'code','build_binding_mismatch','after-lease binding rejects a tampered LCIA method axis');
select is(public.cmd_lcia_result_package_mark_ready(
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-b')),
  'closure-e2e-package-tampered-methods',
  (select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000401',null,
  '{}'::jsonb,'{}'::jsonb,'{}'::jsonb,jsonb_build_array('climate-change'),'climate-change','tampered-methods','{}'::jsonb
)->>'code','closure_certificate_binding_mismatch','mark_ready rechecks and rejects a tampered LCIA method axis');
update public.worker_jobs
set payload_json=jsonb_set(
  payload_json,
  '{lcia_method_set}',
  (select effective_scope_manifest->'lciaMethods' from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
)
where subject_id=(select id from closure_build_ids where label='build-b');

create temporary table closure_e2e_package_ids (label text primary key,id uuid) on commit drop;
select set_config('request.jwt.claim.role','service_role',true);
insert into closure_e2e_package_ids
select 'package-a',(r->'data'->>'packageId')::uuid
from (
  select public.cmd_lcia_result_package_mark_ready(
    (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-a')),
    'closure-e2e-package-a',
    (select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
    'c7220000-0000-4000-8000-000000000401',
    'c7220000-0000-4000-8000-000000000402',
    '{}'::jsonb,'{}'::jsonb,jsonb_build_object('persistenceMode','pinned'),jsonb_build_array('climate-change'),'climate-change','closure-e2e-package-result','{}'::jsonb
  ) r
) q;
select ok((
  select p.closure_check_id=c.id
     and p.closure_certificate_hash=c.certificate_hash
     and p.closure_snapshot_hash=c.snapshot_hash
  from public.lcia_result_packages p
  join public.lcia_scope_closure_checks c on c.id=p.closure_check_id
  where p.id=(select id from closure_e2e_package_ids where label='package-a')
),'mark_ready persists closure check, certificate hash and snapshot hash from the bound build');
select is(public.cmd_lcia_result_package_mark_ready(
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-b')),
  'closure-e2e-package-wrong-snapshot',
  'c7220000-0000-4000-8000-000000000302',
  'c7220000-0000-4000-8000-000000000401',
  null,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,jsonb_build_array('climate-change'),'climate-change','wrong-snapshot','{}'::jsonb
)->>'code','closure_certificate_binding_mismatch','mark_ready fails closed when a build supplies a snapshot other than its certificate snapshot');
update public.worker_jobs
set payload_json=jsonb_set(payload_json,'{snapshot_hash}','"tampered-snapshot-hash"'::jsonb)
where subject_id=(select id from closure_build_ids where label='build-b');
select is(public.cmd_lcia_result_package_mark_ready(
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-b')),
  'closure-e2e-package-tampered-hash',
  (select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000401',
  null,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,jsonb_build_array('climate-change'),'climate-change','tampered-snapshot-hash','{}'::jsonb
)->>'code','closure_certificate_binding_mismatch','mark_ready fails closed when the queued certificate snapshot hash is tampered');

-- The global Task Center consumes the server projection.  Assert a true
-- keyset page, then an event-driven update that leaves worker lifecycle and
-- certificate validity as independent axes.
select set_config('request.jwt.claim.role','authenticated',true);
create temporary table closure_e2e_task_feed (label text primary key,result jsonb) on commit drop;
insert into closure_e2e_task_feed
values ('page-1',public.get_task_summary_v2_feed('data_product',array['lcia.scope_closure_check','lcia_result.package_build']::text[],null,null,null,null,1,false));
insert into closure_e2e_task_feed
select 'page-2',public.get_task_summary_v2_feed(
  'data_product',array['lcia.scope_closure_check','lcia_result.package_build']::text[],null,null,
  (result#>>'{data,nextCursor,updatedAt}')::timestamptz,
  (result#>>'{data,nextCursor,jobId}')::uuid,1,false
)
from closure_e2e_task_feed where label='page-1';
select ok((select result#>'{data,nextCursor}' is not null from closure_e2e_task_feed where label='page-1'),'task feed returns a stable keyset cursor when more data-product tasks exist');
select ok((select (p1.result#>>'{data,items,0,jobId}')<>(p2.result#>>'{data,items,0,jobId}') from closure_e2e_task_feed p1 join closure_e2e_task_feed p2 on true where p1.label='page-1' and p2.label='page-2'),'task feed cursor advances without repeating the first task');

create temporary table closure_e2e_projection_before (updated_at timestamptz not null) on commit drop;
insert into closure_e2e_projection_before
select (item->>'projectionUpdatedAt')::timestamptz
from jsonb_array_elements(public.get_task_summary_v2_feed('data_product',array['lcia.scope_closure_check']::text[],null,null,null,null,50,false)->'data'->'items') item
where item->>'closureCheckId'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a');
select is((select count(*) from closure_e2e_projection_before),1::bigint,'completed closure task is present before a validity event');

select set_config('request.jwt.claim.role','service_role',true);
select is(public.svc_lcia_scope_closure_certificate_event(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'stale','scope content was superseded'
)->>'ok','true','certificate event marks a completed closure certificate stale');
select throws_ok(
  $$update public.lcia_scope_closure_certificate_events
    set reason='rewritten history'
    where closure_check_id=(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')$$,
  '23514','lcia_scope_closure_certificate_event_immutable',
  'certificate event history cannot be rewritten even through direct SQL'
);
select throws_ok(
  $$delete from public.lcia_scope_closure_certificate_events
    where closure_check_id=(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')$$,
  '23514','lcia_scope_closure_certificate_event_immutable',
  'certificate event history cannot be deleted even through direct SQL'
);
select set_config('request.jwt.claim.role','authenticated',true);
insert into closure_e2e_task_feed
select 'after-stale',public.get_task_summary_v2_feed(
  'data_product',array['lcia.scope_closure_check']::text[],null,
  (select updated_at + interval '1 microsecond' from closure_e2e_projection_before),null,null,50,false
);
select ok((select exists(
  select 1 from jsonb_array_elements(result->'data'->'items') item
  where item->>'closureCheckId'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
    and item->>'workerStatus'='completed'
    and item->>'domainValidity'='stale'
    and (item->>'projectionUpdatedAt')::timestamptz>(select updated_at from closure_e2e_projection_before)
) from closure_e2e_task_feed where label='after-stale'),'certificate staleness advances the projection and reappears after updatedSince without changing completed worker status');

select set_config('request.jwt.claim.role','service_role',true);
select is(public.svc_lcia_scope_closure_certificate_event(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'revoked','operator revoked stale certificate'
)->>'ok','true','stale certificate can be revoked by a second append-only event');
insert into public.lca_results (id,job_id,snapshot_id,payload,diagnostics,artifact_url,artifact_sha256,artifact_byte_size,artifact_format,worker_job_id,is_pinned)
values (
  'c7220000-0000-4000-8000-000000000403',
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-revoked')),
  (select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  '{}'::jsonb,'{}'::jsonb,'s3://test/closure-e2e-revoked-result.json',repeat('e',64),64,'application/json',
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-revoked')),false
);
select is(public.cmd_lcia_result_package_mark_ready(
  (select id from public.worker_jobs where subject_id=(select id from closure_build_ids where label='build-revoked')),
  'closure-e2e-package-revoked',
  (select snapshot_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a'),
  'c7220000-0000-4000-8000-000000000403',null,
  '{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'[]'::jsonb,null,'revoked-result','{}'::jsonb
)->>'code','closure_check_revoked','mark_ready rejects a certificate revoked after the build acquired its lease');
select is((select count(*) from public.lcia_result_packages where package_version='closure-e2e-package-revoked'),0::bigint,'revocation-before-ready inserts no package row');
update public.worker_jobs set status='stale',updated_at=clock_timestamp()
where id=(select worker_job_id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b');
select set_config('request.jwt.claim.role','authenticated',true);
insert into closure_e2e_task_feed
values ('after-revoked',public.get_task_summary_v2_feed('data_product',array['lcia.scope_closure_check','lcia_result.package_build']::text[],null,null,null,null,50,false));
select ok((select exists(
  select 1 from jsonb_array_elements(result->'data'->'items') item
  where item->>'closureCheckId'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
    and item->>'workerStatus'='completed'
    and item->>'domainValidity'='revoked'
) from closure_e2e_task_feed where label='after-revoked'),'task feed projects revoked certificate validity without rewriting the completed worker lifecycle');
select ok((select exists(
  select 1 from jsonb_array_elements(result->'data'->'items') item
  where item->>'closureCheckId'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b')
    and item->>'workerStatus'='stale'
    and item->>'domainValidity'='valid'
) from closure_e2e_task_feed where label='after-revoked'),'runtime worker staleness remains distinct from a valid closure certificate');
select ok((select exists(
  select 1 from jsonb_array_elements(result->'data'->'items') item
  where item->>'resultPackageId'=(select id::text from closure_e2e_package_ids where label='package-a')
    and item#>>'{capabilities,canPreviewResult}'='true'
    and item#>>'{capabilities,canOpenWorkbench}'='true'
    and item#>>'{deepLink,routeKey}'='data_product.package'
    and item#>>'{deepLink,params,closureCheckId}'=(select id::text from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
) from closure_e2e_task_feed where label='after-revoked'),'package ready advances the task projection with preview capability and a certificate-aware package deep link');

-- Once every certificate for the snapshot is revoked and strong package/result
-- references are removed, retention may delete bytes while the immutable UUID
-- and hash audit values remain on the historical checks and scan execution.
select set_config('request.jwt.claim.role','service_role',true);
select is(public.svc_lcia_scope_closure_certificate_event(
  (select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-b'),
  'revoked','retention lifecycle test'
)->>'ok','true','the final usable certificate can be revoked before retention');
create temporary table closure_e2e_gc_refs on commit drop as
select
  c.snapshot_id,
  c.snapshot_artifact_id,
  c.snapshot_hash,
  c.snapshot_index_sha256,
  c.snapshot_build_contract_hash,
  e.numerical_snapshot_id
from public.lcia_scope_closure_checks c
join public.lcia_scope_closure_scan_executions e on e.id=c.scan_execution_id
where c.request_idempotency_token='closure-e2e-a';
insert into storage.buckets(id,name,public)
values ('lca_results','lca_results',false)
on conflict(id) do nothing;
insert into storage.objects(bucket_id,name,metadata,created_at,updated_at)
select
  'lca_results',
  'lca-results/snapshots/'||snapshot_id::text||'/snapshot.h5',
  '{"size":100}'::jsonb,
  now()-interval '40 days',
  now()-interval '40 days'
from closure_e2e_gc_refs;
update public.lca_network_snapshots
set created_at=now()-interval '40 days',updated_at=now()-interval '40 days'
where id=(select snapshot_id from closure_e2e_gc_refs);
select throws_ok(
  $$delete from public.lca_snapshot_artifacts where id=(select snapshot_artifact_id from closure_e2e_gc_refs)$$,
  '23503','lca_snapshot_artifact_has_result_reference',
  'revocation does not permit artifact deletion while package or result references remain'
);
select throws_ok(
  $$delete from public.lca_network_snapshots where id=(select snapshot_id from closure_e2e_gc_refs)$$,
  '23503','lca_snapshot_has_result_reference',
  'revocation does not permit deletion while package or result references remain'
);
select is((select count(*) from util.list_lca_snapshot_gc_candidates(
  interval '1 day',interval '1 day',now(),10,10,100000
) where snapshot_id=(select snapshot_id from closure_e2e_gc_refs)),0::bigint,'strong result/package references keep a revoked snapshot out of GC candidates');
delete from public.lcia_result_packages
where id=(select id from closure_e2e_package_ids where label='package-a');
delete from public.lca_latest_all_unit_results
where snapshot_id=(select snapshot_id from closure_e2e_gc_refs);
delete from public.lca_results
where snapshot_id=(select snapshot_id from closure_e2e_gc_refs);
select ok(exists(
  select 1 from util.list_lca_snapshot_gc_candidates(
    interval '1 day',interval '1 day',now(),10,10,100000
  ) where snapshot_id=(select snapshot_id from closure_e2e_gc_refs)
),'a revoked snapshot without package/result references enters GC candidates');
delete from public.lca_network_snapshots
where id=(select snapshot_id from closure_e2e_gc_refs);
select is((select count(*) from public.lca_network_snapshots where id=(select snapshot_id from closure_e2e_gc_refs)),0::bigint,'retention can delete a revoked unreferenced snapshot and its artifact');
select ok((
  select count(*)=2
     and bool_and(c.snapshot_id=g.snapshot_id)
     and bool_and(c.snapshot_artifact_id=g.snapshot_artifact_id)
     and bool_and(c.snapshot_hash=g.snapshot_hash)
     and bool_and(c.snapshot_index_sha256=g.snapshot_index_sha256)
     and bool_and(c.snapshot_build_contract_hash=g.snapshot_build_contract_hash)
  from public.lcia_scope_closure_checks c
  cross join closure_e2e_gc_refs g
  where c.request_idempotency_token in ('closure-e2e-a','closure-e2e-b')
),'retention preserves certificate snapshot UUIDs and hash audit values as soft immutable references');
select ok((select e.numerical_snapshot_id=g.numerical_snapshot_id
  from public.lcia_scope_closure_scan_executions e
  cross join closure_e2e_gc_refs g
  where e.completed_check_id=(select id from public.lcia_scope_closure_checks where request_idempotency_token='closure-e2e-a')
),'retention preserves the scan execution numerical snapshot UUID as a soft immutable reference');

select * from finish();
rollback;
