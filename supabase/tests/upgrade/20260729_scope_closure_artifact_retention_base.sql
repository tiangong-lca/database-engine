begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select plan(2);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values (
  '00000000-0000-0000-0000-000000000000',
  '30830000-0000-4000-8000-000000000001',
  'authenticated', 'authenticated', 'issue-308-upgrade-owner@example.com',
  'x', now(), '{}', '{}', now(), now(), false, false
);
insert into public.users (id, raw_user_meta_data, contact)
values ('30830000-0000-4000-8000-000000000001', '{}', null);

insert into public.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  visibility, payload_schema_version, payload_json, status, created_at
) values (
  '30830000-0000-4000-8000-000000000101',
  'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
  '30830000-0000-4000-8000-000000000001', 'operator',
  'lcia.scope_closure_check.request.v1', '{}', 'completed',
  now() - interval '8 days'
);

insert into public.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata, created_at
) values
  (
    '30830000-0000-4000-8000-000000000201',
    '30830000-0000-4000-8000-000000000101',
    'closure_report_xlsx', 'upgrade-evidence', 'historical/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    201, repeat('1', 64), '{}', now() - interval '8 days'
  ),
  (
    '30830000-0000-4000-8000-000000000202',
    '30830000-0000-4000-8000-000000000101',
    'closure_complete_machine_result', 'upgrade-evidence',
    'historical/manifest.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    202, repeat('2', 64), '{}', now() - interval '8 days'
  ),
  (
    '30830000-0000-4000-8000-000000000203',
    '30830000-0000-4000-8000-000000000101',
    'closure_bundle', 'upgrade-evidence', 'historical/bundle.json',
    'application/json', 203, repeat('3', 64),
    '{"completeMachineResultArtifactId":"30830000-0000-4000-8000-000000000202"}',
    now() - interval '8 days'
  );

insert into public.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, effective_scope_hash,
  policy_fingerprint, data_snapshot_token,
  expected_validator_scanner_fingerprint, status, scan_completeness,
  certificate_status, certificate_hash, report_artifact_id, result_summary,
  finished_at, requested_scope_manifest, effective_scope_manifest,
  certificate_schema_version, source_fingerprint, resolution_map_hash,
  closure_bundle_hash, snapshot_id, snapshot_hash,
  report_artifact_manifest_hash, evidence_hash, closure_bundle_artifact_id,
  snapshot_artifact_id, snapshot_index_sha256, snapshot_build_contract_hash,
  created_at, updated_at
) values (
  '30830000-0000-4000-8000-000000000301',
  '30830000-0000-4000-8000-000000000101',
  '30830000-0000-4000-8000-000000000001',
  'historical-valid', 'historical-valid-key', repeat('4', 64),
  repeat('5', 64), repeat('6', 64), repeat('7', 64),
  'historical-snapshot-token',
  'scope-closure-validator-scanner.v1', 'passed', 'complete', 'valid',
  repeat('8', 64), '30830000-0000-4000-8000-000000000201',
  '{"historical":true}', now() - interval '7 days 23 hours',
  '{}', '{}', 'lcia.scope-closure-certificate.v2',
  'historical-source', repeat('9', 64), repeat('a', 64),
  '30830000-0000-4000-8000-000000000501', repeat('b', 64),
  repeat('c', 64), repeat('d', 64),
  '30830000-0000-4000-8000-000000000203',
  '30830000-0000-4000-8000-000000000502',
  repeat('e', 64), repeat('f', 64),
  now() - interval '8 days', now() - interval '7 days 23 hours'
);

select is(
  (
    select certificate_status
    from public.lcia_scope_closure_checks
    where id = '30830000-0000-4000-8000-000000000301'
  ),
  'valid',
  'canonical PR base contains the historical valid certificate fixture'
);
select is(
  (
    select count(*)
    from public.worker_job_artifacts
    where job_id = '30830000-0000-4000-8000-000000000101'
  ),
  3::bigint,
  'canonical PR base contains all three historical evidence artifacts'
);

select * from finish();
commit;
