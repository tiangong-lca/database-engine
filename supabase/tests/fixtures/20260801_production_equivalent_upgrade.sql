\set ON_ERROR_STOP on

set application_name = 'database-engine-issue-341-fixture';
set lock_timeout = '5s';
set statement_timeout = '10min';

begin;

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values (
  '00000000-0000-0000-0000-000000000000',
  '34100000-0000-4000-8000-000000000001',
  'authenticated', 'authenticated', 'issue-341-owner@example.invalid', 'x',
  '2026-08-01 00:00:00+00', '{}', '{"fixture":"issue-341"}',
  '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00', false, false
);

insert into public.users (id, raw_user_meta_data, contact)
values (
  '34100000-0000-4000-8000-000000000001',
  '{"fixture":"issue-341"}',
  '{"email":"redacted@example.invalid"}'
);

insert into public.reviews (
  id, data_id, data_version, state_code, reviewer_id, json,
  created_at, modified_at, deadline, review_kind, target_table,
  submitted_revision_checksum, target_owner_id
) values (
  '34100000-0000-4000-8000-000000000010',
  '34100000-0000-4000-8000-000000000011', '01.00.000', 0,
  jsonb_build_array('34100000-0000-4000-8000-000000000001'::uuid),
  '{"fixture":"issue-341","comment":"representative active review"}',
  '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00',
  '2026-08-08 00:00:00+00', 'reference', 'processes', repeat('3', 64),
  '34100000-0000-4000-8000-000000000001'
);

insert into public.notifications (
  id, recipient_user_id, sender_user_id, type, dataset_type, dataset_id,
  dataset_version, json, created_at, modified_at
) values (
  '34100000-0000-4000-8000-000000000020',
  '34100000-0000-4000-8000-000000000001',
  '34100000-0000-4000-8000-000000000001',
  'review_fixture', 'processes',
  '34100000-0000-4000-8000-000000000011', '01.00.000',
  '{"fixture":"issue-341","event_key":"issue-341-notification"}',
  '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00'
);

insert into public.command_audit_log (
  command, actor_user_id, target_table, target_id, target_version, payload,
  created_at
) values (
  'issue_341_fixture', '34100000-0000-4000-8000-000000000001',
  'processes', '34100000-0000-4000-8000-000000000011', '01.00.000',
  '{"fixture":"issue-341"}', '2026-08-01 00:00:00+00'
);

insert into public.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  status, phase, attempt_count, max_attempts, leased_by, lease_token,
  lease_expires_at, heartbeat_at, retryable, concurrency_key,
  payload_schema_version, payload_json, diagnostics, created_at, updated_at
) values
  (
    '34100000-0000-4000-8000-000000000101', 'lca.snapshot_gc', 'calculator',
    'maintenance', 'service', null, 'queued', 'admission', 0, 3,
    null, null, null, null, true, 'issue-341-queued',
    'issue-341.fixture.v1', '{"surface":"worker-active"}', '{}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00'
  ),
  (
    '34100000-0000-4000-8000-000000000102', 'lca.snapshot_gc', 'calculator',
    'maintenance', 'service', null, 'running', 'execute', 1, 3,
    'issue-341-worker', '34100000-0000-4000-8000-000000000902',
    '2026-08-01 00:10:00+00', '2026-08-01 00:00:30+00', true,
    'issue-341-running', 'issue-341.fixture.v1',
    '{"surface":"worker-lease"}', '{}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:00:30+00'
  ),
  (
    '34100000-0000-4000-8000-000000000103', 'lca.snapshot_gc', 'calculator',
    'maintenance', 'service', null, 'stale', 'retry', 2, 3,
    null, null, null, null, true, 'issue-341-stale',
    'issue-341.fixture.v1', '{"surface":"worker-retry"}',
    '{"lastFailure":"synthetic"}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:01:00+00'
  ),
  (
    '34100000-0000-4000-8000-000000000104', 'lca.snapshot_gc', 'calculator',
    'maintenance', 'service', null, 'failed', 'terminal', 3, 3,
    null, null, null, null, false, null,
    'issue-341.fixture.v1', '{"surface":"worker-failure"}',
    '{"error":"synthetic"}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:02:00+00'
  ),
  (
    '34100000-0000-4000-8000-000000000105', 'lca.snapshot_gc', 'calculator',
    'maintenance', 'service', null, 'completed', 'terminal', 1, 3,
    null, null, null, null, false, null,
    'issue-341.fixture.v1', '{"surface":"worker-artifact"}', '{}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:03:00+00'
  ),
  (
    '34100000-0000-4000-8000-000000000106', 'lcia.scope_closure_check',
    'calculator', 'solver', 'operator',
    '34100000-0000-4000-8000-000000000001', 'queued', 'admission', 0, 3,
    null, null, null, null, true, 'issue-341-closure',
    'lcia.scope_closure_check.request.v1', '{"surface":"closure"}', '{}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00'
  ),
  (
    '34100000-0000-4000-8000-000000000107', 'tidas.export_package',
    'calculator', 'package', 'operator',
    '34100000-0000-4000-8000-000000000001', 'running', 'materialize', 1, 3,
    'issue-341-worker', '34100000-0000-4000-8000-000000000907',
    '2026-08-01 00:10:00+00', '2026-08-01 00:00:30+00', true,
    'issue-341-package', 'tidas.export_package.request.v1',
    '{"surface":"package"}', '{}',
    '2026-08-01 00:00:00+00', '2026-08-01 00:00:30+00'
  );

insert into public.worker_job_events (
  id, job_id, event_type, status, phase, worker_id, lease_token, details,
  created_at
) values (
  '34100000-0000-4000-8000-000000000201',
  '34100000-0000-4000-8000-000000000102', 'leased', 'running', 'execute',
  'issue-341-worker', '34100000-0000-4000-8000-000000000902',
  '{"fixture":"issue-341"}', '2026-08-01 00:00:30+00'
);

insert into public.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata, visibility, created_at
) values (
  '34100000-0000-4000-8000-000000000202',
  '34100000-0000-4000-8000-000000000105', 'upgrade-evidence',
  'issue-341-local-only', 'redacted/evidence.json', 'application/json', 341,
  repeat('a', 64), '{"fixture":"issue-341"}', 'operator',
  '2026-08-01 00:03:00+00'
);

insert into public.lca_package_artifacts (
  id, job_id, artifact_kind, status, artifact_url, artifact_sha256,
  artifact_byte_size, artifact_format, content_type, metadata, is_pinned,
  created_at, updated_at, worker_job_id
) values (
  '34100000-0000-4000-8000-000000000302',
  '34100000-0000-4000-8000-000000000301', 'export_report', 'ready',
  'local-only://issue-341/package-report.json', repeat('b', 64), 341,
  'tidas-package-export-report:v1', 'application/json',
  '{"fixture":"issue-341"}', false,
  '2026-08-01 00:02:00+00', '2026-08-01 00:02:00+00',
  '34100000-0000-4000-8000-000000000107'
);

insert into public.lca_package_request_cache (
  id, requested_by, operation, request_key, request_payload, status, job_id,
  report_artifact_id, hit_count, last_accessed_at, created_at, updated_at,
  worker_job_id
) values (
  '34100000-0000-4000-8000-000000000303',
  '34100000-0000-4000-8000-000000000001', 'export_package',
  'issue-341-cache', '{"fixture":"issue-341"}', 'running',
  '34100000-0000-4000-8000-000000000301',
  '34100000-0000-4000-8000-000000000302', 2,
  '2026-08-01 00:02:00+00', '2026-08-01 00:00:00+00',
  '2026-08-01 00:02:00+00', '34100000-0000-4000-8000-000000000107'
);

insert into public.lca_package_export_items (
  id, job_id, table_name, dataset_id, version, is_seed, refs_done,
  created_at, updated_at, worker_job_id
)
select
  md5('issue-341-export-item-' || ordinal::text)::uuid,
  '34100000-0000-4000-8000-000000000301'::uuid,
  (array['contacts','sources','unitgroups','flowproperties','flows','processes','lifecyclemodels'])[(ordinal % 7) + 1],
  md5('issue-341-dataset-' || ordinal::text)::uuid,
  '01.00.000', ordinal <= 7, (ordinal % 2 = 0),
  '2026-08-01 00:00:00+00'::timestamptz,
  '2026-08-01 00:00:00+00'::timestamptz,
  '34100000-0000-4000-8000-000000000107'::uuid
from generate_series(1, :representative_rows) as ordinal;

insert into public.lca_release_runs (
  id, release_version, selection_manifest_hash, input_manifest_hash,
  calculation_bundle_hash, calculation_bundle_ref, profile_lock_hash,
  publish_plan_hash, publish_plan, artifact_set_hash, status,
  idempotency_key, request_hash, created_by, created_at, updated_at
) values (
  '34100000-0000-4000-8000-000000000401', '99.99.341',
  repeat('1',64), repeat('2',64), repeat('3',64), '{"fixture":"issue-341"}',
  repeat('4',64), repeat('5',64), '{"fixture":"issue-341"}', repeat('6',64),
  'prepared', 'issue-341-release', repeat('7',64),
  '34100000-0000-4000-8000-000000000001',
  '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00'
);

insert into public.lca_release_dataset_versions (
  release_run_id, dataset_type, dataset_role, dataset_uuid, dataset_version,
  source_process_uuid, source_process_version, version_significant_hash,
  semantic_hash, canonical_content_hash, artifact_ref, created_at
) values (
  '34100000-0000-4000-8000-000000000401', 'process', 'unit_process',
  '34100000-0000-4000-8000-000000000411', '01.00.000',
  '34100000-0000-4000-8000-000000000411', '01.00.000',
  repeat('8',64), repeat('9',64), repeat('c',64),
  '{"kind":"local-only","fixture":"issue-341"}',
  '2026-08-01 00:00:00+00'
);

insert into public.lca_release_artifacts (
  id, release_run_id, profile_id, artifact_format, storage_bucket, object_key,
  sha256, byte_size, media_type, closure_hash, verified_at, pinned, created_at
) values (
  '34100000-0000-4000-8000-000000000412',
  '34100000-0000-4000-8000-000000000401',
  'unit-process-full-closure.v1', 'tidas', 'issue-341-local-only',
  'redacted/release.tidas.zip', repeat('d',64), 341,
  'application/zip', repeat('e',64), '2026-08-01 00:01:00+00', false,
  '2026-08-01 00:01:00+00'
);

insert into public.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, policy_fingerprint,
  data_snapshot_token, expected_validator_scanner_fingerprint, status,
  certificate_status, result_summary, created_at, updated_at
) values (
  '34100000-0000-4000-8000-000000000501',
  '34100000-0000-4000-8000-000000000106',
  '34100000-0000-4000-8000-000000000001', 'issue-341-closure-token',
  'issue-341-closure-key', repeat('f',64), repeat('0',64), repeat('1',64),
  'issue-341-snapshot', 'scope-closure-validator-scanner.v1', 'queued',
  'pending', '{"fixture":"issue-341"}',
  '2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00'
);

commit;

analyze public.lca_package_export_items;

reset statement_timeout;
reset lock_timeout;
