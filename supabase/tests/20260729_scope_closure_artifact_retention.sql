begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_column(
  'public', 'worker_job_artifacts', 'artifact_role',
  'scope-closure artifacts have an authoritative role'
);
select has_column(
  'public', 'worker_job_artifacts', 'lifecycle_state',
  'scope-closure artifacts have an authoritative lifecycle'
);
select has_column(
  'public', 'worker_job_artifacts', 'gc_cleanup_state',
  'scope-closure GC has a DB-owned resumable cleanup state'
);
select has_column(
  'public', 'lcia_scope_closure_checks', 'valid_until',
  'closure certificates have an evidence-bounded validity deadline'
);
select has_table(
  'public', 'lcia_scope_closure_retention_summaries',
  'detail GC retains a compact audit summary'
);
select has_function(
  'public', 'get_lcia_scope_closure_report_download',
  array['uuid', 'text'],
  'actor-bound closure artifact projection has a strict role selector'
);
select has_function(
  'public', 'get_lcia_scope_closure_report_download',
  array['uuid'],
  'temporary selector-less XLSX compatibility overload remains during rollout'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_gc_claim',
  array['integer', 'integer'],
  'service GC claim RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_gc_complete',
  array['uuid', 'uuid', 'boolean', 'integer'],
  'service GC completion RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_gc_fail',
  array['uuid', 'uuid', 'text'],
  'service GC failure RPC exists'
);
select ok(
  has_function_privilege(
    'service_role',
    'public.svc_lcia_scope_closure_artifact_gc_claim(integer,integer)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'public.svc_lcia_scope_closure_artifact_gc_claim(integer,integer)',
    'execute'
  ),
  'GC coordination is service-only'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values
  (
    '00000000-0000-0000-0000-000000000000',
    '30800000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'issue-308-owner@example.com', 'x',
    now(), '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '30800000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'issue-308-other@example.com', 'x',
    now(), '{}', '{}', now(), now(), false, false
  );
insert into public.users (id, raw_user_meta_data, contact) values
  ('30800000-0000-4000-8000-000000000001', '{}', null),
  ('30800000-0000-4000-8000-000000000002', '{}', null);
insert into public.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System"}', 0, false
) on conflict (id) do nothing;
insert into public.roles (user_id, team_id, role) values
  (
    '30800000-0000-4000-8000-000000000001',
    '00000000-0000-0000-0000-000000000000',
    'data_product_manager'
  ),
  (
    '30800000-0000-4000-8000-000000000002',
    '00000000-0000-0000-0000-000000000000',
    'data_product_manager'
  );

insert into public.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  visibility, payload_schema_version, payload_json
) values
  (
    '30800000-0000-4000-8000-000000000101',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30800000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}'
  ),
  (
    '30800000-0000-4000-8000-000000000102',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30800000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}'
  );

insert into public.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata, created_at
) values
  (
    '30800000-0000-4000-8000-000000000201',
    '30800000-0000-4000-8000-000000000101',
    'closure_report_xlsx', 'private-evidence', 'checks/308/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    128, repeat('a', 64), '{}', now()
  ),
  (
    '30800000-0000-4000-8000-000000000202',
    '30800000-0000-4000-8000-000000000101',
    'closure_bundle', 'private-evidence', 'checks/308/bundle.json',
    'application/json', 256, repeat('b', 64),
    '{"closureCheckId":"30800000-0000-4000-8000-000000000301","completeMachineResultArtifactId":"30800000-0000-4000-8000-000000000206"}',
    now()
  ),
  (
    '30800000-0000-4000-8000-000000000203',
    '30800000-0000-4000-8000-000000000102',
    'closure_report_xlsx', 'private-evidence', 'checks/308/expired.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    64, repeat('c', 64), '{}', now() - interval '8 days'
  ),
  (
    '30800000-0000-4000-8000-000000000204',
    '30800000-0000-4000-8000-000000000102',
    'closure_complete_machine_result', 'private-evidence',
    'checks/308/machine-result-1.json', 'application/json',
    65, repeat('d', 64), '{}', now() - interval '7 days 2 hours'
  ),
  (
    '30800000-0000-4000-8000-000000000205',
    '30800000-0000-4000-8000-000000000102',
    'closure_complete_machine_result', 'private-evidence',
    'checks/308/machine-result-2.json', 'application/json',
    66, repeat('e', 64), '{}', now() - interval '7 days 1 hour'
  ),
  (
    '30800000-0000-4000-8000-000000000206',
    '30800000-0000-4000-8000-000000000101',
    'closure_complete_machine_result', 'private-evidence',
    'checks/308/machine-result.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    512, repeat('f', 64), '{}', now()
  );

select is(
  (
    select expires_at
    from public.worker_job_artifacts
    where id = '30800000-0000-4000-8000-000000000201'
  ),
  (
    select created_at + interval '7 days'
    from public.worker_job_artifacts
    where id = '30800000-0000-4000-8000-000000000201'
  ),
  'trusted insertion assigns the exact seven-day expiry'
);
select is(
  (
    select artifact_role || ':' || lifecycle_state
    from public.worker_job_artifacts
    where id = '30800000-0000-4000-8000-000000000203'
  ),
  'closure_report:expired',
  'an already elapsed retention deadline starts expired'
);
select throws_ok(
  $$
    update public.worker_job_artifacts
    set expires_at = expires_at + interval '1 day'
    where id = '30800000-0000-4000-8000-000000000201'
  $$,
  '23514',
  'scope_closure_artifact_identity_or_expiry_is_immutable',
  'callers cannot extend the trusted evidence deadline'
);
select throws_ok(
  $$
    update public.worker_job_artifacts
    set lifecycle_state = 'ready'
    where id = '30800000-0000-4000-8000-000000000203'
  $$,
  '23514',
  'scope_closure_artifact_cannot_return_to_ready',
  'expired artifacts cannot transition back to ready'
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
  snapshot_artifact_id, snapshot_index_sha256, snapshot_build_contract_hash
) values (
  '30800000-0000-4000-8000-000000000301',
  '30800000-0000-4000-8000-000000000101',
  '30800000-0000-4000-8000-000000000001',
  'issue-308-valid', 'issue-308-valid-key', repeat('1', 64),
  repeat('2', 64), repeat('3', 64), repeat('4', 64), 'snapshot-token',
  'scope-closure-validator-scanner.v1', 'passed', 'complete', 'valid',
  repeat('5', 64), '30800000-0000-4000-8000-000000000201',
  '{"issueCount":1}', now(), '{}', '{}',
  'lcia.scope-closure-certificate.v2', 'source-fingerprint', repeat('6', 64),
  repeat('b', 64), '30800000-0000-4000-8000-000000000501',
  repeat('7', 64), repeat('8', 64), repeat('9', 64),
  '30800000-0000-4000-8000-000000000202',
  '30800000-0000-4000-8000-000000000502',
  repeat('a', 64), repeat('b', 64)
);

select is(
  (
    select valid_until
    from public.lcia_scope_closure_checks
    where id = '30800000-0000-4000-8000-000000000301'
  ),
  (
    select least(
      report.expires_at,
      machine_result.expires_at,
      bundle.expires_at
    )
    from public.worker_job_artifacts report
    cross join public.worker_job_artifacts machine_result
    cross join public.worker_job_artifacts bundle
    where report.id = '30800000-0000-4000-8000-000000000201'
      and machine_result.id = '30800000-0000-4000-8000-000000000206'
      and bundle.id = '30800000-0000-4000-8000-000000000202'
  ),
  'certificate valid_until is bounded by required evidence expiry'
);
update public.lcia_scope_closure_checks
set valid_until = valid_until + interval '1 day'
where id = '30800000-0000-4000-8000-000000000301';
select is(
  (
    select valid_until
    from public.lcia_scope_closure_checks
    where id = '30800000-0000-4000-8000-000000000301'
  ),
  (
    select least(
      report.expires_at,
      machine_result.expires_at,
      bundle.expires_at
    )
    from public.worker_job_artifacts report
    cross join public.worker_job_artifacts machine_result
    cross join public.worker_job_artifacts bundle
    where report.id = '30800000-0000-4000-8000-000000000201'
      and machine_result.id = '30800000-0000-4000-8000-000000000206'
      and bundle.id = '30800000-0000-4000-8000-000000000202'
  ),
  'certificate validity cannot be extended beyond its evidence'
);

insert into public.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, policy_fingerprint,
  data_snapshot_token, expected_validator_scanner_fingerprint, status,
  scan_completeness, certificate_status, report_artifact_id, result_summary,
  finished_at
) values (
  '30800000-0000-4000-8000-000000000302',
  '30800000-0000-4000-8000-000000000102',
  '30800000-0000-4000-8000-000000000001',
  'issue-308-expired', 'issue-308-expired-key', repeat('c', 64),
  repeat('d', 64), repeat('e', 64), 'expired-snapshot-token',
  'scope-closure-validator-scanner.v1', 'blocked', 'complete',
  'unavailable', '30800000-0000-4000-8000-000000000203',
  '{"issueCount":1,"occurrenceCount":1,"affectedRootCount":1}', now()
);
insert into public.lcia_scope_closure_issues (
  id, closure_check_id, issue_key, severity, blocking, issue_code, message,
  occurrence_count, affected_root_count, details
) values (
  '30800000-0000-4000-8000-000000000401',
  '30800000-0000-4000-8000-000000000302',
  'expired-detail', 'blocker', true, 'missing_reference',
  'detail eligible for retention cleanup', 1, 1, '{"large":"detail"}'
);
insert into public.lcia_scope_closure_issue_occurrences (
  id, closure_issue_id, occurrence_key, details
) values
  (
    '30800000-0000-4000-8000-000000000402',
    '30800000-0000-4000-8000-000000000401',
    'occurrence-1', '{"path":"large"}'
  ),
  (
    '30800000-0000-4000-8000-000000000404',
    '30800000-0000-4000-8000-000000000401',
    'occurrence-2', '{"path":"also-large"}'
  );
insert into public.lcia_scope_closure_issue_roots (
  closure_issue_id, root_dataset_type, root_dataset_id,
  root_dataset_version, impact_role, witness_path
) values (
  '30800000-0000-4000-8000-000000000401',
  'process', '30800000-0000-4000-8000-000000000403',
  '01.00.000', 'root', '[]'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30800000-0000-4000-8000-000000000001',
  true
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_report_xlsx'
  ) #>> '{data,artifactRole}',
  'closure_report_xlsx',
  'owner can select the public XLSX role'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_report_xlsx'
  ) #>> '{data,artifactId}',
  '30800000-0000-4000-8000-000000000201',
  'XLSX selector resolves only the linked report artifact'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_report_xlsx'
  ) #>> '{data,artifactState}',
  'ready',
  'ready XLSX projection publishes the shared artifact state'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_report_xlsx'
    ) -> 'data'
  ) - array[
    'artifactId',
    'artifactRole',
    'artifactState',
    'filename',
    'format',
    'mediaType',
    'size',
    'checksumSha256',
    'artifactExpiresAt',
    'bucket',
    'objectPath'
  ]::text[],
  '{}'::jsonb,
  'public descriptor contains no fields outside the shared DTO'
);
select is(
  (
    select count(*)
    from jsonb_object_keys(
      public.get_lcia_scope_closure_report_download(
        '30800000-0000-4000-8000-000000000301',
        'closure_report_xlsx'
      ) -> 'data'
    )
  ),
  11::bigint,
  'public descriptor contains every required shared DTO field'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_report_xlsx'
  ) #>> '{data,filename}',
  'scope-closure-30800000-0000-4000-8000-000000000301.xlsx',
  'XLSX selector returns its semantic filename'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_report_xlsx'
    ) #>> '{data,format}'
  ) || ':' || (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_report_xlsx'
    ) #>> '{data,mediaType}'
  ),
  'xlsx:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'XLSX selector returns the exact public format/media pair'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_report_xlsx'
    ) #>> '{data,bucket}'
  ) || ':' || (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_report_xlsx'
    ) #>> '{data,objectPath}'
  ),
  'private-evidence:checks/308/report.xlsx',
  'XLSX locator is projected from the linked artifact without substitution'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_issue_manifest'
  ) #>> '{data,artifactId}',
  '30800000-0000-4000-8000-000000000206',
  'manifest selector resolves only the linked complete-machine artifact'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #>> '{data,artifactRole}'
  ) || ':' || (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #>> '{data,filename}'
  ),
  'closure_issue_manifest:scope-closure-30800000-0000-4000-8000-000000000301-manifest.json',
  'manifest selector returns its public role and semantic filename'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #>> '{data,format}'
  ) || ':' || (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #>> '{data,mediaType}'
  ),
  'json:application/vnd.tiangong.scope-closure-manifest+json',
  'manifest selector returns the exact public format/media pair'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #>> '{data,bucket}'
  ) || ':' || (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #>> '{data,objectPath}'
  ),
  'private-evidence:checks/308/machine-result.json',
  'manifest locator is projected from the linked artifact without substitution'
);
select ok(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000301',
      'closure_issue_manifest'
    ) #> '{data,artifactExpiresAt}'
  ) is not null,
  'public manifest descriptor includes artifactExpiresAt'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_bundle'
  ) ->> 'code',
  'closure_artifact_role_invalid',
  'owner receives a stable error for an unsupported selector'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000302',
    'closure_issue_manifest'
  ) ->> 'code',
  'closure_report_unavailable',
  'owner cannot receive a descriptor for an unready or unlinked artifact'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000302',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_report_expired',
  'owner receives stable expired semantics'
);
select is(
  (
    public.get_lcia_scope_closure_report_download(
      '30800000-0000-4000-8000-000000000302',
      'closure_report_xlsx'
    ) ->> 'status'
  )::integer,
  410,
  'expired owner download uses HTTP 410 semantics'
);
select set_config(
  'request.jwt.claim.sub',
  '30800000-0000-4000-8000-000000000002',
  true
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-user ready artifact remains opaque'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000302',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-user expired artifact remains opaque'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_issue_manifest'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-user manifest selection remains opaque'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000301',
    'closure_bundle'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-user invalid selectors do not reveal check existence'
);
reset role;

update public.worker_job_artifacts
set lifecycle_state = 'expired'
where id in (
  '30800000-0000-4000-8000-000000000201',
  '30800000-0000-4000-8000-000000000202'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30800000-0000-4000-8000-000000000001',
  true
);
select is(
  public.cmd_lcia_result_build_request_v2(
    'expired build', '[]', 'subset', null, '[]', 'issue-308-build',
    '30800000-0000-4000-8000-000000000301',
    repeat('2', 64), repeat('4', 64), '{}'
  ) ->> 'code',
  'closure_certificate_expired',
  'build admission rejects expired certificate evidence'
);
reset role;

select set_config('request.jwt.claim.role', 'service_role', true);
create temporary table issue_308_gc_claims (
  claim_token uuid,
  artifact_id uuid,
  value jsonb
);
with response as (
  select public.svc_lcia_scope_closure_artifact_gc_claim(1, 300) value
)
insert into issue_308_gc_claims
select
  (value #>> '{data,claimToken}')::uuid,
  (value #>> '{data,items,0,artifactId}')::uuid,
  value
from response;

select is(
  (select artifact_id from issue_308_gc_claims),
  '30800000-0000-4000-8000-000000000203'::uuid,
  'GC claims the oldest eligible artifact'
);
select is(
  (select value #>> '{data,items,0,gcPhase}' from issue_308_gc_claims),
  'object_delete',
  'an expired object claim identifies the object-delete phase'
);
select is(
  (
    select value #>> '{data,items,0,objectDeleteRequired}'
    from issue_308_gc_claims
  ),
  'true',
  'initial claim requires exactly one Storage object deletion'
);

create temporary table issue_308_first_completion (value jsonb);
insert into issue_308_first_completion
select public.svc_lcia_scope_closure_artifact_gc_complete(
  (select artifact_id from issue_308_gc_claims),
  (select claim_token from issue_308_gc_claims),
  true,
  1
);
select is(
  (select value #>> '{data,state}' from issue_308_first_completion),
  'deleted',
  'missing-object completion tombstones metadata successfully'
);
select ok(
  (
    select (value #>> '{data,detailsRemaining}')::bigint > 0
      and (value #>> '{data,cleanupPending}')::boolean
      and (value #>> '{data,objectDeleteRequired}')::boolean
    from issue_308_first_completion
  ),
  'bounded first completion reports resumable detail cleanup after object deletion'
);
select ok(
  (
    select storage_bucket is null
      and storage_path is null
      and checksum_sha256 = repeat('c', 64)
      and byte_size = 64
    from public.worker_job_artifacts
    where id = '30800000-0000-4000-8000-000000000203'
  ),
  'GC removes object location but retains compact artifact evidence'
);
select is(
  (
    select lifecycle_state || ':' || gc_cleanup_state
    from public.worker_job_artifacts
    where id = '30800000-0000-4000-8000-000000000203'
  ),
  'deleted:pending',
  'partial completion persists an explicit post-tombstone cleanup candidate'
);

-- Simulate the first Worker process exiting without retaining its token.
update public.worker_job_artifacts
set gc_claim_expires_at = now() - interval '1 second'
where id = '30800000-0000-4000-8000-000000000203';

create temporary table issue_308_recovery_claim (
  claim_token uuid,
  artifact_id uuid,
  value jsonb
);
with response as (
  select public.svc_lcia_scope_closure_artifact_gc_claim(1, 300) value
)
insert into issue_308_recovery_claim
select
  (value #>> '{data,claimToken}')::uuid,
  (value #>> '{data,items,0,artifactId}')::uuid,
  value
from response;
select is(
  (select artifact_id from issue_308_recovery_claim),
  '30800000-0000-4000-8000-000000000203'::uuid,
  'a fresh process reclaims the partial post-tombstone cleanup candidate'
);
select isnt(
  (select claim_token from issue_308_recovery_claim),
  (select claim_token from issue_308_gc_claims),
  'fresh-process recovery receives a newly fenced claim token'
);
select ok(
  (
    select value #>> '{data,items,0,gcPhase}' = 'detail_cleanup'
      and not (
        value #>> '{data,items,0,objectDeleteRequired}'
      )::boolean
      and (value #> '{data,items,0,bucket}') = 'null'::jsonb
      and (value #> '{data,items,0,objectPath}') = 'null'::jsonb
    from issue_308_recovery_claim
  ),
  'recovery claim explicitly forbids a second object deletion and exposes no locator'
);

create temporary table issue_308_final_completion (value jsonb);
insert into issue_308_final_completion
select public.svc_lcia_scope_closure_artifact_gc_complete(
  (select artifact_id from issue_308_recovery_claim),
  (select claim_token from issue_308_recovery_claim),
  false,
  1
);
select ok(
  (
    select value #>> '{data,detailsRemaining}' = '0'
      and not (value #>> '{data,cleanupPending}')::boolean
      and (value #>> '{data,cleanupComplete}')::boolean
      and not (value #>> '{data,objectDeleteRequired}')::boolean
    from issue_308_final_completion
  ),
  'fresh-process completion finishes bounded detail cleanup without object deletion'
);
select is(
  public.svc_lcia_scope_closure_artifact_gc_complete(
    (select artifact_id from issue_308_recovery_claim),
    (select claim_token from issue_308_recovery_claim),
    false,
    1
  ) ->> 'reused',
  'true',
  'repeated final completion with the recovery token is idempotent'
);
select is(
  (
    select lifecycle_state || ':' || gc_cleanup_state
    from public.worker_job_artifacts
    where id = '30800000-0000-4000-8000-000000000203'
  ),
  'deleted:complete',
  'final completion leaves compact audit residue in a terminal cleanup state'
);
set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30800000-0000-4000-8000-000000000001',
  true
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30800000-0000-4000-8000-000000000302',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_report_unavailable',
  'a deleted artifact is unavailable rather than returned as a descriptor'
);
reset role;
select set_config('request.jwt.claim.role', 'service_role', true);
select is(
  (
    select issue_count || ':' || occurrence_count || ':' || affected_root_count
    from public.lcia_scope_closure_retention_summaries
    where closure_check_id = '30800000-0000-4000-8000-000000000302'
  ),
  '1:2:1',
  'retention summary preserves detail counts'
);
select ok(
  (
    select issue_content_hash ~ '^[a-f0-9]{64}$'
      and compact_result_summary->>'issueCount' = '1'
    from public.lcia_scope_closure_retention_summaries
    where closure_check_id = '30800000-0000-4000-8000-000000000302'
  ),
  'retention summary preserves a content hash and compact run summary'
);
select is(
  (
    select count(*)
    from public.lcia_scope_closure_issues
    where closure_check_id = '30800000-0000-4000-8000-000000000302'
  ),
  0::bigint,
  'bounded completion purges high-cardinality issue detail rows'
);

create temporary table issue_308_concurrent_claims (
  ordinal integer,
  claim_token uuid,
  artifact_id uuid
);
with response as (
  select public.svc_lcia_scope_closure_artifact_gc_claim(1, 300) value
)
insert into issue_308_concurrent_claims
select
  1,
  (value #>> '{data,claimToken}')::uuid,
  (value #>> '{data,items,0,artifactId}')::uuid
from response;
with response as (
  select public.svc_lcia_scope_closure_artifact_gc_claim(1, 300) value
)
insert into issue_308_concurrent_claims
select
  2,
  (value #>> '{data,claimToken}')::uuid,
  (value #>> '{data,items,0,artifactId}')::uuid
from response;
select isnt(
  (select artifact_id from issue_308_concurrent_claims where ordinal = 1),
  (select artifact_id from issue_308_concurrent_claims where ordinal = 2),
  'independent active claims cannot select the same artifact'
);
select is(
  public.svc_lcia_scope_closure_artifact_gc_fail(
    (select artifact_id from issue_308_concurrent_claims where ordinal = 1),
    (select claim_token from issue_308_concurrent_claims where ordinal = 1),
    'temporary object-store failure'
  ) #>> '{data,failureCount}',
  '1',
  'GC failure releases the lease and records the retry count'
);
with response as (
  select public.svc_lcia_scope_closure_artifact_gc_claim(1, 300) value
)
select is(
  (value #>> '{data,items,0,artifactId}')::uuid,
  (select artifact_id from issue_308_concurrent_claims where ordinal = 1),
  'a failed artifact is retryable under a fresh claim'
)
from response;

select * from finish();
rollback;
