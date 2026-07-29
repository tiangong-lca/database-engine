begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_table(
  'public', 'lcia_scope_closure_artifact_write_sets',
  'DB-first publication write-set registry exists'
);
select has_table(
  'public', 'lcia_scope_closure_artifact_write_set_items',
  'DB-first publication registers every possible uploaded object'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_write_set_create',
  array['uuid', 'text', 'jsonb', 'integer', 'uuid'],
  'service write-set create RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_write_set_inspect',
  array['uuid'],
  'service write-set inspect RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_write_set_finalize',
  array['uuid', 'uuid'],
  'service write-set finalize RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_write_set_fail',
  array['uuid', 'uuid', 'text'],
  'service write-set failure RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_write_set_reconcile',
  array['integer', 'integer'],
  'service stale-write-set reconcile RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_write_set_reconcile_complete',
  array['uuid', 'uuid'],
  'service stale-write-set reconcile completion RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_gc_preview',
  array['integer'],
  'service non-mutating GC preview RPC exists'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_gc_renew',
  array['uuid', 'integer'],
  'service fenced GC renewal RPC exists'
);
select ok(
  has_function_privilege(
    'service_role',
    'public.svc_lcia_scope_closure_artifact_write_set_create(uuid,text,jsonb,integer,uuid)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'public.svc_lcia_scope_closure_artifact_write_set_create(uuid,text,jsonb,integer,uuid)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.svc_lcia_scope_closure_artifact_write_set_create(uuid,text,jsonb,integer,uuid)',
    'execute'
  ),
  'publication staging mutation is service-only'
);
select ok(
  has_function_privilege(
    'service_role',
    'public.svc_lcia_scope_closure_artifact_gc_preview(integer)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'public.svc_lcia_scope_closure_artifact_gc_preview(integer)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.svc_lcia_scope_closure_artifact_gc_preview(integer)',
    'execute'
  ),
  'GC preview is service-only'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values (
  '00000000-0000-0000-0000-000000000000',
  '30820000-0000-4000-8000-000000000001',
  'authenticated', 'authenticated', 'issue-308-staging-owner@example.com',
  'x', now(), '{}', '{}', now(), now(), false, false
);
insert into public.users (id, raw_user_meta_data, contact)
values ('30820000-0000-4000-8000-000000000001', '{}', null);

insert into public.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  visibility, payload_schema_version, payload_json, status
) values
  (
    '30820000-0000-4000-8000-000000000101',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30820000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'running'
  ),
  (
    '30820000-0000-4000-8000-000000000102',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30820000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'running'
  );

insert into public.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, policy_fingerprint,
  data_snapshot_token, expected_validator_scanner_fingerprint, status,
  certificate_status
) values
  (
    '30820000-0000-4000-8000-000000000201',
    '30820000-0000-4000-8000-000000000101',
    '30820000-0000-4000-8000-000000000001',
    'staging-crash', 'staging-crash-key', repeat('a', 64),
    repeat('b', 64), repeat('c', 64), 'staging-crash-snapshot',
    'scope-closure-validator-scanner.v1', 'running', 'pending'
  ),
  (
    '30820000-0000-4000-8000-000000000202',
    '30820000-0000-4000-8000-000000000102',
    '30820000-0000-4000-8000-000000000001',
    'staging-finalize', 'staging-finalize-key', repeat('d', 64),
    repeat('e', 64), repeat('f', 64), 'staging-finalize-snapshot',
    'scope-closure-validator-scanner.v1', 'running', 'pending'
  );

select set_config('request.jwt.claim.role', 'service_role', true);

create temporary table issue_308_write_set_inputs (items jsonb);
insert into issue_308_write_set_inputs values (jsonb_build_array(
  jsonb_build_object(
    'clientKey', 'report',
    'artifactType', 'closure_report_xlsx',
    'artifactRole', 'closure_report',
    'bucket', 'closure-private',
    'objectPath', 'staging/crash/report.xlsx',
    'mediaType',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'size', 101,
    'checksumSha256', repeat('1', 64),
    'metadata', '{}'::jsonb
  ),
  jsonb_build_object(
    'clientKey', 'manifest',
    'artifactType', 'closure_complete_machine_result',
    'artifactRole', 'complete_machine_result',
    'bucket', 'closure-private',
    'objectPath', 'staging/crash/manifest.json',
    'mediaType', 'application/vnd.tiangong.scope-closure-manifest+json',
    'size', 102,
    'checksumSha256', repeat('2', 64),
    'metadata', '{}'::jsonb
  ),
  jsonb_build_object(
    'clientKey', 'partition-000001',
    'artifactType', 'closure_complete_machine_result',
    'artifactRole', 'complete_machine_result',
    'bucket', 'closure-private',
    'objectPath', 'staging/crash/part-000001.ndjson.zst',
    'mediaType', 'application/x-ndjson+zstd',
    'size', 103,
    'checksumSha256', repeat('3', 64),
    'metadata', '{}'::jsonb
  ),
  jsonb_build_object(
    'clientKey', 'bundle',
    'artifactType', 'closure_bundle',
    'artifactRole', 'closure_bundle',
    'bucket', 'closure-private',
    'objectPath', 'staging/crash/bundle.json',
    'mediaType', 'application/json',
    'size', 104,
    'checksumSha256', repeat('4', 64),
    'metadata', jsonb_build_object(
      'completeMachineResultClientKey', 'manifest'
    )
  )
));

create temporary table issue_308_crash_write_set (value jsonb);
insert into issue_308_crash_write_set
select public.svc_lcia_scope_closure_artifact_write_set_create(
  '30820000-0000-4000-8000-000000000201',
  'crash-after-two-uploads',
  items,
  1
)
from issue_308_write_set_inputs;

select is(
  jsonb_array_length(
    (select value #> '{data,items}' from issue_308_crash_write_set)
  ),
  4,
  'DB-first create registers all object locators before the first upload'
);
select is(
  (
    select value #>> '{data,artifactMap,manifest}'
    from issue_308_crash_write_set
  ),
  (
    select item->>'artifactId'
    from issue_308_crash_write_set,
      jsonb_array_elements(value #> '{data,items}') item
    where item->>'clientKey' = 'manifest'
  ),
  'create artifactMap is the authoritative clientKey-to-artifactId mapping'
);
select is(
  (
    select count(*)
    from public.lcia_scope_closure_artifact_write_set_items
    where write_set_id = (
      select (value #>> '{data,writeSetId}')::uuid
      from issue_308_crash_write_set
    )
  ),
  4::bigint,
  'simulated crash after N uploads leaves every possible object DB-visible'
);
select is(
  (
    select count(*)
    from public.worker_job_artifacts
    where id in (
      select id
      from public.lcia_scope_closure_artifact_write_set_items
      where write_set_id = (
        select (value #>> '{data,writeSetId}')::uuid
        from issue_308_crash_write_set
      )
    )
  ),
  0::bigint,
  'crashed staging leaves no partial ready artifact write-set'
);

update public.lcia_scope_closure_artifact_write_sets
set created_at = now() - interval '2 seconds',
    staging_expires_at = now() - interval '1 second'
where id = (
  select (value #>> '{data,writeSetId}')::uuid
  from issue_308_crash_write_set
);
create temporary table issue_308_reconcile_claim (value jsonb);
insert into issue_308_reconcile_claim
select public.svc_lcia_scope_closure_artifact_write_set_reconcile(1, 30);
select is(
  jsonb_array_length(
    (select value #> '{data,writeSets,0,items}'
     from issue_308_reconcile_claim)
  ),
  4,
  'fresh reconciler receives every pre-registered locator after uploader loss'
);
select is(
  public.svc_lcia_scope_closure_artifact_write_set_reconcile_complete(
    (
      select (value #>> '{data,writeSets,0,writeSetId}')::uuid
      from issue_308_reconcile_claim
    ),
    (
      select (value #>> '{data,reconcileToken}')::uuid
      from issue_308_reconcile_claim
    )
  ) #>> '{data,status}',
  'cleaned',
  'stale staging reconciliation retains a terminal cleaned audit row'
);

create temporary table issue_308_ready_write_set (value jsonb);
insert into issue_308_ready_write_set
select public.svc_lcia_scope_closure_artifact_write_set_create(
  '30820000-0000-4000-8000-000000000202',
  'finalize-all',
  replace(items::text, 'staging/crash/', 'staging/ready/')::jsonb,
  300
)
from issue_308_write_set_inputs;
select is(
  public.svc_lcia_scope_closure_artifact_write_set_finalize(
    (
      select (value #>> '{data,writeSetId}')::uuid
      from issue_308_ready_write_set
    ),
    gen_random_uuid()
  ) ->> 'code',
  'artifact_write_set_token_invalid',
  'write-set finalize rejects a stale or foreign fence token'
);
create temporary table issue_308_finalize_result (value jsonb);
insert into issue_308_finalize_result
select public.svc_lcia_scope_closure_artifact_write_set_finalize(
  (
    select (value #>> '{data,writeSetId}')::uuid
    from issue_308_ready_write_set
  ),
  (
    select (value #>> '{data,writeToken}')::uuid
    from issue_308_ready_write_set
  )
);
select is(
  (select value #>> '{data,status}' from issue_308_finalize_result),
  'ready',
  'valid write token atomically finalizes the entire write-set'
);
select is(
  (
    select count(*)
    from public.worker_job_artifacts
    where id in (
      select id
      from public.lcia_scope_closure_artifact_write_set_items
      where write_set_id = (
        select (value #>> '{data,writeSetId}')::uuid
        from issue_308_ready_write_set
      )
    )
  ),
  4::bigint,
  'atomic finalize publishes all registered artifacts together'
);
select ok(
  (
    select report_artifact_id is not null
      and complete_machine_result_artifact_id is not null
      and closure_bundle_artifact_id is not null
    from public.lcia_scope_closure_checks
    where id = '30820000-0000-4000-8000-000000000202'
  ),
  'atomic finalize binds report, manifest, and bundle in the same transaction'
);
select is(
  (
    select artifact.metadata->>'completeMachineResultArtifactId'
    from public.worker_job_artifacts artifact
    join public.lcia_scope_closure_checks closure_check
      on closure_check.closure_bundle_artifact_id = artifact.id
    where closure_check.id = '30820000-0000-4000-8000-000000000202'
  ),
  (
    select value #>> '{data,artifactMap,manifest}'
    from issue_308_finalize_result
  ),
  'finalize atomically resolves the bundle manifest client key to its artifact UUID'
);
select is(
  (
    select artifact.metadata ? 'completeMachineResultClientKey'
    from public.worker_job_artifacts artifact
    join public.lcia_scope_closure_checks closure_check
      on closure_check.closure_bundle_artifact_id = artifact.id
    where closure_check.id = '30820000-0000-4000-8000-000000000202'
  ),
  false,
  'final closure bundle metadata does not retain the staging client key'
);

insert into public.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  visibility, payload_schema_version, payload_json, status
) values (
  '30820000-0000-4000-8000-000000000103',
  'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
  '30820000-0000-4000-8000-000000000001', 'operator',
  'lcia.scope_closure_check.request.v1', '{}', 'running'
);
update public.lcia_scope_closure_checks
set status = 'passed',
    scan_completeness = 'complete'
where id = '30820000-0000-4000-8000-000000000202';
insert into public.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, policy_fingerprint,
  data_snapshot_token, expected_validator_scanner_fingerprint, status,
  certificate_status
) values (
  '30820000-0000-4000-8000-000000000203',
  '30820000-0000-4000-8000-000000000103',
  '30820000-0000-4000-8000-000000000001',
  'staging-reused', 'staging-reused-key', repeat('7', 64),
  repeat('e', 64), repeat('f', 64), 'staging-finalize-snapshot',
  'scope-closure-validator-scanner.v1', 'running', 'pending'
);

create temporary table issue_308_reused_write_set (value jsonb);
insert into issue_308_reused_write_set
select public.svc_lcia_scope_closure_artifact_write_set_create(
  '30820000-0000-4000-8000-000000000203',
  'finalize-reused-report-only',
  jsonb_build_array(
    jsonb_build_object(
      'clientKey', 'report',
      'artifactType', 'closure_report_xlsx',
      'artifactRole', 'closure_report',
      'bucket', 'closure-private',
      'objectPath', 'staging/reused/report.xlsx',
      'mediaType',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'size', 105,
      'checksumSha256', repeat('5', 64),
      'metadata', '{}'::jsonb
    )
  ),
  300,
  '30820000-0000-4000-8000-000000000202'
);
select is(
  (select value #>> '{data,publicationMode}' from issue_308_reused_write_set),
  'reused',
  'DB derives the report-only reused publication mode from its source binding'
);
select is(
  jsonb_array_length(
    (select value #> '{data,items}' from issue_308_reused_write_set)
  ),
  1,
  'reused publication registers only the newly generated XLSX report'
);
select ok(
  (
    select target.reused_from_check_id = source.id
      and target.complete_machine_result_artifact_id =
        source.complete_machine_result_artifact_id
      and target.closure_bundle_artifact_id =
        source.closure_bundle_artifact_id
    from public.lcia_scope_closure_checks target
    join public.lcia_scope_closure_checks source
      on source.id = '30820000-0000-4000-8000-000000000202'
    where target.id = '30820000-0000-4000-8000-000000000203'
  ),
  'reused create atomically freezes the source manifest and bundle lineage'
);
select is(
  public.svc_lcia_scope_closure_artifact_write_set_finalize(
    (
      select (value #>> '{data,writeSetId}')::uuid
      from issue_308_reused_write_set
    ),
    (
      select (value #>> '{data,writeToken}')::uuid
      from issue_308_reused_write_set
    )
  ) #>> '{data,status}',
  'ready',
  'reused report-only write-set finalizes successfully'
);
select ok(
  (
    select target.report_artifact_id is not null
      and target.report_artifact_id is distinct from source.report_artifact_id
      and target.complete_machine_result_artifact_id =
        source.complete_machine_result_artifact_id
      and target.closure_bundle_artifact_id =
        source.closure_bundle_artifact_id
    from public.lcia_scope_closure_checks target
    join public.lcia_scope_closure_checks source
      on source.id = '30820000-0000-4000-8000-000000000202'
    where target.id = '30820000-0000-4000-8000-000000000203'
  ),
  'reused finalize updates only the current report and preserves source evidence'
);

insert into public.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata, created_at
) values (
  '30820000-0000-4000-8000-000000000301',
  '30820000-0000-4000-8000-000000000101',
  'closure_report_xlsx', 'closure-private', 'gc/expired.xlsx',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  301, repeat('a', 64), '{}', now() - interval '8 days'
);

create temporary table issue_308_gc_before_preview as
select
  id,
  lifecycle_state,
  gc_claim_token,
  gc_claimed_at,
  gc_claim_expires_at,
  gc_failure_count,
  gc_last_error
from public.worker_job_artifacts
where id = '30820000-0000-4000-8000-000000000301';
create temporary table issue_308_gc_preview (value jsonb);
insert into issue_308_gc_preview
select public.svc_lcia_scope_closure_artifact_gc_preview(1);
select is(
  (
    select row_to_json(before_row)::jsonb
    from issue_308_gc_before_preview before_row
  ),
  (
    select row_to_json(after_row)::jsonb
    from (
      select
        id,
        lifecycle_state,
        gc_claim_token,
        gc_claimed_at,
        gc_claim_expires_at,
        gc_failure_count,
        gc_last_error
      from public.worker_job_artifacts
      where id = '30820000-0000-4000-8000-000000000301'
    ) after_row
  ),
  'GC preview leaves candidate state, token, and timestamps byte-for-byte unchanged'
);

create temporary table issue_308_gc_claim (value jsonb);
insert into issue_308_gc_claim
select public.svc_lcia_scope_closure_artifact_gc_claim(1, 2);
select is(
  (select value #>> '{data,items,0,artifactId}' from issue_308_gc_preview),
  (select value #>> '{data,items,0,artifactId}' from issue_308_gc_claim),
  'preview-to-immediate-execute claims the same first ordered candidate'
);
select is(
  jsonb_array_length(
    public.svc_lcia_scope_closure_artifact_gc_claim(1, 2)
      #> '{data,items}'
  ),
  0,
  'a second claimant cannot take an actively leased artifact'
);

create temporary table issue_308_gc_renewals (
  ordinal integer,
  value jsonb
);
insert into issue_308_gc_renewals
select 1, public.svc_lcia_scope_closure_artifact_gc_renew(
  (select (value #>> '{data,claimToken}')::uuid from issue_308_gc_claim),
  3
);
select pg_sleep(0.05);
insert into issue_308_gc_renewals
select 2, public.svc_lcia_scope_closure_artifact_gc_renew(
  (select (value #>> '{data,claimToken}')::uuid from issue_308_gc_claim),
  4
);
select ok(
  (
    select (second.value #>> '{data,leaseExpiresAt}')::timestamptz
         > (first.value #>> '{data,leaseExpiresAt}')::timestamptz
    from issue_308_gc_renewals first
    cross join issue_308_gc_renewals second
    where first.ordinal = 1 and second.ordinal = 2
  ),
  'delayed multi-round heartbeat monotonically extends the current lease'
);
select is(
  jsonb_array_length(
    public.svc_lcia_scope_closure_artifact_gc_claim(1, 2)
      #> '{data,items}'
  ),
  0,
  'renewed lease continues to exclude a second claimant'
);

update public.worker_job_artifacts
set gc_claim_expires_at = now() - interval '1 second'
where id = '30820000-0000-4000-8000-000000000301';
create temporary table issue_308_gc_handoff (value jsonb);
insert into issue_308_gc_handoff
select public.svc_lcia_scope_closure_artifact_gc_claim(1, 2);
select isnt(
  (select value #>> '{data,claimToken}' from issue_308_gc_handoff),
  (select value #>> '{data,claimToken}' from issue_308_gc_claim),
  'expired lease hands the artifact to a fresh fenced token'
);
select is(
  public.svc_lcia_scope_closure_artifact_gc_renew(
    (select (value #>> '{data,claimToken}')::uuid from issue_308_gc_claim),
    3
  ) ->> 'code',
  'gc_claim_invalid',
  'old token cannot renew after expiry handoff'
);
select is(
  public.svc_lcia_scope_closure_artifact_gc_complete(
    '30820000-0000-4000-8000-000000000301',
    (select (value #>> '{data,claimToken}')::uuid from issue_308_gc_claim),
    false,
    1
  ) ->> 'code',
  'gc_claim_invalid',
  'old token cannot complete after expiry handoff'
);

select * from finish();
rollback;
