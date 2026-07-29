begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_function(
  'public', 'get_lcia_scope_closure_check', array['uuid'],
  'owner closure read RPC exists'
);
select has_function(
  'public', 'get_lcia_scope_closure_report_download', array['uuid', 'text'],
  'strict role-selecting download RPC exists'
);
select has_function(
  'public', 'get_lcia_scope_closure_report_download', array['uuid'],
  'temporary selector-less download compatibility overload exists'
);
select ok(
  has_function_privilege(
    'authenticated',
    'public.get_lcia_scope_closure_report_download(uuid)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.get_lcia_scope_closure_report_download(uuid)',
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'public.get_lcia_scope_closure_report_download(uuid)',
    'execute'
  ),
  'legacy download overload is authenticated-only'
);
select ok(
  has_function_privilege(
    'authenticated',
    'public.get_lcia_scope_closure_report_download(uuid,text)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.get_lcia_scope_closure_report_download(uuid,text)',
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'public.get_lcia_scope_closure_report_download(uuid,text)',
    'execute'
  ),
  'strict download RPC is authenticated-only'
);
select ok(
  has_function_privilege(
    'authenticated',
    'public.get_lcia_scope_closure_check(uuid)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.get_lcia_scope_closure_check(uuid)',
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'public.get_lcia_scope_closure_check(uuid)',
    'execute'
  ),
  'owner read projection is authenticated-only'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values
  (
    '00000000-0000-0000-0000-000000000000',
    '30810000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'issue-308-projection-owner@example.com',
    'x', now(), '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '30810000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'issue-308-projection-other@example.com',
    'x', now(), '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '30810000-0000-4000-8000-000000000003',
    'authenticated', 'authenticated', 'issue-308-projection-member@example.com',
    'x', now(), '{}', '{}', now(), now(), false, false
  );

insert into public.users (id, raw_user_meta_data, contact) values
  ('30810000-0000-4000-8000-000000000001', '{}', null),
  ('30810000-0000-4000-8000-000000000002', '{}', null),
  ('30810000-0000-4000-8000-000000000003', '{}', null);

insert into public.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System"}', 0, false
) on conflict (id) do nothing;

insert into public.roles (user_id, team_id, role) values
  (
    '30810000-0000-4000-8000-000000000001',
    '00000000-0000-0000-0000-000000000000',
    'data_product_manager'
  ),
  (
    '30810000-0000-4000-8000-000000000002',
    '00000000-0000-0000-0000-000000000000',
    'data_product_manager'
  );

insert into public.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  visibility, payload_schema_version, payload_json, status
) values
  (
    '30810000-0000-4000-8000-000000000101',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30810000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'queued'
  ),
  (
    '30810000-0000-4000-8000-000000000102',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30810000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'completed'
  ),
  (
    '30810000-0000-4000-8000-000000000103',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30810000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'completed'
  ),
  (
    '30810000-0000-4000-8000-000000000104',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30810000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'completed'
  ),
  (
    '30810000-0000-4000-8000-000000000105',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30810000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'completed'
  ),
  (
    '30810000-0000-4000-8000-000000000106',
    'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
    '30810000-0000-4000-8000-000000000001', 'operator',
    'lcia.scope_closure_check.request.v1', '{}', 'completed'
  );

insert into public.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata, created_at
) values
  (
    '30810000-0000-4000-8000-000000000201',
    '30810000-0000-4000-8000-000000000102',
    'closure_report_xlsx', 'projection-test', 'ready/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    101, repeat('a', 64), '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000202',
    '30810000-0000-4000-8000-000000000102',
    'closure_complete_machine_result', 'projection-test', 'ready/manifest.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    102, repeat('b', 64), '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000203',
    '30810000-0000-4000-8000-000000000103',
    'closure_report_xlsx', 'projection-test', 'expired/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    103, repeat('c', 64), '{}', now() - interval '8 days'
  ),
  (
    '30810000-0000-4000-8000-000000000204',
    '30810000-0000-4000-8000-000000000103',
    'closure_complete_machine_result', 'projection-test',
    'expired/manifest.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    104, repeat('d', 64), '{}', now() - interval '8 days'
  ),
  (
    '30810000-0000-4000-8000-000000000205',
    '30810000-0000-4000-8000-000000000104',
    'closure_report_xlsx', 'projection-test', 'deleted/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    105, repeat('e', 64), '{}', now() - interval '8 days'
  ),
  (
    '30810000-0000-4000-8000-000000000206',
    '30810000-0000-4000-8000-000000000104',
    'closure_complete_machine_result', 'projection-test',
    'deleted/manifest.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    106, repeat('f', 64), '{}', now() - interval '8 days'
  ),
  (
    '30810000-0000-4000-8000-000000000207',
    '30810000-0000-4000-8000-000000000105',
    'closure_report_xlsx', 'projection-test', 'failed/report.xlsx',
    'application/octet-stream',
    107, repeat('1', 64), '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000208',
    '30810000-0000-4000-8000-000000000105',
    'closure_complete_machine_result', 'projection-test',
    'failed/manifest.json',
    'application/json',
    108, repeat('2', 64), '{}', now()
  );

update public.worker_job_artifacts
set lifecycle_state = 'deleted',
    gc_cleanup_state = 'complete'
where id in (
  '30810000-0000-4000-8000-000000000205',
  '30810000-0000-4000-8000-000000000206'
);

insert into public.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, policy_fingerprint,
  data_snapshot_token, expected_validator_scanner_fingerprint, status,
  scan_completeness, certificate_status, report_artifact_id,
  complete_machine_result_artifact_id, result_summary, finished_at
) values
  (
    '30810000-0000-4000-8000-000000000301',
    '30810000-0000-4000-8000-000000000101',
    '30810000-0000-4000-8000-000000000001',
    'projection-pending', 'projection-pending-key', repeat('a', 64),
    repeat('b', 64), repeat('c', 64), 'projection-pending-snapshot',
    'scope-closure-validator-scanner.v1', 'queued', null, 'pending',
    null, null, '{}', null
  ),
  (
    '30810000-0000-4000-8000-000000000302',
    '30810000-0000-4000-8000-000000000102',
    '30810000-0000-4000-8000-000000000001',
    'projection-ready', 'projection-ready-key', repeat('d', 64),
    repeat('e', 64), repeat('f', 64), 'projection-ready-snapshot',
    'scope-closure-validator-scanner.v1', 'passed', 'complete',
    'unavailable', '30810000-0000-4000-8000-000000000201',
    '30810000-0000-4000-8000-000000000202', '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000303',
    '30810000-0000-4000-8000-000000000103',
    '30810000-0000-4000-8000-000000000001',
    'projection-expired', 'projection-expired-key', repeat('1', 64),
    repeat('2', 64), repeat('3', 64), 'projection-expired-snapshot',
    'scope-closure-validator-scanner.v1', 'blocked', 'complete',
    'unavailable', '30810000-0000-4000-8000-000000000203',
    '30810000-0000-4000-8000-000000000204', '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000304',
    '30810000-0000-4000-8000-000000000104',
    '30810000-0000-4000-8000-000000000001',
    'projection-deleted', 'projection-deleted-key', repeat('4', 64),
    repeat('5', 64), repeat('6', 64), 'projection-deleted-snapshot',
    'scope-closure-validator-scanner.v1', 'blocked', 'complete',
    'unavailable', '30810000-0000-4000-8000-000000000205',
    '30810000-0000-4000-8000-000000000206', '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000305',
    '30810000-0000-4000-8000-000000000105',
    '30810000-0000-4000-8000-000000000001',
    'projection-failed', 'projection-failed-key', repeat('7', 64),
    repeat('8', 64), repeat('9', 64), 'projection-failed-snapshot',
    'scope-closure-validator-scanner.v1', 'failed', 'incomplete',
    'unavailable', '30810000-0000-4000-8000-000000000207',
    '30810000-0000-4000-8000-000000000208', '{}', now()
  ),
  (
    '30810000-0000-4000-8000-000000000306',
    '30810000-0000-4000-8000-000000000106',
    '30810000-0000-4000-8000-000000000001',
    'projection-missing', 'projection-missing-key', repeat('0', 64),
    repeat('a', 64), repeat('b', 64), 'projection-missing-snapshot',
    'scope-closure-validator-scanner.v1', 'failed', 'incomplete',
    'unavailable', null, null, '{}', now()
  );

insert into public.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata
) values
  (
    '30810000-0000-4000-8000-000000000209',
    '30810000-0000-4000-8000-000000000106',
    'closure_report_xlsx', 'projection-test', 'wrong-job/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    109, repeat('3', 64), '{}'
  ),
  (
    '30810000-0000-4000-8000-000000000210',
    '30810000-0000-4000-8000-000000000106',
    'closure_complete_machine_result', 'projection-test',
    'source-job/manifest.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    110, repeat('4', 64), '{}'
  );

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000001',
  true
);

select is(
  jsonb_array_length(
    public.get_lcia_scope_closure_check(
      '30810000-0000-4000-8000-000000000302'
    ) #> '{data,artifacts}'
  ),
  2,
  'owner read always returns exactly two public artifact summaries'
);
select is(
  (
    select string_agg(item->>'artifactRole', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000302'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'closure_report_xlsx,closure_issue_manifest',
  'owner artifact summaries have a fixed public-role order'
);
select ok(
  not exists (
    select 1
    from (
      values
        ('30810000-0000-4000-8000-000000000301'::uuid),
        ('30810000-0000-4000-8000-000000000302'::uuid),
        ('30810000-0000-4000-8000-000000000303'::uuid),
        ('30810000-0000-4000-8000-000000000304'::uuid),
        ('30810000-0000-4000-8000-000000000305'::uuid),
        ('30810000-0000-4000-8000-000000000306'::uuid)
    ) as closure_check(id)
    cross join lateral jsonb_array_elements(
      public.get_lcia_scope_closure_check(closure_check.id)
      #> '{data,artifacts}'
    ) as artifact(item)
    where artifact.item ?| array[
      'artifactId',
      'bucket',
      'objectPath',
      'storageBucket',
      'storagePath',
      'service'
    ]
  ),
  'owner artifact summaries never expose identifiers, locators, or service data'
);
select ok(
  not exists (
    select 1
    from (
      values
        ('30810000-0000-4000-8000-000000000301'::uuid),
        ('30810000-0000-4000-8000-000000000302'::uuid),
        ('30810000-0000-4000-8000-000000000303'::uuid),
        ('30810000-0000-4000-8000-000000000304'::uuid),
        ('30810000-0000-4000-8000-000000000305'::uuid),
        ('30810000-0000-4000-8000-000000000306'::uuid)
    ) as closure_check(id)
    cross join lateral jsonb_array_elements(
      public.get_lcia_scope_closure_check(closure_check.id)
      #> '{data,artifacts}'
    ) as artifact(item)
    where (
      select count(*)
      from jsonb_object_keys(artifact.item)
    ) <> 8
       or artifact.item - array[
         'artifactRole',
         'artifactState',
         'filename',
         'format',
         'mediaType',
         'size',
         'checksumSha256',
         'artifactExpiresAt'
       ]::text[] <> '{}'::jsonb
  ),
  'every owner artifact summary has the exact bounded eight-field shape'
);
select is(
  (
    select string_agg(item->>'artifactState', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000301'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'pending,pending',
  'nonterminal missing links synthesize pending for both public roles'
);
select is(
  (
    select string_agg(item->>'artifactState', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000302'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'ready,ready',
  'ready linked evidence projects ready for both public roles'
);
select is(
  (
    select string_agg(item->>'artifactState', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000303'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'expired,expired',
  'elapsed evidence projects expired for both public roles'
);
select is(
  (
    select string_agg(item->>'artifactState', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000304'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'deleted,deleted',
  'tombstoned evidence projects deleted for both public roles'
);
select is(
  (
    select string_agg(item->>'artifactState', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000305'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'failed,failed',
  'linked evidence with invalid public media contracts projects failed'
);
select is(
  (
    select string_agg(item->>'artifactState', ',' order by ordinal)
    from jsonb_array_elements(
      public.get_lcia_scope_closure_check(
        '30810000-0000-4000-8000-000000000306'
      ) #> '{data,artifacts}'
    ) with ordinality as artifact(item, ordinal)
  ),
  'failed,failed',
  'terminal missing evidence projects failed for both public roles'
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #>> '{data,artifacts,0,filename}',
  'scope-closure-30810000-0000-4000-8000-000000000302.xlsx',
  'XLSX summary uses its semantic filename'
);
select is(
  (
    public.get_lcia_scope_closure_check(
      '30810000-0000-4000-8000-000000000302'
    ) #>> '{data,artifacts,1,format}'
  ) || ':' || (
    public.get_lcia_scope_closure_check(
      '30810000-0000-4000-8000-000000000302'
    ) #>> '{data,artifacts,1,mediaType}'
  ),
  'json:application/vnd.tiangong.scope-closure-manifest+json',
  'manifest summary uses the exact public format/media pair'
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #>> '{data,artifacts,0,size}',
  '101',
  'ready summary publishes the linked artifact size'
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #>> '{data,artifacts,1,checksumSha256}',
  repeat('b', 64),
  'ready summary publishes the linked artifact checksum'
);
select ok(
  (
    public.get_lcia_scope_closure_check(
      '30810000-0000-4000-8000-000000000302'
    ) #> '{data,artifacts,0,artifactExpiresAt}'
  ) is not null,
  'ready summary publishes artifact expiry'
);
select ok(
  (
    public.get_lcia_scope_closure_check(
      '30810000-0000-4000-8000-000000000306'
    ) #> '{data,artifacts,0,size}'
  ) = 'null'::jsonb
  and (
    public.get_lcia_scope_closure_check(
      '30810000-0000-4000-8000-000000000306'
    ) #> '{data,artifacts,1,checksumSha256}'
  ) = 'null'::jsonb,
  'terminal missing summaries retain exact null integrity fields'
);

reset role;
update public.lcia_scope_closure_checks
set report_artifact_id = '30810000-0000-4000-8000-000000000209'
where id = '30810000-0000-4000-8000-000000000302';
set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000001',
  true
);
select ok(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #>> '{data,artifacts,0,artifactState}' = 'failed'
  and public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #> '{data,artifacts,0,size}' = 'null'::jsonb
  and public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #> '{data,artifacts,0,checksumSha256}' = 'null'::jsonb
  and public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #> '{data,artifacts,0,artifactExpiresAt}' = 'null'::jsonb,
  'wrong-job XLSX summary fails without leaking integrity or expiry metadata'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_report_unavailable',
  'wrong-job XLSX download matches failed owner-summary eligibility'
);

reset role;
update public.lcia_scope_closure_checks
set report_artifact_id = '30810000-0000-4000-8000-000000000201',
    complete_machine_result_artifact_id =
      '30810000-0000-4000-8000-000000000210',
    reused_from_check_id = null
where id = '30810000-0000-4000-8000-000000000302';
set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000001',
  true
);
select ok(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #>> '{data,artifacts,1,artifactState}' = 'failed'
  and public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #> '{data,artifacts,1,size}' = 'null'::jsonb
  and public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #> '{data,artifacts,1,checksumSha256}' = 'null'::jsonb
  and public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #> '{data,artifacts,1,artifactExpiresAt}' = 'null'::jsonb,
  'non-source-job manifest summary fails without leaking metadata'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_issue_manifest'
  ) ->> 'code',
  'closure_report_unavailable',
  'non-source-job manifest download matches failed owner-summary eligibility'
);

reset role;
update public.lcia_scope_closure_checks
set reused_from_check_id = '30810000-0000-4000-8000-000000000306'
where id = '30810000-0000-4000-8000-000000000302';
set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000001',
  true
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) #>> '{data,artifacts,1,artifactState}',
  'ready',
  'manifest from the exact reused source job is owner-summary ready'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_issue_manifest'
  ) #>> '{data,artifactState}',
  'ready',
  'exact reused-source manifest ready summary is download-success equivalent'
);

reset role;
update public.lcia_scope_closure_checks
set complete_machine_result_artifact_id =
      '30810000-0000-4000-8000-000000000202',
    reused_from_check_id = null
where id = '30810000-0000-4000-8000-000000000302';
set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000001',
  true
);

select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302'
  ),
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_report_xlsx'
  ),
  'legacy Edge call shape is exactly equivalent to the XLSX selector'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_issue_manifest'
  ) #>> '{data,artifactRole}',
  'closure_issue_manifest',
  'strict manifest selector remains available'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_bundle'
  ) ->> 'code',
  'closure_artifact_role_invalid',
  'strict download RPC still rejects non-public selectors'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000303'
  ),
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000303',
    'closure_report_xlsx'
  ),
  'legacy overload preserves owner XLSX expiry semantics'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000303'
  ) ->> 'code',
  'closure_report_expired',
  'owner expired legacy download remains HTTP-410 shaped'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000304',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_report_unavailable',
  'deleted download remains unavailable'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000305',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_report_unavailable',
  'invalid linked media remains unavailable'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000306',
    'closure_report_xlsx'
  ) ->> 'code',
  'closure_report_unavailable',
  'terminal missing report remains unavailable'
);

select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000002',
  true
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-owner ready check read remains opaque'
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000303'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-owner expired check read remains opaque'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302'
  ),
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_report_xlsx'
  ),
  'legacy and strict XLSX calls share cross-owner opacity'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302',
    'closure_issue_manifest'
  ) ->> 'code',
  'closure_check_not_found',
  'cross-owner manifest selector remains opaque'
);

select set_config(
  'request.jwt.claim.sub',
  '30810000-0000-4000-8000-000000000003',
  true
);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) ->> 'code',
  'closure_check_not_found',
  'ordinary authenticated member cannot distinguish an owner check'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302'
  ) ->> 'code',
  'closure_check_not_found',
  'ordinary authenticated legacy download remains opaque'
);

select set_config('request.jwt.claim.sub', '', true);
select is(
  public.get_lcia_scope_closure_check(
    '30810000-0000-4000-8000-000000000302'
  ) ->> 'code',
  'auth_required',
  'authenticated grant without an actor claim fails closed'
);
select is(
  public.get_lcia_scope_closure_report_download(
    '30810000-0000-4000-8000-000000000302'
  ) ->> 'code',
  'auth_required',
  'legacy overload without an actor claim preserves auth-required behavior'
);

reset role;
select * from finish();
rollback;
