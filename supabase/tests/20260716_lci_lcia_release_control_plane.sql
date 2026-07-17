begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select no_plan();

select has_table('public', 'lca_release_runs', 'release runs table exists');
select has_table('public', 'lca_release_dataset_versions', 'release dataset index exists');
select has_table('public', 'lca_release_artifacts', 'release artifacts table exists');
select has_table('public', 'lca_release_approvals', 'release approvals table exists');
select has_table('public', 'lca_release_publications', 'release publications table exists');

select has_function(
  'public', 'cmd_lca_release_prepare',
  array['uuid', 'text', 'text', 'text', 'jsonb', 'text', 'text', 'jsonb', 'text', 'text', 'jsonb'],
  'release prepare command exists'
);
select has_function(
  'public', 'assert_lca_release_manager', array[]::text[],
  'side-effect-free release manager assertion exists'
);
select has_function(
  'public', 'cmd_lca_release_artifacts_finalize_service',
  array['uuid', 'text', 'jsonb', 'text', 'jsonb', 'jsonb'],
  'service artifact finalize command exists'
);
select has_function(
  'public', 'cmd_lca_release_approve',
  array['uuid', 'text', 'timestamptz', 'text', 'jsonb'],
  'release approval command exists'
);
select has_function(
  'public', 'cmd_lca_release_publish',
  array['uuid', 'uuid', 'text', 'text', 'text', 'text', 'text', 'jsonb'],
  'release publish command exists'
);
select has_function(
  'public', 'cmd_lca_release_readback_verify',
  array['uuid', 'text', 'jsonb', 'jsonb'],
  'release readback command exists'
);
select has_function(
  'public', 'cmd_lca_release_unpublish',
  array['uuid', 'text', 'jsonb'],
  'release unpublish command exists'
);
select has_function('public', 'get_lca_release_run', array['uuid'], 'release run query exists');
select has_function('public', 'get_current_lca_release', array[]::text[], 'current release query exists');
select has_function(
  'public', 'get_current_lca_release_process', array['uuid', 'text'],
  'current release process identity projection exists'
);
select has_function(
  'public', 'get_lca_release_artifact_download', array['uuid'],
  'release artifact download projection exists'
);
select has_function(
  'public', 'get_lcia_result_calculation_bundle', array['uuid'],
  'calculation bundle preview query exists'
);

select ok(
  (select bool_and(relrowsecurity)
   from pg_class
   where oid = any(array[
     'public.lca_release_runs'::regclass,
     'public.lca_release_dataset_versions'::regclass,
     'public.lca_release_artifacts'::regclass,
     'public.lca_release_approvals'::regclass,
     'public.lca_release_publications'::regclass
   ])),
  'all release tables have RLS enabled'
);
select ok(
  not has_table_privilege('authenticated', 'public.lca_release_runs', 'SELECT')
  and not has_table_privilege('authenticated', 'public.lca_release_runs', 'INSERT')
  and not has_table_privilege('anon', 'public.lca_release_publications', 'SELECT'),
  'anon and authenticated roles cannot access release tables directly'
);
select ok(
  not has_table_privilege('service_role', 'public.lca_release_runs', 'INSERT')
  and not has_table_privilege('service_role', 'public.lca_release_publications', 'INSERT'),
  'service role cannot bypass release commands with direct table writes'
);
select ok(
  has_function_privilege(
    'service_role',
    'public.cmd_lca_release_artifacts_finalize_service(uuid,text,jsonb,text,jsonb,jsonb)',
    'EXECUTE'
  ),
  'service role can execute only artifact finalization callback'
);
select ok(
  not has_function_privilege(
    'service_role',
    'public.cmd_lca_release_publish(uuid,uuid,text,text,text,text,text,jsonb)',
    'EXECUTE'
  )
  and not has_function_privilege(
    'service_role',
    'public.cmd_lca_release_approve(uuid,text,timestamptz,text,jsonb)',
    'EXECUTE'
  ),
  'service role cannot approve or publish releases'
);
select ok(
  has_function_privilege('authenticated', 'public.assert_lca_release_manager()', 'EXECUTE')
  and not has_function_privilege('anon', 'public.assert_lca_release_manager()', 'EXECUTE')
  and not has_function_privilege('service_role', 'public.assert_lca_release_manager()', 'EXECUTE'),
  'only authenticated user sessions can invoke the release manager assertion'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    'a7160000-0000-4000-8000-000000001001',
    'authenticated', 'authenticated', 'release-manager@example.com',
    'test-password-hash', now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a7160000-0000-4000-8000-000000001001","email":"release-manager@example.com"}'::jsonb,
    now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'a7160000-0000-4000-8000-000000001002',
    'authenticated', 'authenticated', 'release-reader@example.com',
    'test-password-hash', now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a7160000-0000-4000-8000-000000001002","email":"release-reader@example.com"}'::jsonb,
    now(), now(), false, false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a7160000-0000-4000-8000-000000001001', '{"email":"release-manager@example.com"}'::jsonb, null),
  ('a7160000-0000-4000-8000-000000001002', '{"email":"release-reader@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values ('00000000-0000-0000-0000-000000000000', '{"name":"System Team"}'::jsonb, 0, false)
on conflict (id) do nothing;

insert into public.roles (user_id, team_id, role)
values (
  'a7160000-0000-4000-8000-000000001001',
  '00000000-0000-0000-0000-000000000000',
  'data_product_manager'
);

create or replace function pg_temp.release_publish_plan()
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'schemaVersion', 'tiangong.release.publish-plan.v1',
    'releaseRunId', 'a7160000-0000-4000-8000-000000002001',
    'releaseVersion', '01.00.000',
    'profileLockHash', repeat('d', 64),
    'calculationBundleHash', repeat('c', 64),
    'artifactSetHash', repeat('f', 64),
    'datasets', jsonb_build_array(jsonb_build_object(
      'datasetType', 'process',
      'role', 'unit_process',
      'uuid', 'a7160000-0000-4000-8000-000000003001',
      'version', '01.00.000',
      'sourceProcess', jsonb_build_object(
        'id', 'a7160000-0000-4000-8000-000000003001',
        'version', '01.00.000'
      ),
      'canonicalContentHash', repeat('7', 64)
    )),
    'packages', jsonb_build_array(
      jsonb_build_object('profileId', 'unit-process-full-closure.v1', 'format', 'tidas', 'sha256', repeat('1', 64)),
      jsonb_build_object('profileId', 'unit-process-full-closure.v1', 'format', 'ilcd', 'sha256', repeat('2', 64)),
      jsonb_build_object('profileId', 'standalone-lifecyclemodel-result-full-closure.v1', 'format', 'tidas', 'sha256', repeat('3', 64)),
      jsonb_build_object('profileId', 'standalone-lifecyclemodel-result-full-closure.v1', 'format', 'ilcd', 'sha256', repeat('4', 64))
    ),
    'planHash', repeat('e', 64)
  )
$$;

create or replace function pg_temp.report_ref(p_name text)
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'path', 'reports/' || p_name || '.json',
    'sha256', repeat('a', 64),
    'byteSize', 10,
    'mediaType', 'application/json'
  )
$$;

create or replace function pg_temp.release_manifest()
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'schemaVersion', 'tiangong.release-manifest.v1',
    'releaseRunId', 'a7160000-0000-4000-8000-000000002001',
    'releaseVersion', '01.00.000',
    'scope', jsonb_build_object(
      'coverageMode', 'global_eligible',
      'selectionManifestHash', repeat('a', 64),
      'processCount', 1
    ),
    'profileLockHash', repeat('d', 64),
    'calculationBundle', jsonb_build_object(
      'calculationId', 'a7160000-0000-4000-8000-000000004001',
      'bundleContentHash', repeat('c', 64),
      'manifestSha256', repeat('b', 64)
    ),
    'datasets', jsonb_build_array(
      jsonb_build_object(
        'datasetType', 'process', 'role', 'unit_process',
        'uuid', 'a7160000-0000-4000-8000-000000003001', 'version', '01.00.000',
        'sourceProcess', jsonb_build_object('id', 'a7160000-0000-4000-8000-000000003001', 'version', '01.00.000'),
        'versionSignificantHash', repeat('7', 64), 'semanticHash', repeat('7', 64),
        'canonicalContentHash', repeat('7', 64),
        'artifact', jsonb_build_object('path', 'processes/unit.json', 'sha256', repeat('7', 64), 'byteSize', 100, 'mediaType', 'application/json')
      ),
      jsonb_build_object(
        'datasetType', 'lifecyclemodel', 'role', 'lifecycle_model',
        'uuid', 'a7160000-0000-4000-8000-000000003002', 'version', '01.00.000',
        'sourceProcess', jsonb_build_object('id', 'a7160000-0000-4000-8000-000000003001', 'version', '01.00.000'),
        'versionSignificantHash', repeat('8', 64), 'semanticHash', repeat('8', 64),
        'canonicalContentHash', repeat('8', 64),
        'artifact', jsonb_build_object('path', 'lifecyclemodels/model.json', 'sha256', repeat('8', 64), 'byteSize', 110, 'mediaType', 'application/json')
      ),
      jsonb_build_object(
        'datasetType', 'process', 'role', 'result_process',
        'uuid', 'a7160000-0000-4000-8000-000000003003', 'version', '01.00.000',
        'sourceProcess', jsonb_build_object('id', 'a7160000-0000-4000-8000-000000003001', 'version', '01.00.000'),
        'versionSignificantHash', repeat('9', 64), 'semanticHash', repeat('9', 64),
        'canonicalContentHash', repeat('9', 64),
        'artifact', jsonb_build_object('path', 'processes/result.json', 'sha256', repeat('9', 64), 'byteSize', 120, 'mediaType', 'application/json')
      )
    ),
    'packages', jsonb_build_array(
      jsonb_build_object(
        'profileId', 'unit-process-full-closure.v1', 'format', 'tidas', 'selfContained', true,
        'closureHash', repeat('5', 64),
        'artifact', jsonb_build_object('path', 'packages/unit.tidas.zip', 'sha256', repeat('1', 64), 'byteSize', 101, 'mediaType', 'application/zip')
      ),
      jsonb_build_object(
        'profileId', 'unit-process-full-closure.v1', 'format', 'ilcd', 'selfContained', true,
        'closureHash', repeat('5', 64),
        'artifact', jsonb_build_object('path', 'packages/unit.ilcd.zip', 'sha256', repeat('2', 64), 'byteSize', 102, 'mediaType', 'application/zip')
      ),
      jsonb_build_object(
        'profileId', 'standalone-lifecyclemodel-result-full-closure.v1', 'format', 'tidas', 'selfContained', true,
        'closureHash', repeat('6', 64),
        'artifact', jsonb_build_object('path', 'packages/result.tidas.zip', 'sha256', repeat('3', 64), 'byteSize', 103, 'mediaType', 'application/zip')
      ),
      jsonb_build_object(
        'profileId', 'standalone-lifecyclemodel-result-full-closure.v1', 'format', 'ilcd', 'selfContained', true,
        'closureHash', repeat('6', 64),
        'artifact', jsonb_build_object('path', 'packages/result.ilcd.zip', 'sha256', repeat('4', 64), 'byteSize', 104, 'mediaType', 'application/zip')
      )
    ),
    'validation', jsonb_build_object(
      'tidas', jsonb_build_object('status', 'passed', 'report', pg_temp.report_ref('tidas')),
      'ilcd', jsonb_build_object('status', 'passed', 'report', pg_temp.report_ref('ilcd')),
      'semanticRoundtrip', jsonb_build_object('status', 'passed', 'report', pg_temp.report_ref('roundtrip')),
      'referenceClosure', jsonb_build_object('status', 'passed', 'report', pg_temp.report_ref('closure')),
      'numericParity', jsonb_build_object('status', 'passed', 'report', pg_temp.report_ref('numeric'))
    ),
    'artifactSetHash', repeat('f', 64),
    'publishPlanHash', repeat('e', 64)
  )
$$;

create or replace function pg_temp.uploaded_artifacts()
returns jsonb
language sql
immutable
as $$
  select jsonb_build_array(
    jsonb_build_object('profileId', 'unit-process-full-closure.v1', 'format', 'tidas', 'storageBucket', 'lca-release-private', 'objectKey', 'sha256/11/unit.tidas.zip', 'sha256', repeat('1', 64), 'byteSize', 101, 'mediaType', 'application/zip'),
    jsonb_build_object('profileId', 'unit-process-full-closure.v1', 'format', 'ilcd', 'storageBucket', 'lca-release-private', 'objectKey', 'sha256/22/unit.ilcd.zip', 'sha256', repeat('2', 64), 'byteSize', 102, 'mediaType', 'application/zip'),
    jsonb_build_object('profileId', 'standalone-lifecyclemodel-result-full-closure.v1', 'format', 'tidas', 'storageBucket', 'lca-release-private', 'objectKey', 'sha256/33/result.tidas.zip', 'sha256', repeat('3', 64), 'byteSize', 103, 'mediaType', 'application/zip'),
    jsonb_build_object('profileId', 'standalone-lifecyclemodel-result-full-closure.v1', 'format', 'ilcd', 'storageBucket', 'lca-release-private', 'objectKey', 'sha256/44/result.ilcd.zip', 'sha256', repeat('4', 64), 'byteSize', 104, 'mediaType', 'application/zip')
  )
$$;

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.jwt.claim.sub', '', true);

select is(
  public.assert_lca_release_manager()->>'code',
  'auth_required',
  'publishable-key context without a user fails the manager assertion'
);

select is(
  public.cmd_lca_release_prepare(
    'a7160000-0000-4000-8000-000000002001', '01.00.000', repeat('a', 64), repeat('b', 64),
    jsonb_build_object('manifestUrl', 's3://bundle/manifest.json'), repeat('c', 64), repeat('d', 64),
    pg_temp.release_publish_plan(), repeat('e', 64), 'release-prepare-1', '{}'::jsonb
  )->>'code',
  'auth_required',
  'API/publishable key without a user session cannot prepare releases'
);

select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001002', true);

select is(
  public.assert_lca_release_manager()->>'code',
  'not_data_product_manager',
  'ordinary authenticated user fails the side-effect-free manager assertion'
);

select is(
  public.cmd_lca_release_prepare(
    'a7160000-0000-4000-8000-000000002001', '01.00.000', repeat('a', 64), repeat('b', 64),
    jsonb_build_object('manifestUrl', 's3://bundle/manifest.json'), repeat('c', 64), repeat('d', 64),
    pg_temp.release_publish_plan(), repeat('e', 64), 'release-prepare-1', '{}'::jsonb
  )->>'code',
  'not_data_product_manager',
  'ordinary authenticated user cannot prepare releases'
);
select is(
  public.cmd_lca_release_approve(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64), null, null, '{}'::jsonb
  )->>'code',
  'not_data_product_manager',
  'ordinary authenticated user cannot approve releases'
);
select is(
  public.get_lca_release_run('a7160000-0000-4000-8000-000000002001')->>'code',
  'release_run_not_found',
  'uncreated release remains absent'
);

select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001001', true);

select is(
  public.assert_lca_release_manager()->'data'->>'userId',
  'a7160000-0000-4000-8000-000000001001',
  'manager assertion returns the live authenticated manager identity'
);

select is(
  public.cmd_lca_release_prepare(
    'a7160000-0000-4000-8000-000000002001', '01.00.000', repeat('a', 64), repeat('b', 64),
    jsonb_build_object('manifestUrl', 's3://bundle/manifest.json'), repeat('c', 64), repeat('d', 64),
    pg_temp.release_publish_plan() || jsonb_build_object('planHash', repeat('0', 64)),
    repeat('e', 64), 'release-prepare-1', '{}'::jsonb
  )->>'code',
  'publish_plan_mismatch',
  'prepare rejects a plan whose embedded hash differs'
);

select is(
  public.cmd_lca_release_prepare(
    'a7160000-0000-4000-8000-000000002001', '01.00.000', repeat('a', 64), repeat('b', 64),
    jsonb_build_object('manifestUrl', 's3://bundle/manifest.json'), repeat('c', 64), repeat('d', 64),
    pg_temp.release_publish_plan(), repeat('e', 64), 'release-prepare-1', '{}'::jsonb
  )->'data'->>'status',
  'prepared',
  'manager prepares an exact immutable release plan'
);
select ok(
  (public.cmd_lca_release_prepare(
    'a7160000-0000-4000-8000-000000002001', '01.00.000', repeat('a', 64), repeat('b', 64),
    jsonb_build_object('manifestUrl', 's3://bundle/manifest.json'), repeat('c', 64), repeat('d', 64),
    pg_temp.release_publish_plan(), repeat('e', 64), 'release-prepare-1', '{}'::jsonb
  )->>'reused')::boolean,
  'identical prepare retry is idempotent'
);
select is(
  public.cmd_lca_release_prepare(
    'a7160000-0000-4000-8000-000000002001', '01.00.000', repeat('a', 64), repeat('0', 64),
    jsonb_build_object('manifestUrl', 's3://bundle/manifest.json'), repeat('c', 64), repeat('d', 64),
    pg_temp.release_publish_plan(), repeat('e', 64), 'release-prepare-1', '{}'::jsonb
  )->>'code',
  'release_prepare_conflict',
  'same release id cannot be rebound to different inputs'
);
select is(
  public.get_lca_release_run('a7160000-0000-4000-8000-000000002001')->'data'->'calculationBundle'->>'manifestUrl',
  's3://bundle/manifest.json',
  'manager private read exposes Calculation Bundle ref'
);

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  public.cmd_lca_release_artifacts_finalize_service(
    'a7160000-0000-4000-8000-000000002001', repeat('0', 64), pg_temp.release_manifest(),
    repeat('9', 64), pg_temp.uploaded_artifacts(), '{}'::jsonb
  )->>'code',
  'publish_plan_hash_mismatch',
  'service finalize cannot change the prepared plan hash'
);
select is(
  public.cmd_lca_release_artifacts_finalize_service(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64),
    jsonb_set(pg_temp.release_manifest(), '{validation,numericParity,status}', '"failed"'::jsonb),
    repeat('9', 64), pg_temp.uploaded_artifacts(), '{}'::jsonb
  )->>'code',
  'validation_not_passed',
  'service finalize rejects a failed numerical gate'
);
select is(
  public.cmd_lca_release_artifacts_finalize_service(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64),
    jsonb_set(
      pg_temp.release_manifest(), '{datasets,0,sourceProcess,id}',
      '"a7160000-0000-4000-8000-000000009999"'::jsonb
    ),
    repeat('9', 64), pg_temp.uploaded_artifacts(), '{}'::jsonb
  )->>'code',
  'dataset_index_invalid',
  'service finalize requires each Unit Process source identity to map to itself'
);
select is(
  public.cmd_lca_release_artifacts_finalize_service(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64),
    jsonb_set(
      pg_temp.release_manifest(), '{datasets,1,sourceProcess,id}',
      '"a7160000-0000-4000-8000-000000009999"'::jsonb
    ),
    repeat('9', 64), pg_temp.uploaded_artifacts(), '{}'::jsonb
  )->>'code',
  'dataset_source_process_set_invalid',
  'service finalize requires one complete Unit Process, LifecycleModel, and Result identity set per source Process'
);
select is(
  public.cmd_lca_release_artifacts_finalize_service(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64), pg_temp.release_manifest(),
    repeat('9', 64), pg_temp.uploaded_artifacts(), '{}'::jsonb
  )->'data'->>'status',
  'ready_for_approval',
  'service finalizes exact validated artifacts without publishing'
);
select ok(
  (public.cmd_lca_release_artifacts_finalize_service(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64), pg_temp.release_manifest(),
    repeat('9', 64), pg_temp.uploaded_artifacts(), '{}'::jsonb
  )->>'reused')::boolean,
  'identical service finalize retry is idempotent'
);

reset role;
select is((select count(*)::integer from public.lca_release_artifacts), 4, 'four package refs are durable');
select is((select count(*)::integer from public.lca_release_dataset_versions), 3, 'dataset identity/version index is durable');
select is(
  (select status from public.lca_release_runs where id = 'a7160000-0000-4000-8000-000000002001'),
  'ready_for_approval',
  'service callback cannot advance beyond ready_for_approval'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001001', true);

select is(
  public.cmd_lca_release_approve(
    'a7160000-0000-4000-8000-000000002001', repeat('0', 64), null, null, '{}'::jsonb
  )->>'code',
  'publish_plan_hash_mismatch',
  'approval rejects a non-exact plan hash'
);

create temporary table release_test_receipts (
  label text primary key,
  id uuid,
  hash text,
  payload jsonb
) on commit drop;
grant all on release_test_receipts to public;

insert into release_test_receipts(label, id, hash)
select
  'approval',
  (result->'data'->>'approvalId')::uuid,
  result->'data'->>'approvalHash'
from (
  select public.cmd_lca_release_approve(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64), now() + interval '1 day',
    'release approved by data manager', jsonb_build_object('correlationId', 'approval-1')
  ) as result
) as approved;

select matches(
  (select hash from release_test_receipts where label = 'approval'),
  '^[0-9a-f]{64}$',
  'approval creates a durable receipt hash'
);
select ok(
  (public.cmd_lca_release_approve(
    'a7160000-0000-4000-8000-000000002001', repeat('e', 64), null, null, '{}'::jsonb
  )->>'reused')::boolean,
  'approval retry reuses the active receipt'
);

select is(
  public.cmd_lca_release_publish(
    'a7160000-0000-4000-8000-000000002001',
    (select id from release_test_receipts where label = 'approval'),
    repeat('0', 64), repeat('e', 64), 'release-publish-1', repeat('8', 64), null, '{}'::jsonb
  )->>'code',
  'approval_invalid',
  'publish rejects an approval receipt hash mismatch'
);

insert into release_test_receipts(label, id)
select
  'publication',
  (result->'data'->>'publicationId')::uuid
from (
  select public.cmd_lca_release_publish(
    'a7160000-0000-4000-8000-000000002001',
    (select id from release_test_receipts where label = 'approval'),
    (select hash from release_test_receipts where label = 'approval'),
    repeat('e', 64), 'release-publish-1', repeat('8', 64),
    'publish exact four-artifact release', jsonb_build_object('correlationId', 'publish-1')
  ) as result
) as published;

select ok((select id is not null from release_test_receipts where label = 'publication'), 'manager publishes with exact approval and plan');

reset role;
insert into release_test_receipts(label, id)
select 'download_artifact', id
from public.lca_release_artifacts
order by profile_id, artifact_format
limit 1;
insert into release_test_receipts(label, payload)
select
  'readback',
  jsonb_agg(jsonb_build_object('artifactId', id, 'sha256', sha256) order by id)
from public.lca_release_artifacts
where release_run_id = 'a7160000-0000-4000-8000-000000002001';
select ok(
  (select bool_and(pinned and published_at is not null) from public.lca_release_artifacts),
  'published artifacts are permanently pinned'
);
select is(
  (select status from public.lca_release_approvals where id = (select id from release_test_receipts where label = 'approval')),
  'consumed',
  'publish consumes the durable approval'
);
select ok(
  exists (
    select 1 from public.lca_release_publications
    where id = (select id from release_test_receipts where label = 'publication')
      and approved_by = 'a7160000-0000-4000-8000-000000001001'
      and executed_by = 'a7160000-0000-4000-8000-000000001001'
      and credential_fingerprint = repeat('8', 64)
  ),
  'publication separately records approver, executor, and non-secret credential fingerprint'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001001', true);
select ok(
  (public.cmd_lca_release_publish(
    'a7160000-0000-4000-8000-000000002001',
    (select id from release_test_receipts where label = 'approval'),
    (select hash from release_test_receipts where label = 'approval'),
    repeat('e', 64), 'release-publish-1', repeat('8', 64), null, '{}'::jsonb
  )->>'reused')::boolean,
  'publish retry is idempotent after approval consumption'
);

set local role anon;
select set_config('request.jwt.claim.role', 'anon', true);
select set_config('request.jwt.claims', '{"role":"anon"}', true);
select set_config('request.jwt.claim.sub', '', true);

select is(
  public.get_current_lca_release()->'data'->>'releaseVersion',
  '01.00.000',
  'anonymous consumer can read the current public release projection'
);
select is(
  public.get_current_lca_release()->'data'->>'createdBy',
  null,
  'public release projection does not expose the manager user id'
);
select is(
  public.get_current_lca_release()->'data'->'validation'->>'semanticRoundtrip',
  'passed',
  'public release projection exposes sanitized validation status'
);
select is(
  (public.get_current_lca_release()->'data'->'datasetCounts'->>'result_process')::integer,
  1,
  'public release projection exposes role counts without dataset object locators'
);
select is(
  jsonb_array_length(
    public.get_current_lca_release_process(
      'a7160000-0000-4000-8000-000000003001', '01.00.000'
    )->'data'->'datasets'
  ),
  3,
  'process projection resolves Unit Process, LifecycleModel, and Result Process identities'
);
select is(
  jsonb_path_query_first(
    public.get_current_lca_release_process(
      'a7160000-0000-4000-8000-000000003001', '01.00.000'
    ),
    '$.data.datasets[*] ? (@.role == "lifecycle_model").uuid'
  ) #>> '{}',
  'a7160000-0000-4000-8000-000000003002',
  'process projection exposes the generated LifecycleModel identity'
);
select ok(
  public.get_current_lca_release_process(
    'a7160000-0000-4000-8000-000000003001', '01.00.000'
  )::text not like '%objectKey%'
  and public.get_current_lca_release_process(
    'a7160000-0000-4000-8000-000000003001', '01.00.000'
  )::text not like '%artifact_ref%',
  'process projection does not expose internal storage locators'
);
select is(
  public.get_current_lca_release_process(
    'a7160000-0000-4000-8000-000000009999', '01.00.000'
  )->>'code',
  'release_process_not_found',
  'process projection has an explicit legacy or out-of-scope empty state'
);
select ok(
  (public.get_lca_release_artifact_download(
    (select id from release_test_receipts where label = 'download_artifact')
  )->'data'->>'public')::boolean,
  'published artifact is eligible for Edge signed download'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001001', true);

select is(
  public.cmd_lca_release_readback_verify(
    'a7160000-0000-4000-8000-000000002001', repeat('0', 64), '[]'::jsonb, '{}'::jsonb
  )->>'code',
  'readback_manifest_hash_mismatch',
  'readback rejects a different release manifest hash'
);

select is(
  public.cmd_lca_release_readback_verify(
    'a7160000-0000-4000-8000-000000002001',
    repeat('9', 64),
    (select payload from release_test_receipts where label = 'readback'),
    jsonb_build_object('correlationId', 'readback-1')
  )->'data'->>'status',
  'readback_verified',
  'independent readback verifies manifest and all artifact hashes'
);

reset role;
select throws_ok(
  $$
    update public.lca_release_runs
    set publish_plan_hash = repeat('0', 64)
    where id = 'a7160000-0000-4000-8000-000000002001'
  $$,
  '23514',
  null,
  'prepared plan content is immutable even to direct privileged SQL'
);
select throws_ok(
  $$
    update public.lca_release_artifacts
    set sha256 = repeat('0', 64)
    where release_run_id = 'a7160000-0000-4000-8000-000000002001'
  $$,
  '23514',
  null,
  'published artifact content is immutable'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001002', true);

select is(
  public.cmd_lca_release_unpublish(
    (select id from release_test_receipts where label = 'publication'),
    'ordinary user attempt', '{}'::jsonb
  )->>'code',
  'not_data_product_manager',
  'ordinary authenticated user cannot unpublish releases'
);

select set_config('request.jwt.claim.sub', 'a7160000-0000-4000-8000-000000001001', true);
select is(
  public.cmd_lca_release_unpublish(
    (select id from release_test_receipts where label = 'publication'),
    'channel withdrawal test', jsonb_build_object('correlationId', 'unpublish-1')
  )->'data'->>'status',
  'unpublished',
  'manager can unpublish through an audited state transition'
);

set local role anon;
select set_config('request.jwt.claim.role', 'anon', true);
select set_config('request.jwt.claims', '{"role":"anon"}', true);
select set_config('request.jwt.claim.sub', '', true);
select is(
  public.get_current_lca_release()->>'code',
  'publication_not_found',
  'unpublished release is removed from the current public projection'
);

reset role;
select * from finish();
rollback;
