begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger name)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger);
  end if;
end;
$$;

select plan(35);

select has_table('public', 'lcia_result_packages', 'LCIA result packages table exists');
select has_table('public', 'lcia_result_publications', 'LCIA result publications table exists');
select ok(to_regclass('public.data_product_runs') is null, 'data_product_runs table is not created');
select ok(to_regclass('public.data_product_run_inputs') is null, 'data_product_run_inputs table is not created');
select ok(to_regclass('public.data_product_packages') is null, 'data_product_packages table is not created');
select ok(to_regclass('public.data_product_lcia_results') is null, 'data_product_lcia_results table is not created');
select ok(to_regclass('public.data_product_artifacts') is null, 'data_product_artifacts table is not created');

select has_function(
  'public',
  'cmd_lcia_result_build_request',
  array['text', 'jsonb', 'text', 'text', 'jsonb', 'text', 'jsonb'],
  'cmd_lcia_result_build_request exists'
);

select has_function(
  'public',
  'cmd_lcia_result_package_mark_ready',
  array['uuid', 'text', 'uuid', 'uuid', 'uuid', 'jsonb', 'jsonb', 'jsonb', 'jsonb', 'text', 'text', 'jsonb'],
  'cmd_lcia_result_package_mark_ready exists'
);

select has_function(
  'public',
  'cmd_lcia_result_package_publish',
  array['uuid', 'text', 'text', 'jsonb'],
  'cmd_lcia_result_package_publish exists'
);

select has_function(
  'public',
  'get_lcia_result_package_preview',
  array['uuid'],
  'get_lcia_result_package_preview exists'
);

select has_function(
  'public',
  'get_published_lcia_result_package',
  array['uuid', 'text', 'text'],
  'get_published_lcia_result_package exists'
);

select ok(
  (select relrowsecurity from pg_class where oid = 'public.lcia_result_packages'::regclass),
  'lcia_result_packages has RLS enabled'
);

select ok(
  not has_table_privilege('authenticated', 'public.lcia_result_packages', 'SELECT'),
  'authenticated users cannot directly select unpublished LCIA result packages'
);

select ok(
  not has_table_privilege('anon', 'public.lcia_result_publications', 'SELECT'),
  'anon users cannot directly select LCIA result publications'
);

select ok(
  exists (
    select 1
    from pg_constraint
    where conrelid = 'public.roles'::regclass
      and conname = 'roles_role_check'
      and pg_get_constraintdef(oid) like '%data_product_manager%'
  ),
  'roles check allows data_product_manager'
);

select ok(
  exists (
    select 1
    from pg_constraint
    where conrelid = 'public.lca_network_snapshots'::regclass
      and conname = 'lca_network_snapshots_scope_chk'
      and pg_get_constraintdef(oid) like '%data_product%'
  ),
  'lca_network_snapshots scope check allows data_product snapshots'
);

select ok(
  exists (
    select 1
    from public.worker_job_kinds
    where job_kind = 'lcia_result.package_build'
      and worker_queue = 'solver'
      and default_visibility = 'operator'
      and payload_schema_version = 'lcia_result.package_build.request.v2'
  ),
  'lcia_result.package_build worker job kind is registered'
);

select set_config('request.jwt.claim.role', 'authenticated', true);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    '98230000-0000-4000-8000-000000000001',
    'authenticated',
    'authenticated',
    'data-product-manager@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"98230000-0000-4000-8000-000000000001","email":"data-product-manager@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '98230000-0000-4000-8000-000000000002',
    'authenticated',
    'authenticated',
    'ordinary-user@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"98230000-0000-4000-8000-000000000002","email":"ordinary-user@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('98230000-0000-4000-8000-000000000001', '{"email":"data-product-manager@example.com"}'::jsonb, null),
  ('98230000-0000-4000-8000-000000000002', '{"email":"ordinary-user@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values ('00000000-0000-0000-0000-000000000000', '{"name":"System Team"}'::jsonb, 0, false)
on conflict (id) do nothing;

insert into public.roles (user_id, team_id, role)
values ('98230000-0000-4000-8000-000000000001', '00000000-0000-0000-0000-000000000000', 'data_product_manager');

alter table public.processes disable trigger "process_extract_md_trigger_insert";
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_insert');

insert into public.processes (id, version, json, user_id, state_code)
values
  ('98230000-0000-4000-8000-000000000101', '01.00.000', '{"processDataSet":{"name":"eligible-1"}}'::jsonb, '98230000-0000-4000-8000-000000000001', 100),
  ('98230000-0000-4000-8000-000000000101', '01.00.001', '{"processDataSet":{"name":"eligible-1-latest"}}'::jsonb, '98230000-0000-4000-8000-000000000001', 100),
  ('98230000-0000-4000-8000-000000000102', '01.00.000', '{"processDataSet":{"name":"eligible-2"}}'::jsonb, '98230000-0000-4000-8000-000000000001', 100),
  ('98230000-0000-4000-8000-000000000103', '01.00.000', '{"processDataSet":{"name":"draft"}}'::jsonb, '98230000-0000-4000-8000-000000000001', 0);

select is(
  (public.lcia_result_current_eligible_manifest()->>'eligibleInputCount')::integer,
  2,
  'global eligible manifest counts latest published process versions only'
);

select is(
  (
    select process.value->>'version'
    from jsonb_array_elements(public.lcia_result_current_eligible_manifest()->'inputManifest'->'processes') as process(value)
    where process.value->>'id' = '98230000-0000-4000-8000-000000000101'
  ),
  '01.00.001',
  'global eligible manifest keeps the latest published version per process id'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000002', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.cmd_lcia_result_build_request(
    p_name => 'ordinary user build',
    p_processes => null,
    p_coverage_mode => 'global_eligible',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => '[]'::jsonb,
    p_idempotency_key => 'pgtap-lcia-result-ordinary-user',
    p_audit => '{}'::jsonb
  )->>'code',
  'not_data_product_manager',
  'ordinary authenticated users cannot request LCIA result builds'
);

select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.cmd_lcia_result_build_request(
    p_name => 'draft input build',
    p_processes => jsonb_build_array(jsonb_build_object('id', '98230000-0000-4000-8000-000000000103', 'version', '01.00.000')),
    p_coverage_mode => 'subset',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => '[]'::jsonb,
    p_idempotency_key => 'pgtap-lcia-result-draft-input',
    p_audit => '{}'::jsonb
  )->>'code',
  'input_not_eligible',
  'LCIA result build request rejects draft process input'
);

select is(
  public.cmd_lcia_result_build_request(
    p_name => 'global build with manual input',
    p_processes => jsonb_build_array(jsonb_build_object('id', '98230000-0000-4000-8000-000000000101', 'version', '01.00.000')),
    p_coverage_mode => 'global_eligible',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => '[]'::jsonb,
    p_idempotency_key => 'pgtap-lcia-result-global-manual-input',
    p_audit => '{}'::jsonb
  )->>'code',
  'invalid_coverage_mode',
  'global eligible build request rejects manual process subsets'
);

create temporary table lcia_result_test_ids (
  label text primary key,
  id uuid not null
) on commit drop;

grant all on lcia_result_test_ids to public;

insert into lcia_result_test_ids (label, id)
select
  'build',
  (result->'data'->>'buildId')::uuid
from (
  select public.cmd_lcia_result_build_request(
    p_name => 'global published LCIA result build',
    p_processes => null,
    p_coverage_mode => 'global_eligible',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => jsonb_build_array(jsonb_build_object('method', 'EF', 'version', 'v1')),
    p_idempotency_key => 'pgtap-lcia-result-global-build',
    p_audit => '{}'::jsonb
  ) as result
) as created;

select is(
  (
    public.cmd_lcia_result_build_request(
      p_name => 'global published LCIA result build',
      p_processes => null,
      p_coverage_mode => 'global_eligible',
      p_default_impact_category => 'climate-change',
      p_lcia_method_set => jsonb_build_array(jsonb_build_object('method', 'EF', 'version', 'v1')),
      p_idempotency_key => 'pgtap-lcia-result-global-build',
      p_audit => '{}'::jsonb
    )->'data'->'workerJob'->>'jobKind'
  ),
  'lcia_result.package_build',
  'build request returns the LCIA result worker job kind'
);

select is(
  (
    public.cmd_lcia_result_build_request(
      p_name => 'global published LCIA result build',
      p_processes => null,
      p_coverage_mode => 'global_eligible',
      p_default_impact_category => 'climate-change',
      p_lcia_method_set => jsonb_build_array(jsonb_build_object('method', 'EF', 'version', 'v1')),
      p_idempotency_key => 'pgtap-lcia-result-global-build',
      p_audit => '{}'::jsonb
    )->'data'->>'includedInputCount'
  )::integer,
  2,
  'build request resolves only published eligible process inputs'
);

reset role;

insert into public.worker_jobs (
  id,
  job_kind,
  worker_runtime,
  worker_queue,
  subject_type,
  subject_id,
  requester_type,
  requested_by,
  idempotency_key,
  request_hash,
  visibility,
  payload_schema_version,
  payload_json
)
select
  '98230000-0000-4000-8000-000000000301'::uuid,
  'lcia_result.package_build',
  'calculator',
  'solver',
  'lcia_result_build',
  id,
  'operator',
  '98230000-0000-4000-8000-000000000001',
  'pgtap-lcia-result-worker-job',
  'manifest-hash',
  'operator',
  'lcia_result.package_build.request.v1',
  jsonb_build_object(
    'type', 'lcia_result_package_build',
    'build_id', id,
    'requested_by', '98230000-0000-4000-8000-000000000001',
    'coverage_mode', 'global_eligible',
    'input_status_filter', jsonb_build_object('state_code', jsonb_build_object('between', jsonb_build_array(100, 199))),
    'eligibility_definition', jsonb_build_object('predicateVersion', 'published-state-code-100-199:v1'),
    'eligibility_resolved_at', now(),
    'eligible_input_count', 2,
    'included_input_count', 2,
    'input_manifest_hash', public.lcia_result_current_eligible_manifest()->>'inputManifestHash',
    'input_manifest', public.lcia_result_current_eligible_manifest()->'inputManifest',
    'lcia_method_set', jsonb_build_array(jsonb_build_object('method', 'EF', 'version', 'v1')),
    'default_impact_category', 'climate-change',
    'postprocess_manifest', jsonb_build_object('postprocess_mode', 'skipped')
  )
from lcia_result_test_ids
where label = 'build';

insert into lcia_result_test_ids (label, id)
values
  ('worker_job', '98230000-0000-4000-8000-000000000301'),
  ('snapshot', '98230000-0000-4000-8000-000000000401'),
  ('result', '98230000-0000-4000-8000-000000000501'),
  ('latest_all_unit', '98230000-0000-4000-8000-000000000601');

insert into public.lca_network_snapshots (id, scope, process_filter, source_hash, status, created_by)
values (
  (select id from lcia_result_test_ids where label = 'snapshot'),
  'data_product',
  '{"state_code":{"between":[100,199]}}'::jsonb,
  'pgtap-snapshot-hash',
  'ready',
  '98230000-0000-4000-8000-000000000001'
);

insert into public.lca_results (
  id,
  job_id,
  snapshot_id,
  payload,
  diagnostics,
  artifact_url,
  artifact_sha256,
  artifact_byte_size,
  artifact_format,
  worker_job_id,
  is_pinned
)
values (
  (select id from lcia_result_test_ids where label = 'result'),
  (select id from lcia_result_test_ids where label = 'worker_job'),
  (select id from lcia_result_test_ids where label = 'snapshot'),
  '{}'::jsonb,
  '{}'::jsonb,
  's3://bucket/lcia-result.json',
  repeat('a', 64),
  123,
  'application/json',
  (select id from lcia_result_test_ids where label = 'worker_job'),
  false
);

insert into public.lca_latest_all_unit_results (
  id,
  snapshot_id,
  job_id,
  result_id,
  query_artifact_url,
  query_artifact_sha256,
  query_artifact_byte_size,
  query_artifact_format,
  status,
  worker_job_id
)
values (
  (select id from lcia_result_test_ids where label = 'latest_all_unit'),
  (select id from lcia_result_test_ids where label = 'snapshot'),
  (select id from lcia_result_test_ids where label = 'worker_job'),
  (select id from lcia_result_test_ids where label = 'result'),
  's3://bucket/query-sidecar.json',
  repeat('b', 64),
  456,
  'application/json',
  'ready',
  (select id from lcia_result_test_ids where label = 'worker_job')
);

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

insert into lcia_result_test_ids (label, id)
select
  'package',
  (result->'data'->>'packageId')::uuid
from (
  select public.cmd_lcia_result_package_mark_ready(
    p_build_worker_job_id => (select id from lcia_result_test_ids where label = 'worker_job'),
    p_package_version => '2026-06-public',
    p_snapshot_id => (select id from lcia_result_test_ids where label = 'snapshot'),
    p_result_id => (select id from lcia_result_test_ids where label = 'result'),
    p_latest_all_unit_result_id => (select id from lcia_result_test_ids where label = 'latest_all_unit'),
    p_available_impact_categories => jsonb_build_array('climate-change', 'acidification'),
    p_artifact_manifest => jsonb_build_object('persistenceMode', 'pinned'),
    p_audit => '{}'::jsonb
  ) as result
) as marked;

select is(
  (select status from public.lcia_result_packages where id = (select id from lcia_result_test_ids where label = 'package')),
  'preview_ready',
  'service role can mark an LCIA result package preview_ready'
);

select is(
  (select build_worker_job_id from public.lcia_result_packages where id = (select id from lcia_result_test_ids where label = 'package')),
  (select id from lcia_result_test_ids where label = 'worker_job'),
  'package keeps the producing worker_jobs reference'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000002', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.get_lcia_result_package_preview((select id from lcia_result_test_ids where label = 'package'))->>'code',
  'not_data_product_manager',
  'ordinary users cannot preview unpublished LCIA result packages'
);

select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.get_lcia_result_package_preview(
    (select id from lcia_result_test_ids where label = 'package')
  )->'data'->'summary'->>'packageVersion',
  '2026-06-public',
  'manager can preview LCIA result package metadata'
);

select is(
  public.get_lcia_result_package_preview(
    (select id from lcia_result_test_ids where label = 'package')
  )->'data'->'summary'->>'snapshotId',
  (select id::text from lcia_result_test_ids where label = 'snapshot'),
  'manager preview exposes stable snapshot reference for row-level result projection'
);

insert into lcia_result_test_ids (label, id)
select
  'publication',
  (result->'data'->>'publicationId')::uuid
from (
  select public.cmd_lcia_result_package_publish(
    p_package_id => (select id from lcia_result_test_ids where label = 'package'),
    p_display_default_impact_category => 'climate-change',
    p_reason => 'publish pgtap package',
    p_audit => '{}'::jsonb
  ) as result
) as published;

reset role;

select ok(
  (
    select is_current
    from public.lcia_result_publications
    where id = (select id from lcia_result_test_ids where label = 'publication')
  ),
  'manager can publish a package as current global public latest'
);

select ok(
  (select is_pinned from public.lca_results where id = (select id from lcia_result_test_ids where label = 'result')),
  'published LCIA result is pinned against result GC'
);

set local role anon;
select set_config('request.jwt.claim.role', 'anon', true);
select set_config('request.jwt.claims', '{"role":"anon"}', true);

select is(
  public.get_published_lcia_result_package(
    '98230000-0000-4000-8000-000000000101',
    '01.00.001',
    'climate-change'
  )->'data'->'resultArtifact'->>'artifactUrl',
  's3://bucket/lcia-result.json',
  'anon can read only the current public LCIA package artifact reference'
);

reset role;

select throws_ok(
  $$
    update public.lcia_result_packages
    set input_manifest_hash = 'mutated'
    where id = (select id from lcia_result_test_ids where label = 'package')
  $$,
  '23514',
  null,
  'preview_ready package content is immutable'
);

select throws_ok(
  $$
    insert into public.lcia_result_publications (
      package_id,
      publication_series_key,
      publication_channel,
      visibility_scope,
      is_current,
      status
    )
    values (
      (select id from lcia_result_test_ids where label = 'package'),
      'global',
      'public',
      'public',
      true,
      'current'
    )
  $$,
  '23505',
  null,
  'only one global public publication can be current'
);

select * from finish();
rollback;
