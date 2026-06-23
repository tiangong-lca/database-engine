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

select plan(34);

select has_table('public', 'data_product_runs', 'data product runs table exists');
select has_table('public', 'data_product_run_inputs', 'data product run inputs table exists');
select has_table('public', 'data_product_packages', 'data product packages table exists');
select has_table('public', 'data_product_package_items', 'data product package items table exists');
select has_table('public', 'data_product_lcia_results', 'data product LCIA result rows table exists');
select has_table('public', 'data_product_artifacts', 'data product artifacts table exists');
select has_table('public', 'data_product_publications', 'data product publications table exists');

select has_function(
  'public',
  'cmd_data_product_run_create',
  array['text', 'jsonb', 'text', 'text', 'jsonb', 'text', 'jsonb'],
  'cmd_data_product_run_create exists'
);

select has_function(
  'public',
  'cmd_data_product_package_mark_ready',
  array['uuid', 'text', 'uuid', 'uuid', 'text', 'text', 'jsonb', 'jsonb', 'jsonb'],
  'cmd_data_product_package_mark_ready exists'
);

select has_function(
  'public',
  'cmd_data_product_package_publish',
  array['uuid', 'text', 'text', 'jsonb'],
  'cmd_data_product_package_publish exists'
);

select has_function(
  'public',
  'get_data_product_package_preview',
  array['uuid'],
  'get_data_product_package_preview exists'
);

select has_function(
  'public',
  'get_published_process_lcia_results',
  array['uuid', 'text', 'text'],
  'get_published_process_lcia_results exists'
);

select ok(
  (select relrowsecurity from pg_class where oid = 'public.data_product_lcia_results'::regclass),
  'data_product_lcia_results has RLS enabled'
);

select ok(
  not has_table_privilege('authenticated', 'public.data_product_lcia_results', 'SELECT'),
  'authenticated users cannot directly select unpublished data product result rows'
);

select ok(
  not has_table_privilege('anon', 'public.data_product_lcia_results', 'SELECT'),
  'anon users cannot directly select data product result rows'
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
    where job_kind = 'data_product.package_build'
      and worker_queue = 'solver'
      and default_visibility = 'operator'
      and payload_schema_version = 'data_product.package_build.request.v1'
  ),
  'data_product.package_build worker job kind is registered'
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
  ('98230000-0000-4000-8000-000000000102', '01.00.000', '{"processDataSet":{"name":"eligible-2"}}'::jsonb, '98230000-0000-4000-8000-000000000001', 100),
  ('98230000-0000-4000-8000-000000000103', '01.00.000', '{"processDataSet":{"name":"draft"}}'::jsonb, '98230000-0000-4000-8000-000000000001', 0);

set local role authenticated;
select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000002', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.cmd_data_product_run_create(
    p_name => 'ordinary user run',
    p_processes => null,
    p_coverage_mode => 'global_eligible',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => '[]'::jsonb,
    p_idempotency_key => 'pgtap-data-product-ordinary-user',
    p_audit => '{}'::jsonb
  )->>'code',
  'not_data_product_manager',
  'ordinary authenticated users cannot create data product runs'
);

select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.cmd_data_product_run_create(
    p_name => 'draft input run',
    p_processes => jsonb_build_array(jsonb_build_object('id', '98230000-0000-4000-8000-000000000103', 'version', '01.00.000')),
    p_coverage_mode => 'subset',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => '[]'::jsonb,
    p_idempotency_key => 'pgtap-data-product-draft-input',
    p_audit => '{}'::jsonb
  )->>'code',
  'input_not_eligible',
  'data product run creation rejects draft process input'
);

select is(
  public.cmd_data_product_run_create(
    p_name => 'global run with manual input',
    p_processes => jsonb_build_array(jsonb_build_object('id', '98230000-0000-4000-8000-000000000101', 'version', '01.00.000')),
    p_coverage_mode => 'global_eligible',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => '[]'::jsonb,
    p_idempotency_key => 'pgtap-data-product-global-manual-input',
    p_audit => '{}'::jsonb
  )->>'code',
  'invalid_coverage_mode',
  'global eligible run creation rejects manual process subsets'
);

create temporary table data_product_test_ids (
  label text primary key,
  id uuid not null
) on commit drop;

grant all on data_product_test_ids to public;

insert into data_product_test_ids (label, id)
select
  'run',
  (result->'data'->>'runId')::uuid
from (
  select public.cmd_data_product_run_create(
    p_name => 'global published run',
    p_processes => null,
    p_coverage_mode => 'global_eligible',
    p_default_impact_category => 'climate-change',
    p_lcia_method_set => jsonb_build_array(jsonb_build_object('method', 'EF', 'version', 'v1')),
    p_idempotency_key => 'pgtap-data-product-global-run',
    p_audit => '{}'::jsonb
  ) as result
) as created;

select is(
  (
    select coverage_mode || ':' || eligible_input_count::text || ':' || included_input_count::text
    from public.data_product_runs
    where id = (select id from data_product_test_ids where label = 'run')
  ),
  'global_eligible:2:2',
  'global eligible run captures only published predicate inputs'
);

select ok(
  not exists (
    select 1
    from public.data_product_run_inputs
    where run_id = (select id from data_product_test_ids where label = 'run')
      and state_code = 0
  ),
  'global eligible run does not include draft process inputs'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

insert into public.lca_network_snapshots (
  id,
  scope,
  process_filter,
  provider_matching_rule,
  source_hash,
  status,
  created_at,
  updated_at
) values (
  '98230000-0000-4000-8000-000000000201',
  'data_product',
  '{"process_states":[100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199],"include_user_id":null}'::jsonb,
  'split_by_evidence_hybrid',
  'pgtap-data-product-source',
  'ready',
  now(),
  now()
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
  artifact_format
) values (
  '98230000-0000-4000-8000-000000000202',
  '98230000-0000-4000-8000-000000000203',
  '98230000-0000-4000-8000-000000000201',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/pgtap/data-product.h5',
  repeat('a', 64),
  1024,
  'hdf5:v1'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.cmd_data_product_package_publish(
    p_package_id => '98230000-0000-4000-8000-000000000301',
    p_display_default_impact_category => 'climate-change',
    p_reason => 'missing package',
    p_audit => '{}'::jsonb
  )->>'code',
  'package_not_ready',
  'publish rejects missing packages'
);

insert into data_product_test_ids (label, id)
select
  'package',
  (result->'data'->>'packageId')::uuid
from (
  select public.cmd_data_product_package_mark_ready(
    p_run_id => (select id from data_product_test_ids where label = 'run'),
    p_package_version => '2026.06.001',
    p_snapshot_id => '98230000-0000-4000-8000-000000000201',
    p_source_result_id => '98230000-0000-4000-8000-000000000202',
    p_package_result_hash => 'pgtap-package-result-hash',
    p_default_impact_category => 'climate-change',
    p_result_rows => jsonb_build_array(
      jsonb_build_object(
        'process_id', '98230000-0000-4000-8000-000000000101',
        'process_version', '01.00.000',
        'impact_category_id', 'climate-change',
        'impact_label_snapshot', 'Climate change',
        'value', 12.5,
        'unit', 'kg CO2 eq',
        'source_result_id', '98230000-0000-4000-8000-000000000202',
        'source_artifact_sha256', repeat('a', 64)
      ),
      jsonb_build_object(
        'process_id', '98230000-0000-4000-8000-000000000102',
        'process_version', '01.00.000',
        'impact_category_id', 'climate-change',
        'impact_label_snapshot', 'Climate change',
        'value', 25.0,
        'unit', 'kg CO2 eq',
        'source_result_id', '98230000-0000-4000-8000-000000000202',
        'source_artifact_sha256', repeat('a', 64)
      )
    ),
    p_artifacts => jsonb_build_array(
      jsonb_build_object(
        'artifact_type', 'source_result',
        'storage_ref', 'storage://lca_results/pgtap/data-product.h5',
        'sha256', repeat('a', 64),
        'byte_size', 1024,
        'format', 'hdf5:v1',
        'persistence_mode', 'pinned',
        'is_persisted', true,
        'source_result_id', '98230000-0000-4000-8000-000000000202',
        'snapshot_id', '98230000-0000-4000-8000-000000000201'
      ),
      jsonb_build_object(
        'artifact_type', 'snapshot_index',
        'storage_ref', 'storage://lca_results/pgtap/snapshot-index.json',
        'sha256', repeat('b', 64),
        'byte_size', 2048,
        'format', 'snapshot-index:v1',
        'persistence_mode', 'copied',
        'is_persisted', true,
        'snapshot_id', '98230000-0000-4000-8000-000000000201'
      )
    ),
    p_audit => '{}'::jsonb
  ) as result
) as ready;

select is(
  (
    select status || ':' || included_input_count::text || ':' || default_impact_category
    from public.data_product_packages
    where id = (select id from data_product_test_ids where label = 'package')
  ),
  'preview_ready:2:climate-change',
  'package mark ready creates an immutable preview package'
);

select is(
  (
    select count(*)::text
    from public.data_product_lcia_results
    where package_id = (select id from data_product_test_ids where label = 'package')
  ),
  '2',
  'package mark ready materializes LCIA result rows'
);

select is(
  public.cmd_data_product_package_publish(
    p_package_id => (select id from data_product_test_ids where label = 'package'),
    p_display_default_impact_category => 'missing-impact',
    p_reason => 'bad default impact',
    p_audit => '{}'::jsonb
  )->>'code',
  'default_impact_missing',
  'publish rejects a default impact category that does not exist in package rows'
);

insert into data_product_test_ids (label, id)
select
  'publication',
  (result->'data'->>'publicationId')::uuid
from (
  select public.cmd_data_product_package_publish(
    p_package_id => (select id from data_product_test_ids where label = 'package'),
    p_display_default_impact_category => 'climate-change',
    p_reason => 'publish pgtap package',
    p_audit => '{}'::jsonb
  ) as result
) as published;

select is(
  (
    select status || ':' || is_current::text || ':' || publication_series_key || ':' || publication_channel || ':' || visibility_scope
    from public.data_product_publications
    where id = (select id from data_product_test_ids where label = 'publication')
  ),
  'current:true:global:public:public',
  'publish creates current global public publication'
);

select is(
  (
    select is_pinned::text
    from public.lca_results
    where id = '98230000-0000-4000-8000-000000000202'
  ),
  'true',
  'publish pins the source lca_result'
);

select is(
  public.get_data_product_package_preview(
    p_package_id => (select id from data_product_test_ids where label = 'package')
  )->'data'->'summary'->>'packageId',
  (select id::text from data_product_test_ids where label = 'package'),
  'manager can preview unpublished or published package rows through RPC'
);

select set_config('request.jwt.claim.sub', '98230000-0000-4000-8000-000000000002', true);

select is(
  public.get_data_product_package_preview(
    p_package_id => (select id from data_product_test_ids where label = 'package')
  )->>'code',
  'not_data_product_manager',
  'ordinary users cannot preview package rows'
);

reset role;
set local role anon;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'anon', true);
select set_config('request.jwt.claims', '{"role":"anon"}', true);

select is(
  jsonb_array_length(
    public.get_published_process_lcia_results(
      p_process_id => '98230000-0000-4000-8000-000000000101',
      p_process_version => '01.00.000',
      p_impact_category_id => 'climate-change'
    )->'data'->'rows'
  )::text,
  '1',
  'anon public read returns current published process LCIA result rows'
);

select is(
  public.get_published_process_lcia_results(
    p_process_id => '98230000-0000-4000-8000-000000000103',
    p_process_version => '01.00.000',
    p_impact_category_id => 'climate-change'
  )->'data'->>'rowCount',
  '0',
  'public read returns an empty row set for processes outside the current package'
);

select is(
  (
    select count(*)::text
    from public.data_product_publications
    where publication_series_key = 'global'
      and publication_channel = 'public'
      and visibility_scope = 'public'
      and is_current
  ),
  '1',
  'partial unique current publication leaves exactly one current global public row'
);

select * from finish();

rollback;
