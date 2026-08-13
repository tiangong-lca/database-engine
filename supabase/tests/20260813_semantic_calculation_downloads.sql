begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values (
  '00000000-0000-0000-0000-000000000000',
  '81300000-0000-4000-8000-000000000001',
  'authenticated', 'authenticated', 'semantic-downloads@example.com',
  'test-password-hash', now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"sub":"81300000-0000-4000-8000-000000000001"}'::jsonb,
  now(), now(), false, false
);

insert into private.users (id, raw_user_meta_data, contact)
values (
  '81300000-0000-4000-8000-000000000001',
  '{"email":"semantic-downloads@example.com"}'::jsonb,
  null
);
insert into private.teams (id, json, rank, is_public)
values ('00000000-0000-0000-0000-000000000000', '{"name":"System Team"}', 0, false)
on conflict (id) do nothing;
insert into private.roles (user_id, team_id, role)
values (
  '81300000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'data_product_manager'
);

insert into private.lca_network_snapshots (id, scope, status, created_by)
values (
  '81300000-0000-4000-8000-000000000010',
  'full_library', 'ready', '81300000-0000-4000-8000-000000000001'
);
insert into private.worker_jobs (
  id, job_kind, worker_queue, requester_type, requested_by,
  status, payload_schema_version, payload_json
) values
  (
    '81300000-0000-4000-8000-000000000020',
    'lca.solve_all_unit', 'solver', 'user',
    '81300000-0000-4000-8000-000000000001', 'completed',
    'lca.solve_all_unit.request.v1', '{}'
  ),
  (
    '81300000-0000-4000-8000-000000000021',
    'lca.solve_all_unit', 'solver', 'user',
    '81300000-0000-4000-8000-000000000001', 'completed',
    'lca.solve_all_unit.request.v1', '{}'
  ),
  (
    '81300000-0000-4000-8000-000000000022',
    'lca.solve_all_unit', 'solver', 'user',
    '81300000-0000-4000-8000-000000000001', 'completed',
    'lca.solve_all_unit.request.v1', '{}'
  ),
  (
    '81300000-0000-4000-8000-000000000023',
    'lca.solve_all_unit', 'solver', 'user',
    '81300000-0000-4000-8000-000000000001', 'completed',
    'lca.solve_all_unit.request.v1', '{}'
  );
insert into private.lca_results (
  id, job_id, snapshot_id, diagnostics, worker_job_id
) values
  (
    '81300000-0000-4000-8000-000000000030',
    '81300000-0000-4000-8000-000000000020',
    '81300000-0000-4000-8000-000000000010', '{}',
    '81300000-0000-4000-8000-000000000020'
  ),
  (
    '81300000-0000-4000-8000-000000000031',
    '81300000-0000-4000-8000-000000000021',
    '81300000-0000-4000-8000-000000000010', '{}',
    '81300000-0000-4000-8000-000000000021'
  ),
  (
    '81300000-0000-4000-8000-000000000032',
    '81300000-0000-4000-8000-000000000022',
    '81300000-0000-4000-8000-000000000010', '{}',
    '81300000-0000-4000-8000-000000000022'
  ),
  (
    '81300000-0000-4000-8000-000000000033',
    '81300000-0000-4000-8000-000000000023',
    '81300000-0000-4000-8000-000000000010', '{}',
    '81300000-0000-4000-8000-000000000023'
  );

create or replace function pg_temp.semantic_downloads()
returns jsonb
language sql
immutable
as $$
  select jsonb_agg(jsonb_build_object(
    'role', item.role,
    'group', item.group_name,
    'fileName', item.file_name,
    'schemaVersion', 'tiangong.calculation-download.v1',
    'mediaType', item.media_type,
    'artifactUrl', 's3://lca_results/downloads/' || item.file_name,
    'sha256', repeat('a', 64),
    'byteSize', 100,
    'recordCount', 10
  ) order by item.ordinal)
  from (values
    (1, 'lcia_results_xlsx', 'results', 'lcia-results.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
    (2, 'lcia_results_csv_zip', 'results', 'lcia-results.csv.zip', 'application/zip'),
    (3, 'lci_inventory_parquet', 'advanced_data', 'lci-inventory.parquet', 'application/vnd.apache.parquet'),
    (4, 'lci_inventory_csv_zip', 'advanced_data', 'lci-inventory-csv.zip', 'application/zip'),
    (5, 'calculation_evidence_bundle', 'audit_evidence', 'calculation-evidence-bundle.zip', 'application/zip')
  ) as item(ordinal, role, group_name, file_name, media_type)
$$;

insert into private.lcia_result_packages (
  id, build_id, build_worker_job_id, package_version, coverage_mode,
  eligibility_resolved_at, eligible_input_count, included_input_count,
  input_manifest_hash, input_manifest, snapshot_id, result_id,
  artifact_manifest, created_by
) values
  (
    '81300000-0000-4000-8000-000000000040',
    '81300000-0000-4000-8000-000000000050',
    '81300000-0000-4000-8000-000000000020',
    '01.00.000', 'global_eligible', now(), 1, 1, repeat('b', 64), '{}',
    '81300000-0000-4000-8000-000000000010',
    '81300000-0000-4000-8000-000000000030',
    jsonb_build_object(
      'calculationBundle', jsonb_build_object(
        'schemaVersion', 'tiangong.calculation-bundle.v2',
        'manifestUrl', 's3://lca_results/calculation-bundle.json',
        'downloads', pg_temp.semantic_downloads()
      )
    ),
    '81300000-0000-4000-8000-000000000001'
  ),
  (
    '81300000-0000-4000-8000-000000000041',
    '81300000-0000-4000-8000-000000000051',
    '81300000-0000-4000-8000-000000000021',
    '01.00.001', 'global_eligible', now(), 1, 1, repeat('c', 64), '{}',
    '81300000-0000-4000-8000-000000000010',
    '81300000-0000-4000-8000-000000000031',
    jsonb_build_object(
      'calculationBundle', jsonb_build_object(
        'schemaVersion', 'tiangong.calculation-bundle.v2',
        'manifestUrl', 's3://lca_results/legacy-calculation-bundle.json'
      )
    ),
    '81300000-0000-4000-8000-000000000001'
  ),
  (
    '81300000-0000-4000-8000-000000000042',
    '81300000-0000-4000-8000-000000000052',
    '81300000-0000-4000-8000-000000000022',
    '01.00.002', 'global_eligible', now(), 1, 1, repeat('d', 64), '{}',
    '81300000-0000-4000-8000-000000000010',
    '81300000-0000-4000-8000-000000000032',
    jsonb_build_object(
      'calculationBundle', jsonb_build_object(
        'schemaVersion', 'tiangong.calculation-bundle.v2',
        'manifestUrl', 's3://lca_results/invalid-media-calculation-bundle.json',
        'downloads', jsonb_set(
          pg_temp.semantic_downloads(), '{0,mediaType}', '"text/plain"'::jsonb
        )
      )
    ),
    '81300000-0000-4000-8000-000000000001'
  ),
  (
    '81300000-0000-4000-8000-000000000043',
    '81300000-0000-4000-8000-000000000053',
    '81300000-0000-4000-8000-000000000023',
    '01.00.003', 'global_eligible', now(), 1, 1, repeat('e', 64), '{}',
    '81300000-0000-4000-8000-000000000010',
    '81300000-0000-4000-8000-000000000033',
    jsonb_build_object(
      'calculationBundle', jsonb_build_object(
        'schemaVersion', 'tiangong.calculation-bundle.v2',
        'manifestUrl', 's3://lca_results/duplicate-role-calculation-bundle.json',
        'downloads', jsonb_set(
          pg_temp.semantic_downloads(), '{1}', pg_temp.semantic_downloads()->0
        )
      )
    ),
    '81300000-0000-4000-8000-000000000001'
  );

grant execute on function pg_temp.semantic_downloads() to authenticated;
set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config(
  'request.jwt.claim.sub', '81300000-0000-4000-8000-000000000001', true
);

select is(
  jsonb_array_length(
    api.get_lcia_result_calculation_bundle(
      '81300000-0000-4000-8000-000000000040'
    )->'data'->'productDownloads'
  ),
  5,
  'new packages project all five semantic download roles'
);
select ok(
  not (
    api.get_lcia_result_calculation_bundle(
      '81300000-0000-4000-8000-000000000040'
    )->'data'->'calculationBundle' ? 'downloads'
  ),
  'product download locators are separated from the returned bundle reference'
);
select is(
  jsonb_array_length(
    api.get_lcia_result_calculation_bundle(
      '81300000-0000-4000-8000-000000000041'
    )->'data'->'productDownloads'
  ),
  0,
  'legacy packages without semantic downloads remain readable'
);

select is(
  api.get_lcia_result_calculation_bundle(
    '81300000-0000-4000-8000-000000000042'
  )->>'code',
  'calculation_download_ref_invalid',
  'download metadata with a mismatched semantic media type is rejected'
);

select is(
  api.get_lcia_result_calculation_bundle(
    '81300000-0000-4000-8000-000000000043'
  )->>'code',
  'calculation_download_role_conflict',
  'duplicate semantic download roles are rejected'
);

select * from finish();
rollback;
