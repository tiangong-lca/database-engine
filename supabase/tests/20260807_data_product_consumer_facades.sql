begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(20);

select is(
  (
    select count(*)
    from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname = any(array[
        'svc_data_product_publication_list',
        'svc_data_product_worker_metadata',
        'svc_data_product_current_public_package'
      ])
      and routine.prosecdef
      and routine.proconfig = array['search_path=""']::text[]
  ),
  3::bigint,
  'all data-product consumer facades use fixed-path SECURITY DEFINER'
);

select ok(
  has_function_privilege(
    'authenticated',
    'api.svc_data_product_publication_list(integer)',
    'EXECUTE'
  )
    and not has_function_privilege(
      'service_role',
      'api.svc_data_product_publication_list(integer)',
      'EXECUTE'
    ),
  'publication list is authenticated-only'
);

select ok(
  has_function_privilege(
    'service_role',
    'api.svc_data_product_worker_metadata(uuid[])',
    'EXECUTE'
  )
    and has_function_privilege(
      'service_role',
      'api.svc_data_product_current_public_package()',
      'EXECUTE'
    )
    and not has_function_privilege(
      'authenticated',
      'api.svc_data_product_worker_metadata(uuid[])',
      'EXECUTE'
    )
    and not has_function_privilege(
      'anon',
      'api.svc_data_product_current_public_package()',
      'EXECUTE'
    ),
  'metadata and current-public-package projections are service-only'
);

select is(
  (
    select count(*)
    from private.api_capability_grants
    where routine_identity = any(array[
      'api.svc_data_product_publication_list(integer)',
      'api.svc_data_product_worker_metadata(uuid[])',
      'api.svc_data_product_current_public_package()'
    ])
      and capability_id = 'EDGE-DATA-PRODUCT-01'
  ),
  3::bigint,
  'capability manifest records all three exact routine identities'
);

select ok(
  to_regclass('private.lcia_result_publications_list_idx') is not null,
  'publication list ordering has a supporting composite index'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password,
  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
  created_at, updated_at, is_sso_user, is_anonymous
) values
  (
    '00000000-0000-0000-0000-000000000000',
    '42270000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'issue-422-manager@example.com', 'test', now(),
    '{"provider":"email","providers":["email"]}',
    '{"email":"issue-422-manager@example.com","display_name":"Issue 422 Manager"}',
    now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '42270000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'issue-422-member@example.com', 'test', now(),
    '{"provider":"email","providers":["email"]}',
    '{"email":"issue-422-member@example.com","display_name":"Issue 422 Member"}',
    now(), now(), false, false
  );

insert into private.users (id, raw_user_meta_data) values
  (
    '42270000-0000-4000-8000-000000000001',
    '{"email":"issue-422-manager@example.com","display_name":"Issue 422 Manager"}'
  ),
  (
    '42270000-0000-4000-8000-000000000002',
    '{"email":"issue-422-member@example.com","display_name":"Issue 422 Member"}'
  );

insert into private.roles (user_id, team_id, role) values
  (
    '42270000-0000-4000-8000-000000000001',
    '00000000-0000-0000-0000-000000000000',
    'data_product_manager'
  );

insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  status, visibility, payload_schema_version, payload_json, diagnostics
) values (
  '42270000-0000-4000-8000-000000000101',
  'lcia_result.package_build',
  'calculator',
  'package',
  'user',
  '42270000-0000-4000-8000-000000000001',
  'completed',
  'operator',
  'lcia_result.package_build.request.v1',
  '{"name":"Issue 422 package"}',
  '{}'
);

insert into private.lca_network_snapshots (
  id, scope, process_filter, source_hash, status, created_by, created_at, updated_at
) values (
  '42270000-0000-4000-8000-000000000201',
  'data_product',
  '{}',
  'issue-422-data-product',
  'ready',
  '42270000-0000-4000-8000-000000000001',
  '2026-08-07T01:00:00Z',
  '2026-08-07T01:00:00Z'
);

insert into private.lca_results (
  id, job_id, snapshot_id, payload, diagnostics, artifact_url, artifact_format, created_at
) values (
  '42270000-0000-4000-8000-000000000301',
  '42270000-0000-4000-8000-000000000401',
  '42270000-0000-4000-8000-000000000201',
  '{}',
  '{}',
  's3://lca-results/issue-422-result.json',
  'json',
  '2026-08-07T01:05:00Z'
);

insert into private.lcia_result_packages (
  id, build_id, build_worker_job_id, package_version, coverage_mode,
  eligibility_resolved_at, eligible_input_count, included_input_count,
  input_manifest_hash, input_manifest, snapshot_id, result_id,
  result_artifact_ref, query_artifact_ref, artifact_manifest,
  available_impact_categories, default_impact_category, status, created_by,
  created_at, updated_at
) values (
  '42270000-0000-4000-8000-000000000501',
  '42270000-0000-4000-8000-000000000601',
  '42270000-0000-4000-8000-000000000101',
  'issue-422-v1',
  'global_eligible',
  '2026-08-07T01:10:00Z',
  3,
  2,
  repeat('a', 64),
  '{"processes":[]}',
  '42270000-0000-4000-8000-000000000201',
  '42270000-0000-4000-8000-000000000301',
  '{"artifactUrl":"s3://lca-results/result.json"}',
  '{"artifactUrl":"s3://lca-results/query.json"}',
  '{"schemaVersion":"v1"}',
  '[{"id":"climate-change"}]',
  'climate-change',
  'preview_ready',
  '42270000-0000-4000-8000-000000000001',
  '2026-08-07T01:10:00Z',
  '2026-08-07T01:10:00Z'
);

insert into private.lcia_result_publications (
  id, package_id, publication_series_key, publication_channel, visibility_scope,
  is_current, status, display_default_impact_category, published_by, published_at,
  created_at, updated_at
) values (
  '42270000-0000-4000-8000-000000000701',
  '42270000-0000-4000-8000-000000000501',
  'global',
  'public',
  'public',
  true,
  'current',
  'climate-change',
  '42270000-0000-4000-8000-000000000001',
  '2026-08-07T01:15:00Z',
  '2026-08-07T01:15:00Z',
  '2026-08-07T01:15:00Z'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config(
  'request.jwt.claim.sub',
  '42270000-0000-4000-8000-000000000002',
  true
);

select is(
  api.svc_data_product_publication_list(50) ->> 'code',
  'DATA_PRODUCT_MANAGER_REQUIRED',
  'non-manager cannot list publications'
);

select set_config(
  'request.jwt.claim.sub',
  '42270000-0000-4000-8000-000000000001',
  true
);

select is(
  api.svc_data_product_publication_list(50) #>> '{data,0,packageName}',
  'Issue 422 package',
  'manager publication list includes worker-owned package name'
);

select is(
  api.svc_data_product_publication_list(50) #>> '{data,0,packageVersion}',
  'issue-422-v1',
  'manager publication list includes package version'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
select set_config('request.jwt.claim.sub', '', true);

select is(
  api.svc_data_product_worker_metadata(
    array['42270000-0000-4000-8000-000000000101'::uuid]
  ) #>> '{worker_rows,0,payload_json,name}',
  'Issue 422 package',
  'worker metadata projection includes the allowlisted payload'
);

select is(
  api.svc_data_product_worker_metadata(
    array['42270000-0000-4000-8000-000000000101'::uuid]
  ) #>> '{package_rows,0,id}',
  '42270000-0000-4000-8000-000000000501',
  'worker metadata projection joins package metadata'
);

select is(
  api.svc_data_product_worker_metadata(array[]::uuid[]) -> 'worker_rows',
  '[]'::jsonb,
  'worker metadata projection handles an empty request'
);

select is(
  api.svc_data_product_worker_metadata(
    array_fill('42270000-0000-4000-8000-000000000101'::uuid, array[201])
  ) ->> 'code',
  'WORKER_JOB_LIMIT_EXCEEDED',
  'worker metadata projection enforces the bounded request size'
);

select is(
  api.svc_data_product_current_public_package() #>> '{data,publication,id}',
  '42270000-0000-4000-8000-000000000701',
  'current-public-package projection selects the active global publication'
);

select is(
  api.svc_data_product_current_public_package() #>> '{data,package,id}',
  '42270000-0000-4000-8000-000000000501',
  'current-public-package projection selects the preview-ready package'
);

select is(
  api.svc_data_product_current_public_package() #>> '{data,package,query_artifact_ref,artifactUrl}',
  's3://lca-results/query.json',
  'current-public-package projection includes query artifact metadata'
);

select is(
  api.svc_data_product_current_public_package() #>> '{data,package,default_impact_category}',
  'climate-change',
  'current-public-package projection includes impact metadata'
);

insert into private.lca_package_artifacts (
  id, job_id, artifact_kind, status, artifact_url, artifact_format,
  content_type, metadata, expires_at, created_at, updated_at
) values
  (
    '42270000-0000-4000-8000-000000000801',
    '42270000-0000-4000-8000-000000000901',
    'import_source',
    'deleted',
    's3://packages/deleted.zip',
    'tidas-package-zip:v1',
    'application/zip',
    '{"requested_by":"42270000-0000-4000-8000-000000000001"}',
    null,
    now(),
    now()
  ),
  (
    '42270000-0000-4000-8000-000000000802',
    '42270000-0000-4000-8000-000000000902',
    'import_source',
    'pending',
    's3://packages/expired.zip',
    'tidas-package-zip:v1',
    'application/zip',
    '{"requested_by":"42270000-0000-4000-8000-000000000001"}',
    now() - interval '1 minute',
    now(),
    now()
  );

select is(
  api.svc_tidas_package_import_enqueue(
    '42270000-0000-4000-8000-000000000001',
    '42270000-0000-4000-8000-000000000901',
    '42270000-0000-4000-8000-000000000801',
    repeat('d', 64),
    10,
    'deleted.zip',
    'application/zip'
  ) ->> 'code',
  'PACKAGE_ARTIFACT_DELETED',
  'TIDAS import facade preserves the deleted-artifact error contract'
);

select is(
  api.svc_tidas_package_import_enqueue(
    '42270000-0000-4000-8000-000000000001',
    '42270000-0000-4000-8000-000000000902',
    '42270000-0000-4000-8000-000000000802',
    repeat('e', 64),
    10,
    'expired.zip',
    'application/zip'
  ) ->> 'code',
  'PACKAGE_ARTIFACT_EXPIRED',
  'TIDAS import facade preserves the expired-artifact error contract'
);

select is(
  (
    select count(*)
    from information_schema.tables
    where table_schema = 'public'
      and table_name not in (
        'processes', 'flows', 'contacts', 'sources', 'unitgroups', 'flowproperties',
        'lciamethods', 'lifecyclemodels', 'ilcd'
      )
  ),
  0::bigint,
  'consumer facade closure does not regress the public table boundary'
);

select is(
  (
    select count(*)
    from information_schema.routines
    where routine_schema = 'public'
  ),
  0::bigint,
  'consumer facade closure does not recreate public routines'
);

select * from finish();

rollback;
