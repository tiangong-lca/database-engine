begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(49);

select is(
  (
    select count(*)
    from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname = any(array[
        'svc_lca_snapshot_candidates', 'svc_lca_snapshot_build_enqueue',
        'svc_lca_cached_job_enqueue', 'svc_lca_latest_all_unit_result',
        'svc_tidas_package_export_enqueue', 'svc_tidas_package_import_prepare',
        'svc_tidas_package_import_enqueue', 'svc_tidas_package_read'
      ])
      and routine.prosecdef
      and routine.proconfig = array['search_path=""']::text[]
  ),
  8::bigint,
  'all LCA and package capability facades use fixed-path SECURITY DEFINER'
);

select ok(
  has_function_privilege('service_role', 'api.svc_lca_snapshot_candidates(text,uuid,jsonb,integer)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_lca_snapshot_build_enqueue(text,jsonb,uuid,text,uuid,uuid,jsonb,text)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_lca_cached_job_enqueue(text,uuid,text,jsonb,text,uuid,jsonb,text,uuid,text,text)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_tidas_package_import_enqueue(uuid,uuid,uuid,text,bigint,text,text)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_tidas_package_read(uuid,uuid)', 'EXECUTE'),
  'service role can execute all capability families'
);

select ok(
  not has_function_privilege('anon', 'api.svc_lca_snapshot_candidates(text,uuid,jsonb,integer)', 'EXECUTE')
    and not has_function_privilege('authenticated', 'api.svc_lca_snapshot_candidates(text,uuid,jsonb,integer)', 'EXECUTE')
    and not has_function_privilege('anon', 'api.svc_tidas_package_read(uuid,uuid)', 'EXECUTE')
    and not has_function_privilege('authenticated', 'api.svc_tidas_package_read(uuid,uuid)', 'EXECUTE'),
  'browser roles cannot execute service capability facades'
);

select ok(
  to_regclass('private.lca_network_snapshots') is not null
    and to_regclass('private.lca_result_cache') is not null
    and to_regclass('private.lca_package_artifacts') is not null
    and to_regclass('api.lca_network_snapshots') is null,
  'capability facades do not expose internal relations'
);

select ok(
  to_regclass('private.lca_network_snapshots_ready_scope_created_idx') is not null
    and to_regclass('private.lca_package_import_prepare_idempotency_uk') is not null,
  'candidate lookup and import idempotency have supporting indexes'
);

create temporary table issue_422_results (
  label text primary key,
  value jsonb not null
) on commit drop;
grant all on issue_422_results to service_role;

set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  status, visibility, payload_schema_version, payload_json, diagnostics
) values (
  '42250000-0000-4000-8000-000000000202', 'lca.build_snapshot', 'calculator',
  'solver', 'user', '42250000-0000-4000-8000-000000000101', 'queued',
  'user', 'lca.build_snapshot.request.v2', '{}', '{}'
);
insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, root_job_id, requester_type,
  requested_by, status, visibility, payload_schema_version, payload_json, diagnostics
) values (
  '42250000-0000-4000-8000-000000000201', 'lca.build_snapshot', 'calculator',
  'solver', '42250000-0000-4000-8000-000000000202', 'user',
  '42250000-0000-4000-8000-000000000101', 'queued', 'user',
  'lca.build_snapshot.request.v2', '{}', '{}'
);

insert into issue_422_results values (
  'worker',
  api.svc_worker_enqueue_job(
    p_job_kind => 'lca.build_snapshot',
    p_payload_json => '{"fixture":true}',
    p_payload_schema_version => 'lca.build_snapshot.request.v2',
    p_subject_type => 'lca_snapshot',
    p_subject_id => '42250000-0000-4000-8000-000000000001',
    p_subject_version => 'full_library',
    p_requested_by => '42250000-0000-4000-8000-000000000101',
    p_requester_type => 'user',
    p_idempotency_key => 'issue422:worker-wrapper',
    p_concurrency_key => 'issue422:worker-wrapper',
    p_priority => 7,
    p_queue_key => 'issue422-queue',
    p_visibility => 'operator',
    p_payload_ref => '{"ref":"fixture"}',
    p_parent_job_id => '42250000-0000-4000-8000-000000000201',
    p_root_job_id => '42250000-0000-4000-8000-000000000202'
  )
);

select is((select value ->> 'ok' from issue_422_results where label = 'worker'), 'true', 'worker thin facade enqueues successfully');
select is(
  (
    select concat_ws('|', subject_type, subject_id, subject_version, requested_by, requester_type)
    from private.worker_jobs
    where id = (select (value #>> '{data,id}')::uuid from issue_422_results where label = 'worker')
  ),
  'lca_snapshot|42250000-0000-4000-8000-000000000001|full_library|42250000-0000-4000-8000-000000000101|user',
  'worker facade preserves subject and requester semantics'
);
select is(
  (
    select concat_ws('|', priority, queue_key, visibility, parent_job_id, root_job_id)
    from private.worker_jobs
    where id = (select (value #>> '{data,id}')::uuid from issue_422_results where label = 'worker')
  ),
  '7|issue422-queue|operator|42250000-0000-4000-8000-000000000201|42250000-0000-4000-8000-000000000202',
  'worker facade preserves queue, visibility, and parent/root ordering'
);

insert into private.lca_network_snapshots (
  id, scope, process_filter, source_hash, status, created_by, created_at, updated_at
) values
  ('42251000-0000-4000-8000-000000000001', 'full_library', '{"region":"CN"}', 'hash-old', 'ready', '42250000-0000-4000-8000-000000000101', '2026-08-01', '2026-08-01'),
  ('42251000-0000-4000-8000-000000000002', 'full_library', '{"region":"CN","year":2025}', 'hash-new', 'ready', '42250000-0000-4000-8000-000000000101', '2026-08-02', '2026-08-02');

insert into private.lca_snapshot_artifacts (
  id, snapshot_id, artifact_url, artifact_sha256, artifact_byte_size,
  artifact_format, process_count, flow_count, impact_count, a_nnz, b_nnz,
  c_nnz, status, created_at, updated_at
) values
  ('42252000-0000-4000-8000-000000000001', '42251000-0000-4000-8000-000000000001', 's3://snap/old', repeat('a',64), 10, 'npz', 1, 1, 1, 1, 1, 1, 'ready', '2026-08-01', '2026-08-01'),
  ('42252000-0000-4000-8000-000000000002', '42251000-0000-4000-8000-000000000002', 's3://snap/new', repeat('b',64), 20, 'npz', 2, 2, 2, 2, 2, 2, 'ready', '2026-08-02', '2026-08-02');

insert into private.lca_active_snapshots (scope, snapshot_id, source_hash)
values ('full_library', '42251000-0000-4000-8000-000000000001', 'hash-old');

select is(
  api.svc_lca_snapshot_candidates('full_library', null, '{"region":"CN"}', 100) #>> '{data,0,snapshotId}',
  '42251000-0000-4000-8000-000000000001',
  'snapshot candidates prioritize the active snapshot'
);
select is(
  jsonb_array_length(api.svc_lca_snapshot_candidates('full_library', null, '{"year":2025}', 100) -> 'data'),
  1,
  'snapshot candidates apply process-filter containment'
);
select is(
  api.svc_lca_snapshot_candidates('full_library', '42251000-0000-4000-8000-000000000002', null, 100) #>> '{data,0,artifact,artifactUrl}',
  's3://snap/new',
  'explicit snapshot lookup returns the ready artifact DTO'
);
select is(
  api.svc_lca_snapshot_candidates('invalid', null, null, 100) ->> 'code',
  'INVALID_LCA_SCOPE',
  'snapshot lookup rejects unknown scope'
);

insert into issue_422_results values (
  'snapshot_build_1',
  api.svc_lca_snapshot_build_enqueue(
    'full_library', '{"region":"EU"}', '42250000-0000-4000-8000-000000000101',
    'snapshot-build-key', '42251100-0000-4000-8000-000000000001',
    '42251000-0000-4000-8000-000000000003',
    '{"scope_manifest":{"schema_version":"fixture"}}', 'lca.build_snapshot.request.v2'
  )
);
insert into issue_422_results values (
  'snapshot_build_2',
  api.svc_lca_snapshot_build_enqueue(
    'full_library', '{"region":"EU"}', '42250000-0000-4000-8000-000000000101',
    'snapshot-build-key', '42251100-0000-4000-8000-000000000002',
    '42251000-0000-4000-8000-000000000004',
    '{"scope_manifest":{"schema_version":"fixture"}}', 'lca.build_snapshot.request.v2'
  )
);
select is((select value ->> 'mode' from issue_422_results where label = 'snapshot_build_1'), 'queued', 'first snapshot build is queued');
select is((select value ->> 'mode' from issue_422_results where label = 'snapshot_build_2'), 'in_progress', 'duplicate snapshot build reuses the active job');
select is(
  (select (value ->> 'job_id') || '|' || (value ->> 'snapshot_id') from issue_422_results where label = 'snapshot_build_2'),
  '42251100-0000-4000-8000-000000000001|42251000-0000-4000-8000-000000000003',
  'duplicate snapshot build returns the original logical job and snapshot identities'
);
select is(
  (
    select concat_ws('|', subject_type, subject_id, subject_version, payload_json ->> 'job_id', payload_json ->> 'snapshot_id')
    from private.worker_jobs
    where concurrency_key = 'lca.build_snapshot:full_library:snapshot-build-key'
  ),
  'lca_job|42251100-0000-4000-8000-000000000001|42251000-0000-4000-8000-000000000003|42251100-0000-4000-8000-000000000001|42251000-0000-4000-8000-000000000003',
  'snapshot build preserves the existing logical job subject and payload contract'
);
select is(
  (select count(*) from private.worker_jobs where concurrency_key = 'lca.build_snapshot:full_library:snapshot-build-key'),
  1::bigint,
  'snapshot build idempotency creates one worker job'
);
select is(
  (select count(*) from private.lca_network_snapshots where id in ('42251000-0000-4000-8000-000000000003','42251000-0000-4000-8000-000000000004')),
  1::bigint,
  'snapshot build idempotency creates one draft snapshot'
);

insert into issue_422_results values (
  'cached_1',
  api.svc_lca_cached_job_enqueue(
    'prod', '42251000-0000-4000-8000-000000000001', 'solve-key',
    '{"demand":{"process_index":1,"amount":1}}', 'lca.solve_one',
    '42253000-0000-4000-8000-000000000001', '{"demand":{"process_index":1}}',
    'lca.solve_one.request.v1', '42250000-0000-4000-8000-000000000101',
    'issue422:solve-key', 'solve'
  )
);
insert into issue_422_results values (
  'cached_2',
  api.svc_lca_cached_job_enqueue(
    'prod', '42251000-0000-4000-8000-000000000001', 'solve-key',
    '{"demand":{"process_index":1,"amount":1}}', 'lca.solve_one',
    '42253000-0000-4000-8000-000000000002', '{"demand":{"process_index":1}}',
    'lca.solve_one.request.v1', '42250000-0000-4000-8000-000000000101',
    'issue422:solve-key', 'solve'
  )
);
select is((select value ->> 'mode' from issue_422_results where label = 'cached_1'), 'queued', 'first cached LCA request is queued');
select is((select value ->> 'mode' from issue_422_results where label = 'cached_2'), 'in_progress', 'duplicate cached LCA request reuses active work');
select is(
  (select count(*) from private.lca_result_cache where scope='prod' and request_key='solve-key'),
  1::bigint,
  'cached LCA admission creates one cache row'
);

update private.worker_jobs
set status = 'blocked', blocker_codes = array['fixture_blocked'], resolution_scope = 'user'
where id = (select (value ->> 'worker_job_id')::uuid from issue_422_results where label = 'cached_1');
insert into issue_422_results values (
  'cached_blocked',
  api.svc_lca_cached_job_enqueue(
    'prod', '42251000-0000-4000-8000-000000000001', 'solve-key',
    '{"demand":{"process_index":1,"amount":1}}', 'lca.solve_one',
    '42253000-0000-4000-8000-000000000003', '{"demand":{"process_index":1}}',
    'lca.solve_one.request.v1', '42250000-0000-4000-8000-000000000101',
    'issue422:solve-key', 'solve'
  )
);
select is(
  (select (value ->> 'mode') || '|' || (value ->> 'job_id') from issue_422_results where label = 'cached_blocked'),
  'blocked|42253000-0000-4000-8000-000000000001',
  'blocked LCA idempotency replay returns the original logical job identity'
);
select is(
  (select job_id::text from private.lca_result_cache where scope='prod' and request_key='solve-key'),
  '42253000-0000-4000-8000-000000000001',
  'blocked LCA replay never writes an unqueued replacement job id into cache'
);
select is(
  (select status from private.lca_result_cache where scope='prod' and request_key='solve-key'),
  'failed',
  'blocked LCA replay persists a terminal cache status'
);

insert into issue_422_results values (
  'export_1',
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'current_user', '[]', 'export-key',
    '{"scope":"current_user","roots":[]}', '42254000-0000-4000-8000-000000000001',
    'issue422:export-key'
  )
);
insert into issue_422_results values (
  'export_2',
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'current_user', '[]', 'export-key',
    '{"scope":"current_user","roots":[]}', '42254000-0000-4000-8000-000000000002',
    'issue422:export-key'
  )
);
select is((select value ->> 'mode' from issue_422_results where label = 'export_1'), 'queued', 'first package export is queued');
select is((select value ->> 'mode' from issue_422_results where label = 'export_2'), 'in_progress', 'duplicate package export reuses active work');
select is(
  (select count(*) from private.lca_package_request_cache where requested_by='42250000-0000-4000-8000-000000000101' and request_key='export-key'),
  1::bigint,
  'package export creates one request cache row'
);
select is(
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000102', 'open_data', '[]', 'forbidden', '{}',
    '42254000-0000-4000-8000-000000000003', 'issue422:forbidden'
  ) ->> 'code',
  'EXPORT_SCOPE_FORBIDDEN',
  'open-data package export requires system admin'
);

update private.worker_jobs
set status = 'blocked', blocker_codes = array['fixture_blocked'], resolution_scope = 'user'
where id = (select (value ->> 'worker_job_id')::uuid from issue_422_results where label = 'export_1');
insert into issue_422_results values (
  'export_blocked',
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'current_user', '[]', 'export-key',
    '{"scope":"current_user","roots":[]}', '42254000-0000-4000-8000-000000000004',
    'issue422:export-key'
  )
);
select is(
  (select (value ->> 'mode') || '|' || (value ->> 'job_id') from issue_422_results where label = 'export_blocked'),
  'blocked|42254000-0000-4000-8000-000000000001',
  'blocked package idempotency replay returns the original logical job identity'
);
select is(
  (select job_id::text from private.lca_package_request_cache where requested_by='42250000-0000-4000-8000-000000000101' and request_key='export-key'),
  '42254000-0000-4000-8000-000000000001',
  'blocked package replay never writes an unqueued replacement job id into cache'
);
select is(
  (select status from private.lca_package_request_cache where requested_by='42250000-0000-4000-8000-000000000101' and request_key='export-key'),
  'failed',
  'blocked package replay persists a terminal cache status'
);

insert into public.contacts (id, version, user_id, state_code, json) values
  ('42257000-0000-4000-8000-000000000001', '01.00.001', '42250000-0000-4000-8000-000000000101', 0, '{}'),
  ('42257000-0000-4000-8000-000000000002', '01.00.001', '42250000-0000-4000-8000-000000000102', 100, '{}'),
  ('42257000-0000-4000-8000-000000000003', '01.00.001', '42250000-0000-4000-8000-000000000102', 0, '{}');
insert into issue_422_results values (
  'selected_roots',
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'selected_roots',
    '[{"table":"contacts","id":"42257000-0000-4000-8000-000000000001","version":"01.00.001"},{"table":"contacts","id":"42257000-0000-4000-8000-000000000002","version":"01.00.001"}]',
    'selected-roots-key', '{}', '42254000-0000-4000-8000-000000000005', 'issue422:selected-roots'
  )
);
select is(
  (select (value ->> 'mode') || '|' || (value ->> 'root_count') from issue_422_results where label = 'selected_roots'),
  'queued|2',
  'selected-roots export accepts only caller-owned and open datasets'
);
select is(
  (
    select jsonb_array_length(payload_json -> 'roots')
    from private.worker_jobs
    where id = (select (value ->> 'worker_job_id')::uuid from issue_422_results where label = 'selected_roots')
  ),
  2,
  'selected-roots worker payload uses the database-validated normalized roots'
);
select is(
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'selected_roots',
    '[{"table":"contacts","id":"42257000-0000-4000-8000-000000000003","version":"01.00.001"}]',
    'foreign-root-key', '{}', '42254000-0000-4000-8000-000000000006', 'issue422:foreign-root'
  ) ->> 'code',
  'ROOT_EXPORT_FORBIDDEN',
  'selected-roots export rejects another user draft dataset'
);
select is(
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'selected_roots',
    '[{"table":"roles","id":"42257000-0000-4000-8000-000000000001","version":"01.00.001"}]',
    'invalid-root-key', '{}', '42254000-0000-4000-8000-000000000007', 'issue422:invalid-root'
  ) ->> 'code',
  'INVALID_PACKAGE_ROOT',
  'selected-roots export rejects non-core relation identifiers'
);
select is(
  api.svc_tidas_package_export_enqueue(
    '42250000-0000-4000-8000-000000000101', 'selected_roots',
    '[{"table":"contacts","id":"42257000-0000-4000-8000-000000000001","version":"01.00.001"},{"table":"contacts","id":"42257000-0000-4000-8000-000000000001","version":"01.00.001"}]',
    'duplicate-root-key', '{}', '42254000-0000-4000-8000-000000000008', 'issue422:duplicate-root'
  ) ->> 'root_count',
  '1',
  'selected-roots export deterministically deduplicates roots before enqueue'
);

insert into issue_422_results values (
  'prepare_1',
  api.svc_tidas_package_import_prepare(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000001',
    '42256000-0000-4000-8000-000000000001', 's3://packages/source.zip',
    'application/zip', 'source.zip', 'issue422:prepare'
  )
);
insert into issue_422_results values (
  'prepare_2',
  api.svc_tidas_package_import_prepare(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000002',
    '42256000-0000-4000-8000-000000000002', 's3://packages/other.zip',
    'application/zip', 'other.zip', 'issue422:prepare'
  )
);
select is((select value ->> 'mode' from issue_422_results where label = 'prepare_1'), 'prepared', 'first import prepare creates an artifact');
select is((select value ->> 'mode' from issue_422_results where label = 'prepare_2'), 'reused', 'import prepare reuses the idempotency key');
select is(
  (select count(*) from private.lca_package_artifacts where metadata ->> 'import_prepare_idempotency_key' = 'issue422:prepare'),
  1::bigint,
  'import prepare idempotency is enforced in storage'
);
insert into issue_422_results values (
  'prepare_trimmed_1',
  api.svc_tidas_package_import_prepare(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000003',
    '42256000-0000-4000-8000-000000000003', 's3://packages/trimmed.zip',
    'application/zip', 'trimmed.zip', ' issue422:trimmed '
  )
);
insert into issue_422_results values (
  'prepare_trimmed_2',
  api.svc_tidas_package_import_prepare(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000004',
    '42256000-0000-4000-8000-000000000004', 's3://packages/trimmed-replay.zip',
    'application/zip', 'trimmed-replay.zip', 'issue422:trimmed'
  )
);
select is(
  (select value ->> 'mode' from issue_422_results where label = 'prepare_trimmed_2'),
  'reused',
  'import prepare normalizes idempotency before lock and lookup'
);
select is(
  (select count(*) from private.lca_package_artifacts where metadata ->> 'import_prepare_idempotency_key' = 'issue422:trimmed'),
  1::bigint,
  'normalized import idempotency stores exactly one artifact'
);

insert into issue_422_results values (
  'import_1',
  api.svc_tidas_package_import_enqueue(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000001',
    '42256000-0000-4000-8000-000000000001', repeat('c',64), 123,
    'source.zip', 'application/zip'
  )
);
insert into issue_422_results values (
  'import_2',
  api.svc_tidas_package_import_enqueue(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000001',
    '42256000-0000-4000-8000-000000000001', repeat('c',64), 123,
    'source.zip', 'application/zip'
  )
);
select is((select value ->> 'mode' from issue_422_results where label = 'import_1'), 'queued', 'first import enqueue is atomic and queued');
select is((select value ->> 'mode' from issue_422_results where label = 'import_2'), 'in_progress', 'duplicate import enqueue reuses active work');
select is(
  (select status || '|' || artifact_byte_size::text from private.lca_package_artifacts where id='42256000-0000-4000-8000-000000000001'),
  'ready|123',
  'import enqueue finalizes and links the artifact in one transaction'
);
select is(
  api.svc_tidas_package_import_enqueue(
    '42250000-0000-4000-8000-000000000102', '42255000-0000-4000-8000-000000000001',
    '42256000-0000-4000-8000-000000000001', repeat('c',64), 123,
    'source.zip', 'application/zip'
  ) ->> 'code',
  'PACKAGE_JOB_NOT_FOUND',
  'another requester cannot enqueue an owned import artifact'
);
select is(
  api.svc_tidas_package_read(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000001'
  ) #>> '{data,artifacts,0,artifactKind}',
  'import_source',
  'package read returns the owned artifact DTO'
);
select is(
  api.svc_tidas_package_read(
    '42250000-0000-4000-8000-000000000102', '42255000-0000-4000-8000-000000000001'
  ) -> 'data',
  'null'::jsonb,
  'package read does not reveal another requester job'
);
select is(
  api.svc_tidas_package_read(
    '42250000-0000-4000-8000-000000000101', '42255000-0000-4000-8000-000000000001'
  ) #>> '{data,artifacts,0,metadata,requested_by}',
  null::text,
  'package read removes internal requester metadata from artifact DTO'
);

reset role;

select is(
  (
    select count(*) from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname like 'svc_%'
      and exists (
        select 1 from aclexplode(coalesce(routine.proacl, acldefault('f', routine.proowner))) as acl
        where acl.grantee = 0 and acl.privilege_type = 'EXECUTE'
      )
  ),
  0::bigint,
  'no service facade inherits PUBLIC execute'
);

select * from finish();

rollback;
