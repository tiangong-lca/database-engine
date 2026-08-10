begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(14);

set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

create temporary table issue_455_results (
  label text primary key,
  value jsonb not null
) on commit drop;
grant all on issue_455_results to service_role;

insert into issue_455_results values (
  'mutable_first',
  api.svc_tidas_package_export_enqueue(
    '45500000-0000-4000-8000-000000000001',
    'current_user',
    '[]'::jsonb,
    'issue455-mutable',
    '{"scope":"current_user","roots":[]}'::jsonb,
    '45510000-0000-4000-8000-000000000001',
    'issue455:mutable'
  )
);

insert into issue_455_results values (
  'mutable_active_duplicate',
  api.svc_tidas_package_export_enqueue(
    '45500000-0000-4000-8000-000000000001',
    'current_user',
    '[]'::jsonb,
    'issue455-mutable',
    '{"scope":"current_user","roots":[]}'::jsonb,
    '45510000-0000-4000-8000-000000000002',
    'issue455:mutable'
  )
);

select is(
  (select value ->> 'mode' from issue_455_results where label = 'mutable_first'),
  'queued',
  'the first mutable-scope export is queued'
);

select is(
  (select value ->> 'mode' from issue_455_results where label = 'mutable_active_duplicate'),
  'in_progress',
  'an active mutable-scope duplicate reuses current work'
);

select is(
  (select value ->> 'job_id' from issue_455_results where label = 'mutable_active_duplicate'),
  (select value ->> 'job_id' from issue_455_results where label = 'mutable_first'),
  'the active duplicate keeps the original package job identity'
);

select is(
  (select value ->> 'worker_job_id' from issue_455_results where label = 'mutable_active_duplicate'),
  (select value ->> 'worker_job_id' from issue_455_results where label = 'mutable_first'),
  'the active duplicate keeps the original Worker job identity'
);

update private.worker_jobs
set status = 'completed', progress = 1, finished_at = now(), updated_at = now()
where id = (
  select (value ->> 'worker_job_id')::uuid
  from issue_455_results
  where label = 'mutable_first'
);

update private.lca_package_request_cache
set status = 'ready', updated_at = now()
where requested_by = '45500000-0000-4000-8000-000000000001'
  and operation = 'export_package'
  and request_key = 'issue455-mutable';

insert into issue_455_results values (
  'mutable_after_terminal',
  api.svc_tidas_package_export_enqueue(
    '45500000-0000-4000-8000-000000000001',
    'current_user',
    '[]'::jsonb,
    'issue455-mutable',
    '{"scope":"current_user","roots":[]}'::jsonb,
    '45510000-0000-4000-8000-000000000003',
    'issue455:mutable'
  )
);

select is(
  (select value ->> 'mode' from issue_455_results where label = 'mutable_after_terminal'),
  'queued',
  'a new mutable-scope intent after completion queues fresh work'
);

select is(
  (select value ->> 'job_id' from issue_455_results where label = 'mutable_after_terminal'),
  '45510000-0000-4000-8000-000000000003',
  'the refreshed mutable export uses the newly requested package job identity'
);

select isnt(
  (select value ->> 'worker_job_id' from issue_455_results where label = 'mutable_after_terminal'),
  (select value ->> 'worker_job_id' from issue_455_results where label = 'mutable_first'),
  'the refreshed mutable export receives a fresh Worker job identity'
);

select is(
  (
    select count(*)
    from private.worker_jobs
    where requested_by = '45500000-0000-4000-8000-000000000001'
      and job_kind = 'tidas.export_package'
      and idempotency_key = 'issue455:mutable'
  ),
  2::bigint,
  'the unchanged idempotency key permits one fresh Worker job after completion'
);

select is(
  (
    select job_id::text || '|' || worker_job_id::text || '|' || status
    from private.lca_package_request_cache
    where requested_by = '45500000-0000-4000-8000-000000000001'
      and operation = 'export_package'
      and request_key = 'issue455-mutable'
  ),
  '45510000-0000-4000-8000-000000000003|'
    || (select value ->> 'worker_job_id' from issue_455_results where label = 'mutable_after_terminal')
    || '|pending',
  'the request cache advances to the fresh mutable export'
);

insert into issue_455_results values (
  'mutable_refreshed_active_duplicate',
  api.svc_tidas_package_export_enqueue(
    '45500000-0000-4000-8000-000000000001',
    'current_user',
    '[]'::jsonb,
    'issue455-mutable',
    '{"scope":"current_user","roots":[]}'::jsonb,
    '45510000-0000-4000-8000-000000000004',
    'issue455:mutable'
  )
);

select is(
  (
    select (value ->> 'mode') || '|' || (value ->> 'job_id') || '|' || (value ->> 'worker_job_id')
    from issue_455_results
    where label = 'mutable_refreshed_active_duplicate'
  ),
  'in_progress|45510000-0000-4000-8000-000000000003|'
    || (select value ->> 'worker_job_id' from issue_455_results where label = 'mutable_after_terminal'),
  'active dedupe continues to work after a mutable cache refresh'
);

insert into public.contacts (id, version, user_id, state_code, json)
values (
  '45520000-0000-4000-8000-000000000001',
  '01.00.001',
  '45500000-0000-4000-8000-000000000001',
  0,
  '{}'::jsonb
);

insert into issue_455_results values (
  'selected_first',
  api.svc_tidas_package_export_enqueue(
    '45500000-0000-4000-8000-000000000001',
    'selected_roots',
    '[{"table":"contacts","id":"45520000-0000-4000-8000-000000000001","version":"01.00.001"}]'::jsonb,
    'issue455-selected',
    '{}'::jsonb,
    '45510000-0000-4000-8000-000000000005',
    'issue455:selected'
  )
);

select is(
  (select value ->> 'mode' from issue_455_results where label = 'selected_first'),
  'queued',
  'the first exact selected-roots export is queued'
);

update private.worker_jobs
set status = 'completed', progress = 1, finished_at = now(), updated_at = now()
where id = (
  select (value ->> 'worker_job_id')::uuid
  from issue_455_results
  where label = 'selected_first'
);

update private.lca_package_request_cache
set status = 'ready', updated_at = now()
where requested_by = '45500000-0000-4000-8000-000000000001'
  and operation = 'export_package'
  and request_key = 'issue455-selected';

insert into issue_455_results values (
  'selected_after_terminal',
  api.svc_tidas_package_export_enqueue(
    '45500000-0000-4000-8000-000000000001',
    'selected_roots',
    '[{"table":"contacts","id":"45520000-0000-4000-8000-000000000001","version":"01.00.001"}]'::jsonb,
    'issue455-selected',
    '{}'::jsonb,
    '45510000-0000-4000-8000-000000000006',
    'issue455:selected'
  )
);

select is(
  (select value ->> 'mode' from issue_455_results where label = 'selected_after_terminal'),
  'cache_hit',
  'an exact selected-roots export retains terminal cache reuse'
);

select is(
  (
    select (value ->> 'job_id') || '|' || (value ->> 'worker_job_id')
    from issue_455_results
    where label = 'selected_after_terminal'
  ),
  '45510000-0000-4000-8000-000000000005|'
    || (select value ->> 'worker_job_id' from issue_455_results where label = 'selected_first'),
  'selected-roots terminal reuse returns the original canonical identities'
);

select is(
  (
    select count(*)
    from private.worker_jobs
    where requested_by = '45500000-0000-4000-8000-000000000001'
      and job_kind = 'tidas.export_package'
      and idempotency_key = 'issue455:selected'
  ),
  1::bigint,
  'selected-roots terminal reuse does not enqueue duplicate Worker work'
);

select * from finish();

rollback;
