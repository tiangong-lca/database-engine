begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(19);

select is(
  (
    select worker_queue
    from public.worker_job_kinds
    where job_kind = 'review_submit.submit'
  ),
  'review_submit',
  'review_submit.submit root job kind is registered on the review_submit queue'
);

select has_function(
  'public',
  'worker_read_latest_job',
  array['uuid', 'text', 'uuid', 'text', 'text', 'text[]', 'boolean'],
  'worker_read_latest_job service projection exists'
);

select has_view(
  'public',
  'worker_job_domain_refs',
  'worker job domain reference projection exists'
);

select has_view(
  'public',
  'worker_legacy_lifecycle_audit',
  'legacy lifecycle audit projection exists'
);

with expected(table_name, column_name) as (
  values
    ('lca_jobs', 'worker_job_id'),
    ('lca_results', 'worker_job_id'),
    ('lca_result_cache', 'worker_job_id'),
    ('lca_latest_all_unit_results', 'worker_job_id'),
    ('lca_factorization_registry', 'prepared_worker_job_id'),
    ('lca_package_jobs', 'worker_job_id'),
    ('lca_package_artifacts', 'worker_job_id'),
    ('lca_package_export_items', 'worker_job_id'),
    ('lca_package_request_cache', 'worker_job_id'),
    ('dataset_review_submit_jobs', 'submit_worker_job_id'),
    ('dataset_review_submit_gate_runs', 'worker_job_id')
),
missing as (
  select expected.*
  from expected
  left join information_schema.columns as columns
    on columns.table_schema = 'public'
   and columns.table_name = expected.table_name
   and columns.column_name = expected.column_name
  where columns.column_name is null
)
select is(
  (select count(*)::text from missing),
  '0',
  'all retained domain/history tables expose canonical worker job reference columns'
);

with expected(table_name, column_name) as (
  values
    ('lca_jobs', 'worker_job_id'),
    ('lca_results', 'worker_job_id'),
    ('lca_result_cache', 'worker_job_id'),
    ('lca_latest_all_unit_results', 'worker_job_id'),
    ('lca_factorization_registry', 'prepared_worker_job_id'),
    ('lca_package_jobs', 'worker_job_id'),
    ('lca_package_artifacts', 'worker_job_id'),
    ('lca_package_export_items', 'worker_job_id'),
    ('lca_package_request_cache', 'worker_job_id'),
    ('dataset_review_submit_jobs', 'submit_worker_job_id'),
    ('dataset_review_submit_gate_runs', 'worker_job_id')
),
actual as (
  select
    rel.relname as table_name,
    att.attname as column_name
  from pg_constraint as con
  join pg_class as rel on rel.oid = con.conrelid
  join pg_namespace as rel_ns on rel_ns.oid = rel.relnamespace
  join pg_class as ref on ref.oid = con.confrelid
  join pg_namespace as ref_ns on ref_ns.oid = ref.relnamespace
  join pg_attribute as att on att.attrelid = rel.oid and att.attnum = any(con.conkey)
  where con.contype = 'f'
    and rel_ns.nspname = 'public'
    and ref_ns.nspname = 'public'
    and ref.relname = 'worker_jobs'
),
missing as (
  select expected.*
  from expected
  left join actual
    on actual.table_name = expected.table_name
   and actual.column_name = expected.column_name
  where actual.column_name is null
)
select is(
  (select count(*)::text from missing),
  '0',
  'all canonical worker job reference columns have foreign keys to worker_jobs'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '96000000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.worker_read_latest_job(uuid,text,uuid,text,text,text[],boolean)',
    'EXECUTE'
  ),
  'authenticated users cannot execute worker_read_latest_job directly'
);

select ok(
  not has_table_privilege('authenticated', 'public.worker_job_domain_refs', 'SELECT'),
  'authenticated users cannot read internal worker domain refs'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

create temporary table worker_canonical_test_ids (
  label text primary key,
  job_id uuid not null
) on commit drop;

grant all on worker_canonical_test_ids to public;

insert into worker_canonical_test_ids (label, job_id)
select
  'submit_root',
  (result->'data'->>'id')::uuid
from (
  select public.worker_enqueue_job(
    p_job_kind => 'review_submit.submit',
    p_payload_json => jsonb_build_object(
      'datasetRevision',
      jsonb_build_object(
        'table', 'processes',
        'id', '97000000-0000-4000-8000-000000000101',
        'version', '01.00.000',
        'revisionChecksum', repeat('a', 64)
      ),
      'policy',
      jsonb_build_object(
        'profile', 'review_submit_fast.v1',
        'reportSchemaVersion', 'review_submit_gate_report.v1'
      )
    ),
    p_requested_by => '97000000-0000-4000-8000-000000000001',
    p_idempotency_key => concat_ws(
      ':',
      'review_submit.submit',
      'processes',
      '97000000-0000-4000-8000-000000000101',
      '01.00.000',
      repeat('a', 64),
      'review_submit_fast.v1',
      'review_submit_gate_report.v1',
      '97000000-0000-4000-8000-000000000001'
    ),
    p_concurrency_key => 'review_submit.submit:processes:97000000-0000-4000-8000-000000000101:01.00.000:97000000-0000-4000-8000-000000000001',
    p_subject_type => 'processes',
    p_subject_id => '97000000-0000-4000-8000-000000000101',
    p_subject_version => '01.00.000'
  ) as result
) as enqueue;

select is(
  (
    select job_kind || ':' || worker_queue || ':' || status
    from public.worker_jobs
    where id = (select job_id from worker_canonical_test_ids where label = 'submit_root')
  ),
  'review_submit.submit:review_submit:queued',
  'review-submit root job enqueues as a canonical worker_jobs row'
);

select is(
  public.worker_read_latest_job(
    p_requested_by => '97000000-0000-4000-8000-000000000001',
    p_subject_type => 'processes',
    p_subject_id => '97000000-0000-4000-8000-000000000101',
    p_subject_version => '01.00.000',
    p_job_kind => 'review_submit.submit'
  )->'data'->>'id',
  (select job_id::text from worker_canonical_test_ids where label = 'submit_root'),
  'worker_read_latest_job returns the latest matching root job'
);

select is(
  public.worker_read_latest_job(
    p_requested_by => '97000000-0000-4000-8000-000000000001',
    p_subject_type => 'processes',
    p_subject_id => '97000000-0000-4000-8000-000000000102',
    p_subject_version => '01.00.000',
    p_job_kind => 'review_submit.submit'
  )->>'data',
  null,
  'worker_read_latest_job returns null data when no job matches'
);

select is(
  (
    select claimed->>'id'
    from jsonb_array_elements(
      public.worker_claim_jobs(
        p_worker_queue => 'review_submit',
        p_worker_id => 'worker-submit-test',
        p_limit => 1,
        p_lease_seconds => 300
      )->'data'
    ) as claimed
    limit 1
  ),
  (select job_id::text from worker_canonical_test_ids where label = 'submit_root'),
  'review_submit queue is claimable by worker_claim_jobs'
);

insert into public.dataset_review_submit_jobs (
  dataset_table,
  dataset_id,
  dataset_version,
  revision_checksum,
  policy_profile,
  report_schema_version,
  status,
  requested_by
) values (
  'processes',
  '97000000-0000-4000-8000-000000000101',
  '01.00.000',
  repeat('a', 64),
  'review_submit_fast.v1',
  'review_submit_gate_report.v1',
  'waiting_gate',
  '97000000-0000-4000-8000-000000000001'
);

select is(
  (
    select submit_worker_job_id::text
    from public.dataset_review_submit_jobs
    where dataset_id = '97000000-0000-4000-8000-000000000101'
      and dataset_version = '01.00.000'
    limit 1
  ),
  (select job_id::text from worker_canonical_test_ids where label = 'submit_root'),
  'review-submit coordinator rows automatically attach the canonical root worker job'
);

select is(
  (
    select count(*)::text
    from public.worker_job_domain_refs
    where worker_job_id = (select job_id from worker_canonical_test_ids where label = 'submit_root')
      and domain_source = 'dataset_review_submit_jobs'
      and domain_role = 'review_submit_coordinator'
  ),
  '1',
  'worker_job_domain_refs links root jobs to retained review-submit coordinator rows'
);

select is(
  (
    select active_count::text
    from public.worker_legacy_lifecycle_audit
    where legacy_source = 'dataset_review_submit_jobs'
      and task_family = 'review_submit.submit'
      and legacy_status = 'waiting_gate'
    limit 1
  ),
  '1',
  'legacy lifecycle audit includes active review-submit coordinator rows'
);

select is(
  (
    select public.cmd_dataset_review_submit_job_payload(j)->>'submitWorkerJobId'
    from public.dataset_review_submit_jobs as j
    where j.submit_worker_job_id = (select job_id from worker_canonical_test_ids where label = 'submit_root')
    limit 1
  ),
  (select job_id::text from worker_canonical_test_ids where label = 'submit_root'),
  'review-submit job payload exposes submitWorkerJobId'
);

select is(
  (
    select public.cmd_dataset_review_submit_job_payload(j)->'submitWorkerJob'->>'id'
    from public.dataset_review_submit_jobs as j
    where j.submit_worker_job_id = (select job_id from worker_canonical_test_ids where label = 'submit_root')
    limit 1
  ),
  (select job_id::text from worker_canonical_test_ids where label = 'submit_root'),
  'review-submit job payload includes the root worker job projection'
);

update public.dataset_review_submit_jobs
  set status = 'submitted',
      result = jsonb_build_object('reviewStateCode', 20)
where submit_worker_job_id = (select job_id from worker_canonical_test_ids where label = 'submit_root');

select is(
  (
    select status || ':' || phase || ':' || coalesce(progress::text, '')
    from public.worker_jobs
    where id = (select job_id from worker_canonical_test_ids where label = 'submit_root')
  ),
  'completed:submitted:1',
  'review-submit coordinator terminal status syncs to the canonical root worker job'
);

select ok(
  col_description('public.dataset_review_submit_jobs'::regclass, (
    select attnum
    from pg_attribute
    where attrelid = 'public.dataset_review_submit_jobs'::regclass
      and attname = 'submit_worker_job_id'
  )) is not null
  and col_description('public.lca_results'::regclass, (
    select attnum
    from pg_attribute
    where attrelid = 'public.lca_results'::regclass
      and attname = 'worker_job_id'
  )) is not null
  and obj_description('public.worker_job_domain_refs'::regclass, 'pg_class') is not null,
  'canonical worker job references are documented with comments'
);

select * from finish();
rollback;
