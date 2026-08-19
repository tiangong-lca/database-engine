begin;

select plan(26);

select has_table('private', 'dataset_review_submit_requests', 'review-submit request history is retained');
select has_table('private', 'dataset_review_submit_gate_runs', 'review-submit Gate history is retained');
select has_table('private', 'worker_jobs', 'canonical worker-job history is retained');

select has_function('api', 'cmd_review_submit', array['text', 'uuid', 'text', 'jsonb'], 'stable direct review submit remains');
select has_function('api', 'cmd_review_quality_diagnostic_start', array[]::text[], 'Review Admin quality diagnostic remains');

select hasnt_function('util', 'process_dataset_review_submit_jobs', array['integer', 'integer', 'integer'], 'coordinator cron helper is removed');
select hasnt_function('api', 'cmd_dataset_assert_review_submit_gate_passed', 'Gate assertion RPC is removed');
select hasnt_function('api', 'cmd_dataset_review_submit_gate', 'Gate RPC is removed');
select hasnt_function('api', 'cmd_dataset_review_submit_job_enqueue', 'coordinator enqueue RPC is removed');
select hasnt_function('api', 'cmd_dataset_review_submit_job_read', 'coordinator read RPC is removed');
select hasnt_function('api', 'cmd_dataset_review_submit_job_read_latest', 'coordinator latest-read RPC is removed');
select hasnt_function('api', 'svc_dataset_review_submit_job_claim', 'coordinator service claim facade is removed');
select hasnt_function('api', 'svc_dataset_review_submit_job_record_result', 'coordinator service result facade is removed');
select hasnt_function('api', 'svc_review_submit_from_job', 'job-backed submit facade is removed');
select hasnt_function('api', 'cmd_review_submit_v2', 'Gate compatibility submit RPC is removed');
select hasnt_function('api', 'cmd_review_submit_without_gate', 'legacy alternate submit RPC is removed');

select hasnt_function('private', 'cmd_dataset_review_submit_job_claim', 'private coordinator claim is removed');
select hasnt_function('private', 'cmd_dataset_review_submit_job_record_result', 'private coordinator result recorder is removed');
select hasnt_function('private', 'cmd_dataset_review_submit_gate_record_result', 'private Gate result recorder is removed');
select hasnt_function('private', 'cmd_dataset_review_submit_gate_enqueue_worker_job', 'private Gate enqueue is removed');
select hasnt_function('private', 'cmd_review_submit_from_job', 'private job-backed submit is removed');
select hasnt_function('private', 'dataset_review_submit_requests_assign_submit_worker_job', 'request-to-worker assignment trigger function is removed');
select hasnt_function('private', 'dataset_review_submit_requests_sync_submit_worker_job', 'request-to-worker sync trigger function is removed');

select is(
  (select count(*)::integer from private.worker_job_kinds where job_kind in ('review_submit.gate', 'review_submit.submit')),
  2,
  'historically referenced review-submit job kinds remain registered'
);

select is(
  (select count(*)::integer from pg_trigger where tgrelid = 'private.dataset_review_submit_requests'::regclass and not tgisinternal),
  0,
  'review-submit request history has no runtime triggers'
);

select ok(
  not exists (
    select 1
    from cron.job
    where jobname = 'process-dataset-review-submit-jobs'
  ),
  'review-submit coordinator cron is unscheduled'
);

select * from finish();

rollback;
