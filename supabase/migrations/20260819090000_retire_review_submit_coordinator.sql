begin;

set local lock_timeout = '10s';
set local statement_timeout = '5min';

-- Stop new coordinator invocations before removing any callable surface.
do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('process-dataset-review-submit-jobs');
    exception
      when others then
        null;
    end;
  end if;
end
$$;

drop trigger if exists dataset_review_submit_requests_assign_submit_worker_job_trigger
  on private.dataset_review_submit_requests;
drop trigger if exists dataset_review_submit_requests_sync_submit_worker_job_trigger
  on private.dataset_review_submit_requests;

-- Remove public and service facades first so internal implementations have no callers.
drop function if exists api.cmd_dataset_assert_review_submit_gate_passed(text, uuid, text, uuid, text, text, text);
drop function if exists api.cmd_dataset_review_submit_gate(text, uuid, text, text, text, text, text, uuid, jsonb);
drop function if exists api.cmd_dataset_review_submit_job_enqueue(text, uuid, text, text, text, text, jsonb);
drop function if exists api.cmd_dataset_review_submit_job_read(uuid);
drop function if exists api.cmd_dataset_review_submit_job_read_latest(text, uuid, text, text);
drop function if exists api.svc_dataset_review_submit_job_claim(integer, integer);
drop function if exists api.svc_dataset_review_submit_job_record_result(uuid, text, uuid, jsonb, text, text, jsonb, jsonb);
drop function if exists api.svc_review_submit_from_job(uuid, jsonb);
drop function if exists api.cmd_review_submit_without_gate(text, uuid, text, jsonb);
drop function if exists api.cmd_review_submit_v2(text, uuid, text, jsonb, jsonb);

-- Remove coordinator, Gate execution, and compatibility implementations.
drop function if exists private.cmd_review_submit_from_job(uuid, jsonb);
drop function if exists private.cmd_dataset_review_submit_job_record_result(uuid, text, uuid, jsonb, text, text, jsonb, jsonb);
drop function if exists private.cmd_dataset_review_submit_job_claim(integer, integer);
drop function if exists private.cmd_dataset_review_submit_job_payload(anyelement);
drop function if exists private.cmd_dataset_review_submit_gate_record_result(uuid, text, jsonb, jsonb, text, jsonb);
drop function if exists private.cmd_dataset_review_submit_gate_link_worker_job(uuid, text);
drop function if exists private.cmd_dataset_review_submit_gate_enqueue_worker_job(text, uuid, text, text, text, text, uuid, uuid, text);
drop function if exists private.cmd_dataset_review_submit_gate_payload(private.dataset_review_submit_gate_runs, text);
drop function if exists private.dataset_review_submit_requests_assign_submit_worker_job();
drop function if exists private.dataset_review_submit_requests_sync_submit_worker_job();
drop function if exists private.cmd_review_submit_without_gate_pre_v2(text, uuid, text, jsonb);
drop function if exists private.cmd_review_submit_without_gate_issue304_legacy(text, uuid, text, jsonb);
drop function if exists util.process_dataset_review_submit_jobs(integer, integer, integer);

delete from private.api_capability_grants
where routine_identity ~ '^(api[.])?(cmd_dataset_assert_review_submit_gate_passed|cmd_dataset_review_submit_gate|cmd_dataset_review_submit_job_enqueue|cmd_dataset_review_submit_job_read|cmd_dataset_review_submit_job_read_latest|cmd_review_submit_v2|cmd_review_submit_without_gate|svc_dataset_review_submit_job_claim|svc_dataset_review_submit_job_record_result|svc_review_submit_from_job)[(]';

comment on table private.dataset_review_submit_requests is
  'Retained terminal review-submit coordinator history. No runtime enqueues, claims, retries, or state-sync triggers remain.';
comment on table private.dataset_review_submit_gate_runs is
  'Retained numerical Gate history from the retired review-submit coordinator. No runtime Gate execution remains.';

commit;
