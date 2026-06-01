alter table public.unitgroups
  drop constraint if exists unitgroups_state_code_check;

alter table public.unitgroups
  add constraint unitgroups_state_code_check
  check (state_code in (0, 20, 100, 200));

create or replace function util.invoke_edge_function(
  name text,
  body jsonb,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
security definer
set search_path to ''
as $$
declare
  service_key text;
begin
  service_key := util.project_secret_key();

  perform net.http_post(
    url => util.project_url() || '/functions/v1/' || name,
    headers => jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || service_key,
      'apikey', service_key,
      'x_region', 'us-east-1'
    ),
    body => body,
    timeout_milliseconds => timeout_milliseconds
  );
end;
$$;

alter function util.invoke_edge_function(text, jsonb, integer) owner to postgres;

create or replace function util.process_dataset_review_submit_jobs(
  batch_size integer default 10,
  stale_submitting_seconds integer default 300,
  timeout_milliseconds integer default 60000
) returns void
language plpgsql
security definer
set search_path to ''
as $$
begin
  if coalesce(batch_size, 0) <= 0 then
    return;
  end if;

  if not pg_try_advisory_xact_lock(hashtext('util.process_dataset_review_submit_jobs')) then
    return;
  end if;

  perform util.invoke_edge_function(
    name => 'process_dataset_review_submit_jobs',
    body => jsonb_build_object(
      'batchSize', least(greatest(batch_size, 1), 50),
      'staleSubmittingSeconds', least(greatest(coalesce(stale_submitting_seconds, 300), 1), 3600)
    ),
    timeout_milliseconds => least(greatest(coalesce(timeout_milliseconds, 60000), 1000), 300000)
  );
end;
$$;

alter function util.process_dataset_review_submit_jobs(integer, integer, integer) owner to postgres;
revoke all on function util.process_dataset_review_submit_jobs(integer, integer, integer) from public;

do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('process-dataset-review-submit-jobs');
    exception when others then
      null;
    end;

    perform cron.schedule(
      'process-dataset-review-submit-jobs',
      '* * * * *',
      'select util.process_dataset_review_submit_jobs();'
    );
  end if;
end
$$;

comment on function util.process_dataset_review_submit_jobs(integer, integer, integer) is
  'Invokes the Edge review-submit coordinator that advances persisted dataset_review_submit_jobs after calculator worker gate results are available.';
