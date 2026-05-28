-- Keep markdown extraction webhooks in migration-managed form and remove the
-- retired webhook_jobs path that used to be driven by embedding_flag updates.

drop trigger if exists flow_extract_md_trigger_update on public.flows;
drop trigger if exists lifecyclemodel_extract_md_trigger_insert on public.lifecyclemodels;
drop trigger if exists lifecyclemodel_extract_md_trigger_update on public.lifecyclemodels;
drop trigger if exists process_extract_md_trigger_insert on public.processes;
drop trigger if exists process_extract_md_trigger_update on public.processes;

create trigger flow_extract_md_trigger_update
after update of json on public.flows
for each row
when (new.json is distinct from old.json)
execute function util.invoke_edge_webhook('webhook_flow_embedding_ft', '1000');

create trigger lifecyclemodel_extract_md_trigger_insert
after insert on public.lifecyclemodels
for each row
execute function util.invoke_edge_webhook('webhook_model_embedding_ft', '1000');

create trigger lifecyclemodel_extract_md_trigger_update
after update of json on public.lifecyclemodels
for each row
when (new.json is distinct from old.json)
execute function util.invoke_edge_webhook('webhook_model_embedding_ft', '1000');

create trigger process_extract_md_trigger_insert
after insert on public.processes
for each row
execute function util.invoke_edge_webhook('webhook_process_embedding_ft', '1000');

create trigger process_extract_md_trigger_update
after update of json on public.processes
for each row
when (new.json is distinct from old.json)
execute function util.invoke_edge_webhook('webhook_process_embedding_ft', '1000');

-- Text extraction is now database-side extracted_text sync. Keep the old
-- non-*_ft Edge webhook path explicitly removed.
drop trigger if exists flow_extract_text_trigger_insert on public.flows;
drop trigger if exists flow_extract_text_trigger_update on public.flows;
drop trigger if exists process_extract_text_trigger_insert on public.processes;
drop trigger if exists process_extract_text_trigger_update on public.processes;
drop trigger if exists lifecyclemodels_extract_text_trigger_insert on public.lifecyclemodels;
drop trigger if exists lifecyclemodels_extract_text_trigger_update on public.lifecyclemodels;

-- The old embedding_flag path wrote to pgmq.webhook_jobs. The worker cron is
-- retired, and markdown/embedding updates now use dataset_extraction_jobs and
-- embedding_jobs instead.
drop trigger if exists flow_extract_md_trigger_update_flag on public.flows;
drop trigger if exists lifecyclemodel_extract_md_trigger_update_flag on public.lifecyclemodels;
drop trigger if exists process_extract_md_trigger_update_flag on public.processes;

drop function if exists util.queue_embedding_webhook();
drop function if exists util.process_webhook_jobs(integer, integer, integer);

do $$
begin
  if to_regclass('pgmq.q_webhook_jobs') is not null
      or to_regclass('pgmq.a_webhook_jobs') is not null then
    if to_regprocedure('pgmq.drop_queue(text)') is not null then
      perform pgmq.drop_queue('webhook_jobs');
    elsif to_regprocedure('pgmq.drop_queue(text, boolean)') is not null then
      perform pgmq.drop_queue('webhook_jobs', true);
    end if;
  end if;
end
$$;

do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('process-dataset-extraction-jobs');
    exception when others then
      null;
    end;

    perform cron.schedule(
      'process-dataset-extraction-jobs',
      '10 seconds',
      $cron$
        select util.process_dataset_extraction_jobs(
          batch_size => 5,
          visibility_timeout_seconds => 300,
          max_read_count => 5,
          timeout_milliseconds => 300000
        );
      $cron$
    );
  end if;
end
$$;
