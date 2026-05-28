do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('process-webhook-jobs');
    exception when others then
      null;
    end;

    begin
      perform cron.unschedule('process-embeddings');
    exception when others then
      null;
    end;

    perform cron.schedule(
      'process-embeddings',
      '10 seconds',
      $cron$
        select util.process_embeddings(
          batch_size => 3,
          max_requests => 3,
          timeout_milliseconds => 300000
        );
      $cron$
    );
  end if;
end
$$;
