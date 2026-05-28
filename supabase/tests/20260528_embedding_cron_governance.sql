begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(6);

select ok(
  exists (select 1 from pg_extension where extname = 'pg_cron'),
  'pg_cron extension is available for cron governance assertions'
);

select is(
  (
    select count(*)::integer
    from cron.job
    where jobname = 'process-embeddings'
  ),
  1,
  'process-embeddings is scheduled exactly once'
);

select is(
  (
    select schedule
    from cron.job
    where jobname = 'process-embeddings'
  ),
  '10 seconds',
  'process-embeddings keeps the intended sub-minute schedule'
);

select is(
  (
    select active
    from cron.job
    where jobname = 'process-embeddings'
  ),
  true,
  'process-embeddings is active'
);

select ok(
  (
    select command like '%util.process_embeddings(%'
      and command like '%batch_size => 3%'
      and command like '%max_requests => 3%'
    from cron.job
    where jobname = 'process-embeddings'
  ),
  'process-embeddings uses explicit conservative dispatcher arguments'
);

select is(
  (
    select count(*)::integer
    from cron.job
    where jobname = 'process-webhook-jobs'
  ),
  0,
  'legacy process-webhook-jobs cron is not scheduled'
);

select * from finish();

rollback;
