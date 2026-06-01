begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(4);

select ok(
  strpos(
    pg_get_constraintdef(
      (
        select oid
        from pg_constraint
        where conrelid = 'public.unitgroups'::regclass
          and conname = 'unitgroups_state_code_check'
      )
    ),
    '20'
  ) > 0,
  'unitgroups state_code constraint allows under-review state 20'
);

select ok(
  strpos(
    pg_get_functiondef('util.invoke_edge_function(text,jsonb,integer)'::regprocedure),
    '''Authorization'''
  ) > 0
    and strpos(
      pg_get_functiondef('util.invoke_edge_function(text,jsonb,integer)'::regprocedure),
      '''Bearer '' || service_key'
    ) > 0,
  'database Edge invocations include Authorization bearer service key header'
);

select ok(
  to_regprocedure('util.process_dataset_review_submit_jobs(integer,integer,integer)') is not null,
  'review-submit coordinator cron wrapper exists'
);

select ok(
  case
    when exists (select 1 from pg_extension where extname = 'pg_cron') then
      exists (
        select 1
        from cron.job
        where jobname = 'process-dataset-review-submit-jobs'
          and command = 'select util.process_dataset_review_submit_jobs();'
      )
    else true
  end,
  'review-submit coordinator cron is scheduled when pg_cron is available'
);

select * from finish();
rollback;
