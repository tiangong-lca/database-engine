begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.has_empty_search_path(p_signature text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_proc p
    cross join lateral unnest(coalesce(p.proconfig, array[]::text[])) as config(setting)
    where p.oid = p_signature::regprocedure
      and config.setting in ('search_path=', 'search_path=""')
  );
$$;

select plan(24);

select ok(
  to_regclass('supabase_functions.hooks') is not null,
  'supabase_functions.hooks audit table exists'
);

select ok(
  exists (
    select 1
    from pg_indexes
    where schemaname = 'supabase_functions'
      and tablename = 'hooks'
      and indexname = 'supabase_functions_hooks_created_at_idx'
  ),
  'supabase_functions hooks retention has a created_at index'
);

select ok(
  to_regprocedure('util.preview_supabase_functions_hooks_retention(interval,timestamp with time zone)') is not null,
  'supabase functions hooks retention preview function exists'
);

select ok(
  to_regprocedure('util.purge_supabase_functions_hooks(interval,integer)') is not null,
  'supabase functions hooks retention purge function exists'
);

select ok(
  has_function_privilege(
    'service_role',
    'util.preview_supabase_functions_hooks_retention(interval,timestamp with time zone)',
    'EXECUTE'
  ),
  'service_role can execute supabase functions hooks retention preview'
);

select is(
  (
    select count(*)::integer
    from cron.job
    where jobname = 'purge-supabase-functions-hooks'
  ),
  1,
  'purge-supabase-functions-hooks is scheduled exactly once'
);

select is(
  (
    select schedule
    from cron.job
    where jobname = 'purge-supabase-functions-hooks'
  ),
  '47 3 * * *',
  'purge-supabase-functions-hooks uses the daily low-frequency cadence'
);

select is(
  (
    select active
    from cron.job
    where jobname = 'purge-supabase-functions-hooks'
  ),
  true,
  'purge-supabase-functions-hooks is active'
);

select ok(
  (
    select command like '%util.purge_supabase_functions_hooks%'
      and command like '%14 days%'
      and command like '%50000%'
    from cron.job
    where jobname = 'purge-supabase-functions-hooks'
  ),
  'purge-supabase-functions-hooks uses the 14 day retention window and bounded batch size'
);

delete from supabase_functions.hooks
where hook_name like 'pgtap_hooks_retention_%';

delete from net._http_response
where id in (-9151013);

insert into net._http_response (
  id,
  status_code,
  content_type,
  headers,
  content,
  timed_out,
  error_msg,
  created
) values (
  -9151013,
  200,
  'application/json',
  '{}'::jsonb,
  '{}',
  false,
  null,
  pg_catalog.now()
);

insert into supabase_functions.hooks (
  hook_table_id,
  hook_name,
  created_at,
  request_id
) values
  (
    9151001,
    'pgtap_hooks_retention_old_eligible_1',
    timestamp with time zone '2000-01-01 00:00:00+00',
    -9151011
  ),
  (
    9151001,
    'pgtap_hooks_retention_old_eligible_2',
    timestamp with time zone '2000-01-02 00:00:00+00',
    -9151012
  ),
  (
    9151001,
    'pgtap_hooks_retention_old_live_response',
    timestamp with time zone '2000-01-03 00:00:00+00',
    -9151013
  ),
  (
    9151001,
    'pgtap_hooks_retention_recent',
    pg_catalog.now() - interval '1 day',
    -9151014
  );

create temporary table pg_temp.hooks_retention_preview as
select *
from util.preview_supabase_functions_hooks_retention(
  interval '14 days',
  pg_catalog.now()
);

select ok(
  (select total_rows >= 4 from pg_temp.hooks_retention_preview),
  'preview reports at least the inserted hooks audit rows'
);

select ok(
  (select eligible_rows >= 2 from pg_temp.hooks_retention_preview),
  'preview reports old hooks without live pg_net responses as eligible'
);

select ok(
  (select protected_recent_rows >= 1 from pg_temp.hooks_retention_preview),
  'preview protects hooks inside the retention window'
);

select ok(
  (select protected_live_response_rows >= 1 from pg_temp.hooks_retention_preview),
  'preview protects old hooks still linked to live pg_net responses'
);

create temporary table pg_temp.hooks_retention_first_purge as
select util.purge_supabase_functions_hooks(interval '14 days', 1) as deleted_rows;

select is(
  (select deleted_rows from pg_temp.hooks_retention_first_purge),
  1::bigint,
  'purge respects the requested batch size'
);

select is(
  (
    select count(*)::integer
    from supabase_functions.hooks
    where hook_name = 'pgtap_hooks_retention_old_eligible_2'
  ),
  1,
  'first bounded purge leaves the later old eligible row for the next batch'
);

create temporary table pg_temp.hooks_retention_second_purge as
select util.purge_supabase_functions_hooks(interval '14 days', 100) as deleted_rows;

select ok(
  (select deleted_rows >= 1 from pg_temp.hooks_retention_second_purge),
  'second purge deletes remaining old eligible hooks'
);

select is(
  (
    select count(*)::integer
    from supabase_functions.hooks
    where hook_name like 'pgtap_hooks_retention_old_eligible_%'
  ),
  0,
  'purge deletes old hooks without live pg_net responses'
);

select is(
  (
    select count(*)::integer
    from supabase_functions.hooks
    where hook_name = 'pgtap_hooks_retention_old_live_response'
  ),
  1,
  'purge keeps old hooks still linked to live pg_net responses'
);

select is(
  (
    select count(*)::integer
    from supabase_functions.hooks
    where hook_name = 'pgtap_hooks_retention_recent'
  ),
  1,
  'purge keeps hooks inside the retention window'
);

create temporary table pg_temp.hooks_retention_error_check (
  case_name text primary key,
  raised boolean not null
);

do $$
begin
  perform util.purge_supabase_functions_hooks(interval '12 hours', 100);
  insert into pg_temp.hooks_retention_error_check values ('purge-window', false);
exception when invalid_parameter_value then
  insert into pg_temp.hooks_retention_error_check values ('purge-window', true);
end
$$;

do $$
begin
  perform util.purge_supabase_functions_hooks(interval '14 days', 0);
  insert into pg_temp.hooks_retention_error_check values ('purge-batch', false);
exception when invalid_parameter_value then
  insert into pg_temp.hooks_retention_error_check values ('purge-batch', true);
end
$$;

do $$
begin
  perform util.preview_supabase_functions_hooks_retention(interval '14 days', null);
  insert into pg_temp.hooks_retention_error_check values ('preview-as-of', false);
exception when invalid_parameter_value then
  insert into pg_temp.hooks_retention_error_check values ('preview-as-of', true);
end
$$;

select is(
  (select raised from pg_temp.hooks_retention_error_check where case_name = 'purge-window'),
  true,
  'purge rejects retention windows shorter than one day'
);

select is(
  (select raised from pg_temp.hooks_retention_error_check where case_name = 'purge-batch'),
  true,
  'purge rejects invalid batch sizes'
);

select is(
  (select raised from pg_temp.hooks_retention_error_check where case_name = 'preview-as-of'),
  true,
  'preview rejects null as_of timestamps'
);

select ok(
  pg_temp.has_empty_search_path('util.preview_supabase_functions_hooks_retention(interval,timestamp with time zone)'),
  'supabase functions hooks retention preview function pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.purge_supabase_functions_hooks(interval,integer)'),
  'supabase functions hooks retention purge function pins an empty search_path'
);

select * from finish();

rollback;
