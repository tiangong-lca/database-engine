begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(15);

select ok(
  to_regclass('pgmq.q_embedding_jobs') is not null,
  'embedding_jobs queue exists from migration history'
);

do $$
begin
  perform pgmq.create('embedding_jobs');
exception when others then
  if to_regclass('pgmq.q_embedding_jobs') is null then
    raise;
  end if;
end
$$;

delete from pgmq.q_embedding_jobs;
delete from util.embedding_job_failures;
delete from util.pending_embedding_jobs;

create temporary table embedding_invocations (
  name text not null,
  body jsonb not null,
  timeout_milliseconds integer not null
) on commit drop;

create or replace function util.invoke_edge_function(
  name text,
  body jsonb,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
security definer
set search_path to 'pg_temp'
as $$
begin
  insert into embedding_invocations (name, body, timeout_milliseconds)
  values ($1, $2, $3);
end;
$$;

select is(
  (select max_in_flight
   from util.embedding_queue_policy_for(
     'public',
     'flows',
     'embedding_ft',
     'embedding_ft'
   )),
  2,
  'flows.embedding_ft has a conservative default in-flight cap'
);

update util.embedding_queue_policy
set max_in_flight = 1
where scope_schema = 'public'
  and scope_table = 'flows'
  and scope_edge_function = 'embedding_ft'
  and scope_embedding_column = 'embedding_ft';

select pgmq.send(
  queue_name => 'embedding_jobs',
  msg => jsonb_build_object(
    'id', id_value,
    'version', '01.00.000',
    'schema', 'public',
    'table', 'flows',
    'contentFunction', 'flows_embedding_ft_input',
    'embeddingColumn', 'embedding_ft',
    'edgeFunction', 'embedding_ft'
  )
)
from (
  values
    ('00000000-0000-0000-0000-000000000101'::uuid),
    ('00000000-0000-0000-0000-000000000102'::uuid),
    ('00000000-0000-0000-0000-000000000103'::uuid)
) as jobs(id_value);

select util.process_embeddings(10, 10, 300000);

select is(
  (select count(*)::integer from embedding_invocations),
  1,
  'dispatcher invokes one batch when scoped capacity is available'
);

select is(
  (select jsonb_array_length(body) from embedding_invocations limit 1),
  1,
  'dispatcher only claims one flows.embedding_ft job when cap is one'
);

select is(
  (
    select count(*)::integer
    from pgmq.q_embedding_jobs
    where vt > clock_timestamp()
  ),
  1,
  'one embedding job is marked in-flight after dispatch'
);

select util.process_embeddings(10, 10, 300000);

select is(
  (select count(*)::integer from embedding_invocations),
  1,
  'dispatcher does not read more jobs while scoped in-flight cap is saturated'
);

select is(
  (
    select count(*)::integer
    from pgmq.q_embedding_jobs
    where vt <= clock_timestamp()
  ),
  2,
  'remaining visible jobs stay queued instead of being claimed over cap'
);

delete from pgmq.q_embedding_jobs;
delete from embedding_invocations;

select pgmq.send(
  queue_name => 'embedding_jobs',
  msg => jsonb_build_object(
    'id', '00000000-0000-0000-0000-000000000201'::uuid,
    'version', '01.00.000',
    'schema', 'public',
    'table', 'flows',
    'contentFunction', 'flows_embedding_ft_input',
    'embeddingColumn', 'embedding_ft',
    'edgeFunction', 'embedding_ft'
  )
);

update pgmq.q_embedding_jobs
set
  read_ct = 20,
  vt = clock_timestamp() - interval '1 second';

select util.process_embeddings(10, 10, 300000);

select is(
  (select count(*)::integer from util.embedding_job_failures),
  1,
  'retry-capped visible jobs are recorded in the failure table'
);

select is(
  (select count(*)::integer from pgmq.q_embedding_jobs),
  0,
  'retry-capped jobs are removed from the live queue'
);

select is(
  (select count(*)::integer from embedding_invocations),
  0,
  'retry-capped jobs are not dispatched to the Edge Function'
);

delete from util.embedding_job_failures;
delete from util.pending_embedding_jobs;
delete from pgmq.q_embedding_jobs;

create temporary table embedding_queue_trigger_rows (
  id uuid not null,
  version text not null,
  extracted_md text
) on commit drop;

create trigger embedding_queue_trigger_rows_queue_embeddings
after update of extracted_md on embedding_queue_trigger_rows
for each row
execute function util.queue_embeddings(
  'test_embedding_input',
  'embedding_ft',
  'embedding_ft'
);

insert into util.embedding_queue_policy (
  scope_schema,
  scope_table,
  scope_edge_function,
  scope_embedding_column,
  mode,
  max_in_flight,
  max_read_count,
  retry_backoff_seconds
)
values (
  '*',
  'embedding_queue_trigger_rows',
  'embedding_ft',
  'embedding_ft',
  'deferred',
  1,
  20,
  300
);

insert into embedding_queue_trigger_rows (id, version, extracted_md)
values (
  '00000000-0000-0000-0000-000000000301',
  '01.00.000',
  'old content'
);

update embedding_queue_trigger_rows
set extracted_md = 'new content'
where id = '00000000-0000-0000-0000-000000000301';

select is(
  (select count(*)::integer from util.pending_embedding_jobs where status = 'pending'),
  1,
  'deferred queue policy records pending embedding work'
);

select is(
  (select count(*)::integer from pgmq.q_embedding_jobs),
  0,
  'deferred queue policy does not immediately enqueue pgmq jobs'
);

select is(
  util.enqueue_pending_embeddings(
    10,
    null,
    'embedding_queue_trigger_rows',
    'embedding_ft',
    'embedding_ft'
  ),
  1,
  'bounded backfill enqueues one pending embedding job'
);

select is(
  (select count(*)::integer from util.pending_embedding_jobs where status = 'enqueued'),
  1,
  'backfilled pending job is marked enqueued'
);

select is(
  (select count(*)::integer from pgmq.q_embedding_jobs),
  1,
  'backfill creates one live pgmq embedding job'
);

select * from finish();

rollback;
