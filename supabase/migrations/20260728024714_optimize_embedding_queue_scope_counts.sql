create or replace function util.process_embeddings(
  batch_size integer default 3,
  max_requests integer default 3,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
set search_path to ''
as $$
declare
  job_batches jsonb[];
  batch jsonb;
  edge_fn text;
  timeout_seconds integer;
  selector_time timestamp with time zone;
begin
  if batch_size <= 0 or max_requests <= 0 then
    return;
  end if;

  if not pg_try_advisory_xact_lock(hashtext('util.process_embeddings')) then
    return;
  end if;

  timeout_seconds := greatest(1, ceiling(timeout_milliseconds / 1000.0)::integer);
  selector_time := clock_timestamp();

  with expired_jobs as (
    select
      q.msg_id,
      q.message,
      q.read_ct,
      p.max_read_count
    from pgmq.q_embedding_jobs q
    cross join lateral util.embedding_queue_policy_for(
      coalesce(q.message->>'schema', ''),
      coalesce(q.message->>'table', ''),
      coalesce(q.message->>'edgeFunction', 'embedding'),
      coalesce(q.message->>'embeddingColumn', '')
    ) p
    where q.vt <= selector_time
      and q.read_ct >= p.max_read_count
    order by q.msg_id
    limit greatest(batch_size * max_requests, 100)
  ),
  recorded_failures as (
    insert into util.embedding_job_failures (
      queue_name,
      msg_id,
      read_count,
      reason,
      message
    )
    select
      'embedding_jobs',
      expired.msg_id,
      expired.read_ct,
      format('read_ct reached retry cap %s', expired.max_read_count),
      expired.message
    from expired_jobs expired
    returning msg_id
  )
  delete from pgmq.q_embedding_jobs queued
  using recorded_failures failure
  where queued.msg_id = failure.msg_id;

  selector_time := clock_timestamp();

  with visible_jobs as materialized (
    select
      queued.msg_id,
      queued.message,
      coalesce(queued.message->>'schema', '') as schema_name,
      coalesce(queued.message->>'table', '') as table_name,
      coalesce(queued.message->>'edgeFunction', 'embedding') as edge_function,
      coalesce(queued.message->>'embeddingColumn', '') as embedding_column
    from pgmq.q_embedding_jobs queued
    where queued.vt <= selector_time
  ),
  visible_scopes as materialized (
    select distinct
      visible.schema_name,
      visible.table_name,
      visible.edge_function,
      visible.embedding_column
    from visible_jobs visible
  ),
  scope_policies as materialized (
    select
      scope.schema_name,
      scope.table_name,
      scope.edge_function,
      scope.embedding_column,
      policy.max_in_flight
    from visible_scopes scope
    cross join lateral util.embedding_queue_policy_for(
      scope.schema_name,
      scope.table_name,
      scope.edge_function,
      scope.embedding_column
    ) policy
    where policy.mode = 'normal'
  ),
  active_counts as materialized (
    select
      coalesce(active.message->>'schema', '') as schema_name,
      coalesce(active.message->>'table', '') as table_name,
      coalesce(active.message->>'edgeFunction', 'embedding') as edge_function,
      coalesce(active.message->>'embeddingColumn', '') as embedding_column,
      count(*)::integer as active_count
    from pgmq.q_embedding_jobs active
    where active.vt > selector_time
    group by 1, 2, 3, 4
  ),
  scoped_jobs as (
    select
      visible.*,
      policy.max_in_flight,
      coalesce(active.active_count, 0) as active_count,
      row_number() over (
        partition by
          visible.schema_name,
          visible.table_name,
          visible.edge_function,
          visible.embedding_column
        order by visible.msg_id
      ) as scope_position
    from visible_jobs visible
    join scope_policies policy using (
      schema_name,
      table_name,
      edge_function,
      embedding_column
    )
    left join active_counts active using (
      schema_name,
      table_name,
      edge_function,
      embedding_column
    )
  ),
  selected_jobs as (
    select *
    from scoped_jobs
    where active_count < max_in_flight
      and scope_position <= (max_in_flight - active_count)
    order by msg_id
    limit (batch_size * max_requests)
  ),
  claimed_jobs as (
    update pgmq.q_embedding_jobs queued
    set
      vt = clock_timestamp() + timeout_seconds * interval '1 second',
      read_ct = queued.read_ct + 1
    from selected_jobs selected
    where queued.msg_id = selected.msg_id
    returning
      queued.msg_id,
      queued.message || jsonb_build_object('jobId', queued.msg_id) as job_info,
      coalesce(queued.message->>'edgeFunction', 'embedding') as edge_function
  ),
  numbered_jobs as (
    select
      claimed.msg_id,
      claimed.job_info,
      claimed.edge_function,
      ((row_number() over (
        partition by claimed.edge_function
        order by claimed.msg_id
      ) - 1) / batch_size) as batch_num
    from claimed_jobs claimed
  ),
  batched_jobs as (
    select
      jsonb_agg(numbered.job_info order by numbered.msg_id) as batch_array,
      numbered.edge_function,
      numbered.batch_num
    from numbered_jobs numbered
    group by numbered.edge_function, numbered.batch_num
    order by numbered.edge_function, numbered.batch_num
  )
  select array_agg(batched.batch_array)
  from batched_jobs batched
  into job_batches;

  if job_batches is null then
    return;
  end if;

  foreach batch in array job_batches loop
    edge_fn := batch->0->>'edgeFunction';

    perform util.invoke_edge_function(
      name => edge_fn,
      body => batch,
      timeout_milliseconds => timeout_milliseconds
    );
  end loop;
end;
$$;

alter function util.process_embeddings(integer, integer, integer) owner to postgres;

comment on function util.process_embeddings(integer, integer, integer) is
  'Claims embedding work under per-scope backpressure using one active-count aggregation per queue snapshot rather than one full queue scan per visible job.';
