CREATE OR REPLACE FUNCTION "util"."enqueue_pending_embeddings"("p_limit" integer DEFAULT 100, "p_schema_name" "text" DEFAULT NULL::"text", "p_table_name" "text" DEFAULT NULL::"text", "p_edge_function" "text" DEFAULT NULL::"text", "p_embedding_column" "text" DEFAULT NULL::"text") RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  pending_job record;
  sent_count integer := 0;
  sent_msg_id bigint;
begin
  if p_limit <= 0 then
    return 0;
  end if;

  if not pg_try_advisory_xact_lock(hashtext('util.enqueue_pending_embeddings')) then
    return 0;
  end if;

  for pending_job in
    select pending.*
    from util.pending_embedding_jobs pending
    cross join lateral util.embedding_queue_policy_for(
      pending.schema_name,
      pending.table_name,
      pending.edge_function,
      pending.embedding_column
    ) p
    where pending.status = 'pending'
      and p.mode <> 'paused'
      and (p_schema_name is null or pending.schema_name = p_schema_name)
      and (p_table_name is null or pending.table_name = p_table_name)
      and (p_edge_function is null or pending.edge_function = p_edge_function)
      and (p_embedding_column is null or pending.embedding_column = p_embedding_column)
    order by pending.created_at, pending.id
    limit p_limit
    for update skip locked
  loop
    select pgmq.send(
      queue_name => 'embedding_jobs',
      msg => pending_job.message
    )
    into sent_msg_id;

    update util.pending_embedding_jobs
    set
      status = 'enqueued',
      queue_msg_id = sent_msg_id,
      enqueued_at = now(),
      updated_at = now()
    where id = pending_job.id;

    sent_count := sent_count + 1;
  end loop;

  return sent_count;
end;
$$;

ALTER FUNCTION "util"."enqueue_pending_embeddings"("p_limit" integer, "p_schema_name" "text", "p_table_name" "text", "p_edge_function" "text", "p_embedding_column" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."enqueue_pending_embeddings"("p_limit" integer, "p_schema_name" "text", "p_table_name" "text", "p_edge_function" "text", "p_embedding_column" "text") FROM PUBLIC;
