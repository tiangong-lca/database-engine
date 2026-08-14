CREATE OR REPLACE FUNCTION "util"."queue_embeddings"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_content_function text = tg_argv[0];
  v_embedding_column text = tg_argv[1];
  v_edge_function text := coalesce(tg_argv[2], 'embedding');
  v_queue_mode text;
  v_message jsonb;
begin
  if tg_table_schema = 'public'
    and tg_table_name in ('flows', 'processes')
    and exists (
      select 1
      from util.dataset_derivative_rebuild_permits as permit
      join util.dataset_derivative_rebuild_requests as request
        on request.id = permit.request_id
      where permit.permit_kind = 'markdown'
        and permit.backend_pid = pg_catalog.pg_backend_pid()
        and permit.transaction_id = pg_catalog.txid_current()
        and request.target_table = tg_table_name
        and request.target_id = new.id
        and request.target_version = btrim(new.version::text)
    ) then
    return new;
  end if;

  v_message := jsonb_build_object(
    'id', new.id,
    'version', new.version,
    'schema', tg_table_schema,
    'table', tg_table_name,
    'contentFunction', v_content_function,
    'embeddingColumn', v_embedding_column,
    'edgeFunction', v_edge_function
  );

  select policy.mode
  into v_queue_mode
  from util.embedding_queue_policy_for(
    tg_table_schema,
    tg_table_name,
    v_edge_function,
    v_embedding_column
  ) as policy;

  if v_queue_mode in ('deferred', 'paused') then
    insert into util.pending_embedding_jobs (
      schema_name,
      table_name,
      record_id,
      record_version,
      content_function,
      embedding_column,
      edge_function,
      message
    ) values (
      tg_table_schema,
      tg_table_name,
      new.id::text,
      new.version::text,
      v_content_function,
      v_embedding_column,
      v_edge_function,
      v_message
    )
    on conflict (
      schema_name,
      table_name,
      record_id,
      record_version,
      embedding_column,
      edge_function
    ) where status = 'pending'
    do update set
      content_function = excluded.content_function,
      message = excluded.message,
      updated_at = pg_catalog.clock_timestamp();
    return new;
  end if;

  perform pgmq.send(
    queue_name => 'embedding_jobs',
    msg => v_message
  );
  return new;
end;
$$;

ALTER FUNCTION "util"."queue_embeddings"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."queue_embeddings"() FROM PUBLIC;
