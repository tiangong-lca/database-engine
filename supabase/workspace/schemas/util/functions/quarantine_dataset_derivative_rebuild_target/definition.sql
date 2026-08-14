CREATE OR REPLACE FUNCTION "util"."quarantine_dataset_derivative_rebuild_target"("p_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select util.quarantine_dataset_derivative_rebuild_target(
    'processes',
    p_id,
    p_version
  )
$$;

ALTER FUNCTION "util"."quarantine_dataset_derivative_rebuild_target"("p_id" "uuid", "p_version" "text") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."quarantine_dataset_derivative_rebuild_target"("p_table" "text", "p_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_http integer := 0;
  v_embedding integer := 0;
  v_pending integer := 0;
begin
  if p_table is null or p_table not in ('flows', 'processes') then
    raise exception using
      errcode = '22023',
      message = 'Derivative quarantine target table must be flows or processes';
  end if;

  delete from net.http_request_queue as request
  where (
      request.url like '%/functions/v1/webhook_process_embedding_ft'
      or request.url like '%/functions/v1/webhook_flow_embedding_ft'
      or request.url like '%/functions/v1/embedding_ft'
    )
    and util.dataset_derivative_rebuild_http_body_matches(
      request.body,
      p_table,
      p_id,
      p_version
    );
  get diagnostics v_http = row_count;

  delete from pgmq.q_embedding_jobs as job
  where job.message->>'id' = p_id::text
    and btrim(job.message->>'version') = p_version
    and job.message->>'schema' = 'public'
    and job.message->>'table' = p_table
    and job.message->>'embeddingColumn' = 'embedding_ft';
  get diagnostics v_embedding = row_count;

  delete from util.pending_embedding_jobs as pending
  where pending.schema_name = 'public'
    and pending.table_name = p_table
    and pending.record_id = p_id::text
    and btrim(pending.record_version) = p_version
    and pending.embedding_column = 'embedding_ft';
  get diagnostics v_pending = row_count;

  return jsonb_build_object(
    'http_requests', v_http,
    'embedding_jobs', v_embedding,
    'pending_jobs', v_pending
  );
end;
$$;

ALTER FUNCTION "util"."quarantine_dataset_derivative_rebuild_target"("p_table" "text", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."quarantine_dataset_derivative_rebuild_target"("p_id" "uuid", "p_version" "text") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."quarantine_dataset_derivative_rebuild_target"("p_table" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;
