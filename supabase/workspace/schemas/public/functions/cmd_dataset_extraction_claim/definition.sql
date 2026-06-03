CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer DEFAULT 10, "p_vt_seconds" integer DEFAULT 300, "p_max_read_count" integer DEFAULT 5) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_qty integer;
  v_vt_seconds integer;
  v_max_read_count integer;
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  v_qty := least(greatest(coalesce(p_qty, 10), 1), 50);
  v_vt_seconds := least(greatest(coalesce(p_vt_seconds, 300), 1), 3600);
  v_max_read_count := least(greatest(coalesce(p_max_read_count, 5), 1), 100);

  with expired_jobs as (
    select
      q.msg_id,
      q.message,
      q.read_ct
    from pgmq.q_dataset_extraction_jobs q
    where q.vt <= clock_timestamp()
      and q.read_ct >= v_max_read_count
    order by q.msg_id
    limit greatest(v_qty, 100)
  ),
  recorded_failures as (
    insert into util.dataset_extraction_job_failures (
      queue_name,
      msg_id,
      read_count,
      reason,
      message
    )
    select
      'dataset_extraction_jobs',
      e.msg_id,
      e.read_ct,
      format('read_ct reached retry cap %s', v_max_read_count),
      e.message
    from expired_jobs e
    on conflict (queue_name, msg_id) do update
    set
      read_count = excluded.read_count,
      reason = excluded.reason,
      message = excluded.message,
      created_at = now()
    returning msg_id
  )
  delete from pgmq.q_dataset_extraction_jobs q
  using recorded_failures f
  where q.msg_id = f.msg_id;

  with claimed_jobs as (
    select *
    from pgmq.read('dataset_extraction_jobs', v_vt_seconds, v_qty)
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'msg_id', msg_id,
        'read_ct', read_ct,
        'enqueued_at', enqueued_at,
        'vt', vt,
        'message', message
      )
      order by msg_id
    ),
    '[]'::jsonb
  )
  into v_jobs
  from claimed_jobs;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) TO "service_role";
