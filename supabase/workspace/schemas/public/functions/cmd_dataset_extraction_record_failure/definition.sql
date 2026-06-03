CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text" DEFAULT NULL::"text", "p_delete" boolean DEFAULT true) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  insert into util.dataset_extraction_job_failures (
    queue_name,
    msg_id,
    read_count,
    reason,
    message,
    last_error
  )
  values (
    'dataset_extraction_jobs',
    p_msg_id,
    coalesce(p_read_count, 0),
    coalesce(nullif(p_reason, ''), 'worker failure'),
    coalesce(p_message, '{}'::jsonb),
    p_last_error
  )
  on conflict (queue_name, msg_id) do update
  set
    read_count = excluded.read_count,
    reason = excluded.reason,
    message = excluded.message,
    last_error = excluded.last_error,
    created_at = now();

  if coalesce(p_delete, true) then
    perform pgmq.delete('dataset_extraction_jobs', p_msg_id);
  end if;

  return jsonb_build_object('ok', true);
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) TO "service_role";
