CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_deleted jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  if coalesce(array_length(p_msg_ids, 1), 0) = 0 then
    return jsonb_build_object('ok', true, 'data', jsonb_build_object('deleted_msg_ids', v_deleted));
  end if;

  select coalesce(jsonb_agg(deleted_msg_id order by deleted_msg_id), '[]'::jsonb)
  into v_deleted
  from pgmq.delete('dataset_extraction_jobs', p_msg_ids) as deleted_msg_id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object('deleted_msg_ids', v_deleted)
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) TO "service_role";
