CREATE OR REPLACE FUNCTION "api"."svc_ai_tidas_suggestion_enqueue"("p_requested_by" "uuid", "p_data_type" "text", "p_data" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_data_type text := lower(trim(coalesce(p_data_type, '')));
  v_payload jsonb;
  v_request_hash text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to enqueue AI jobs'
    );
  end if;

  if p_requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_REQUESTED_BY_REQUIRED',
      'status', 400,
      'message', 'requestedBy is required'
    );
  end if;

  if v_data_type not in ('process', 'flow') then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_DATA_TYPE_INVALID',
      'status', 400,
      'message', 'dataType must be process or flow'
    );
  end if;

  if jsonb_typeof(p_data) is distinct from 'object'
    or (
      v_data_type = 'process'
      and jsonb_typeof(p_data->'processDataSet') is distinct from 'object'
    )
    or (
      v_data_type = 'flow'
      and jsonb_typeof(p_data->'flowDataSet') is distinct from 'object'
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_DATA_INVALID',
      'status', 400,
      'message', 'data must contain the matching TIDAS dataset root object'
    );
  end if;

  v_payload := jsonb_build_object(
    'dataType', v_data_type,
    'data', p_data
  );

  if pg_catalog.octet_length(v_payload::text) > 16777216 then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_DATA_TOO_LARGE',
      'status', 413,
      'message', 'AI request exceeds the 16 MiB absolute contract limit'
    );
  end if;

  v_request_hash := pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(v_payload::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  return private.worker_enqueue_job(
    p_job_kind => 'ai.tidas_suggestion',
    p_payload_json => v_payload,
    p_payload_schema_version => 'ai.tidas_suggestion.request.v1',
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => 'ai.tidas_suggestion:' || v_request_hash,
    p_request_hash => v_request_hash,
    p_visibility => 'user',
    p_max_attempts => 3,
    p_timeout_at => now() + interval '30 minutes'
  );
end;
$$;

ALTER FUNCTION "api"."svc_ai_tidas_suggestion_enqueue"("p_requested_by" "uuid", "p_data_type" "text", "p_data" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_ai_tidas_suggestion_enqueue"("p_requested_by" "uuid", "p_data_type" "text", "p_data" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_ai_tidas_suggestion_enqueue"("p_requested_by" "uuid", "p_data_type" "text", "p_data" "jsonb") TO "service_role";
