CREATE OR REPLACE FUNCTION "util"."fail_dataset_derivative_rebuild_after_drain"("p_request_id" "uuid", "p_code" "text", "p_message" "text", "p_details" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_quarantine jsonb;
  v_now timestamp with time zone := pg_catalog.clock_timestamp();
begin
  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.status not in ('completed', 'stale', 'failed')
  for update;

  if v_request.id is null then
    return;
  end if;

  v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
    v_request.target_table,
    v_request.target_id,
    v_request.target_version
  );

  update util.dataset_derivative_rebuild_proposals
  set
    status = 'discarded',
    discarded_at = v_now
  where request_id = v_request.id
    and status in ('captured', 'accepted');

  update util.dataset_derivative_rebuild_requests
  set
    status = 'dispatching',
    phase = 'failure_draining',
    failure_release_not_before = coalesce(
      v_request.failure_release_not_before,
      v_now + interval '420 seconds'
    ),
    last_error = coalesce(
      v_request.last_error,
      jsonb_build_object(
        'code', p_code,
        'message', p_message,
        'details', coalesce(p_details, '{}'::jsonb)
      )
    ),
    quarantined_http_requests = quarantined_http_requests
      + coalesce((v_quarantine->>'http_requests')::integer, 0),
    quarantined_embedding_jobs = quarantined_embedding_jobs
      + coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
    quarantined_pending_jobs = quarantined_pending_jobs
      + coalesce((v_quarantine->>'pending_jobs')::integer, 0),
    updated_at = v_now
  where id = p_request_id
    and status not in ('completed', 'stale', 'failed');
end;
$$;

ALTER FUNCTION "util"."fail_dataset_derivative_rebuild_after_drain"("p_request_id" "uuid", "p_code" "text", "p_message" "text", "p_details" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."fail_dataset_derivative_rebuild_after_drain"("p_request_id" "uuid", "p_code" "text", "p_message" "text", "p_details" "jsonb") FROM PUBLIC;
