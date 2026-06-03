CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer DEFAULT 10, "p_stale_submitting_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_qty integer := greatest(1, least(coalesce(p_qty, 10), 50));
  v_stale_seconds integer := greatest(30, coalesce(p_stale_submitting_seconds, 300));
  v_jobs jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim review-submit jobs'
    );
  end if;

  with claimed as (
    select id
    from public.dataset_review_submit_requests
    where status in ('queued', 'waiting_gate')
      or (
        status = 'submitting'
        and modified_at < now() - make_interval(secs => v_stale_seconds)
      )
    order by
      case status
        when 'queued' then 0
        when 'waiting_gate' then 1
        else 2
      end,
      created_at asc
    for update skip locked
    limit v_qty
  ),
  updated as (
    update public.dataset_review_submit_requests as request
      set status = 'submitting',
          attempt_count = request.attempt_count + 1,
          modified_at = now()
    from claimed
    where request.id = claimed.id
    returning request.*
  )
  select coalesce(jsonb_agg(public.cmd_dataset_review_submit_job_payload(updated)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) TO "service_role";
