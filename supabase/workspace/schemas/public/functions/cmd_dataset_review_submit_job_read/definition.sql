CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_is_service boolean := coalesce(util.is_service_request(), false);
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if p_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_ID_REQUIRED',
      'status', 400,
      'message', 'reviewSubmitJobId is required'
    );
  end if;

  if v_actor is null and not v_is_service then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if not v_is_service and v_job.requested_by is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can read this review-submit job'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") TO "authenticated";
