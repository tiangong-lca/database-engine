CREATE OR REPLACE FUNCTION "api"."qry_review_quality_diagnostic"("p_run_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_job private.worker_jobs%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not api.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Review Admin role is required'
    );
  end if;

  select job.*
  into v_job
  from private.worker_jobs as job
  where job.job_kind = 'review.quality_diagnostic'
    and (p_run_id is null or job.id = p_run_id)
  order by job.updated_at desc, job.id desc
  limit 1;

  if not found then
    if p_run_id is null then
      return jsonb_build_object('ok', true, 'data', null);
    end if;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_QUALITY_DIAGNOSTIC_NOT_FOUND',
      'status', 404,
      'message', 'Review quality diagnostic run not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', private.review_quality_diagnostic_projection(v_job)
  );
end;
$$;

ALTER FUNCTION "api"."qry_review_quality_diagnostic"("p_run_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_quality_diagnostic"("p_run_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_quality_diagnostic"("p_run_id" "uuid") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_review_quality_diagnostic"("p_run_id" "uuid") TO "authenticated";
