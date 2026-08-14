CREATE OR REPLACE FUNCTION "api"."cmd_review_quality_diagnostic_start"() RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_job private.worker_jobs%rowtype;
  v_concurrency_key constant text := 'review.quality_diagnostic.pending_review';
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
    and job.concurrency_key = v_concurrency_key
    and job.status in ('queued', 'running', 'waiting', 'stale')
  order by job.created_at desc, job.id desc
  limit 1;

  if found then
    return jsonb_build_object(
      'ok', true,
      'data', private.review_quality_diagnostic_projection(v_job),
      'reused', true
    );
  end if;

  insert into private.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    queue_key,
    subject_type,
    requester_type,
    requested_by,
    concurrency_key,
    status,
    phase,
    progress,
    visibility,
    max_attempts,
    payload_schema_version,
    payload_json,
    result_schema_version
  ) values (
    'review.quality_diagnostic',
    'calculator',
    'review_quality',
    20,
    'review-admin',
    'pending_review',
    'user',
    v_actor,
    v_concurrency_key,
    'queued',
    'pending_review_snapshot',
    0,
    'operator',
    3,
    'review.quality_diagnostic.request.v1',
    jsonb_build_object(
      'scope', jsonb_build_object(
        'kind', 'pending_review',
        'reviewStates', jsonb_build_array(0, 1)
      ),
      'requestedAt', now()
    ),
    'review.quality_diagnostic.report.v1'
  )
  returning * into v_job;

  insert into private.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    details
  ) values (
    v_job.id,
    'enqueued',
    v_job.status,
    v_job.phase,
    v_job.progress,
    jsonb_build_object(
      'jobKind', v_job.job_kind,
      'scopeKind', 'pending_review',
      'requestedByReviewAdmin', true
    )
  );

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    'cmd_review_quality_diagnostic_start',
    v_actor,
    'reviews',
    null,
    null,
    jsonb_build_object(
      'run_id', v_job.id,
      'scope_kind', 'pending_review'
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', private.review_quality_diagnostic_projection(v_job),
    'reused', false
  );
exception
  when unique_violation then
    select job.*
    into v_job
    from private.worker_jobs as job
    where job.job_kind = 'review.quality_diagnostic'
      and job.concurrency_key = v_concurrency_key
      and job.status in ('queued', 'running', 'waiting', 'stale')
    order by job.created_at desc, job.id desc
    limit 1;

    if found then
      return jsonb_build_object(
        'ok', true,
        'data', private.review_quality_diagnostic_projection(v_job),
        'reused', true
      );
    end if;

    raise;
end;
$$;

ALTER FUNCTION "api"."cmd_review_quality_diagnostic_start"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_quality_diagnostic_start"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_quality_diagnostic_start"() TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_review_quality_diagnostic_start"() TO "authenticated";
