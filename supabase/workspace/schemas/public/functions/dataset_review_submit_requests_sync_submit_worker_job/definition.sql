CREATE OR REPLACE FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_worker_status text;
  v_progress numeric;
  v_result_json jsonb;
  v_result_ref jsonb;
  v_error_code text;
  v_error_message text;
  v_error_details jsonb;
  v_blocker_codes text[];
  v_resolution_scope text;
begin
  if new.submit_worker_job_id is null then
    return new;
  end if;

  v_worker_status := case
    when new.status in ('queued', 'waiting_gate', 'submitting') then 'waiting'
    when new.status = 'submitted' then 'completed'
    when new.status = 'blocked' then 'blocked'
    when new.status = 'stale' then 'stale'
    when new.status = 'cancelled' then 'cancelled'
    else 'failed'
  end;

  v_progress := case
    when new.status = 'queued' then 0
    when new.status = 'waiting_gate' then 0.25
    when new.status = 'submitting' then 0.75
    when new.status = 'submitted' then 1
    else null
  end;

  v_result_ref := jsonb_build_object(
    'domainSource', 'dataset_review_submit_requests',
    'reviewSubmitJobId', new.id
  );

  v_result_json := case
    when new.status = 'submitted' then jsonb_strip_nulls(
      jsonb_build_object(
        'status', 'submitted',
        'reviewSubmitJobId', new.id,
        'datasetRevision', jsonb_build_object(
          'table', new.dataset_table,
          'id', new.dataset_id,
          'version', new.dataset_version,
          'revisionChecksum', new.revision_checksum
        ),
        'result', new.result
      )
    )
    else null
  end;

  v_error_code := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_code, 'REVIEW_SUBMIT_JOB_' || upper(new.status))
    else null
  end;
  v_error_message := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_message, 'Review-submit job status is ' || new.status)
    else null
  end;
  v_error_details := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_details, '{}'::jsonb)
    else null
  end;
  v_blocker_codes := case
    when v_worker_status = 'blocked' then array[coalesce(new.last_error_code, 'REVIEW_SUBMIT_GATE_BLOCKED')]
    else '{}'::text[]
  end;
  v_resolution_scope := case
    when v_worker_status = 'blocked' then 'user'
    else null
  end;

  update public.worker_jobs
    set status = v_worker_status,
        phase = new.status,
        progress = v_progress,
        result_json = case
          when v_worker_status = 'completed' then v_result_json
          else result_json
        end,
        result_ref = coalesce(result_ref, '{}'::jsonb) || v_result_ref,
        error_code = v_error_code,
        error_message = v_error_message,
        error_details = v_error_details,
        blocker_codes = v_blocker_codes,
        resolution_scope = v_resolution_scope,
        retryable = case
          when v_worker_status in ('failed', 'stale') then true
          when v_worker_status in ('completed', 'blocked', 'cancelled') then false
          else null
        end,
        updated_at = now(),
        finished_at = case
          when v_worker_status in ('completed', 'blocked', 'stale', 'failed', 'cancelled') then coalesce(finished_at, now())
          else null
        end,
        cancelled_at = case
          when v_worker_status = 'cancelled' then coalesce(cancelled_at, now())
          else cancelled_at
        end
  where id = new.submit_worker_job_id;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    message,
    details
  )
  select
    new.submit_worker_job_id,
    'review_submit_status_synced',
    v_worker_status,
    new.status,
    v_progress,
    v_error_message,
    jsonb_strip_nulls(
      jsonb_build_object(
        'reviewSubmitJobId', new.id,
        'errorCode', v_error_code,
        'blockerCodes', to_jsonb(v_blocker_codes),
        'resolutionScope', v_resolution_scope
      )
    );

  return new;
end;
$$;

ALTER FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() TO "service_role";
