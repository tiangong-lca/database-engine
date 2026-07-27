-- Keep the owner-scoped closure-check read DTO valid while a check is still
-- queued or running. The persisted "pending" certificate state and NULL scan
-- completeness are internal lifecycle values; the public V1 projection uses
-- "unavailable" and "unknown" until terminal evidence exists.

create or replace function public.get_lcia_scope_closure_check(
  p_closure_check_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;

  select *
    into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;

  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_check.worker_job_id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-check.v1',
      'closureCheckId', v_check.id,
      'runStatus', v_check.status,
      'scanCompleteness', coalesce(v_check.scan_completeness, 'unknown'),
      'certificateValidity', case
        when v_check.certificate_status = 'pending' then 'unavailable'
        else v_check.certificate_status
      end,
      'requestedScopeHash', v_check.requested_scope_hash,
      'effectiveScopeHash', v_check.effective_scope_hash,
      'policyFingerprint', v_check.policy_fingerprint,
      'dataSnapshotToken', v_check.data_snapshot_token,
      'blockerCodes', to_jsonb(v_check.blocker_codes),
      'summary', v_check.result_summary,
      'scanExecutionId', v_check.scan_execution_id,
      'reusedFromCheckId', v_check.reused_from_check_id,
      'createdAt', v_check.created_at,
      'updatedAt', v_check.updated_at,
      'finishedAt', v_check.finished_at,
      'workerJob', case
        when v_job.id is null then null
        else jsonb_strip_nulls(jsonb_build_object(
          'jobId', v_job.id,
          'status', v_job.status,
          'phase', v_job.phase,
          'progressFraction', v_job.progress,
          'errorCode', v_job.error_code,
          'blockerCodes', to_jsonb(v_job.blocker_codes),
          'createdAt', v_job.created_at,
          'updatedAt', v_job.updated_at,
          'finishedAt', v_job.finished_at
        ))
      end
    ))
  );
end;
$$;

comment on function public.get_lcia_scope_closure_check(uuid) is
  'Returns the owner-scoped V1 closure-check DTO; internal pending/null lifecycle values project as unavailable/unknown while execution is nonterminal.';
