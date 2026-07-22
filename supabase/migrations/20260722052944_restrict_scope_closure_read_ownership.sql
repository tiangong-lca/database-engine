-- Scope-closure scans are private operator work.  A data_product_manager role
-- permits an actor to run and read *their own* checks; it does not grant an
-- organization-wide read capability.  These SECURITY DEFINER RPCs must make
-- the owner predicate explicit because table RLS is intentionally bypassed.

create or replace function public.get_lcia_scope_closure_check(p_closure_check_id uuid)
returns jsonb
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
    return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;

  select *
    into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;

  if v_check.id is null then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_check.worker_job_id;

  return jsonb_build_object('ok', true, 'data', jsonb_strip_nulls(jsonb_build_object(
    'schemaVersion', 'lcia.scope-closure-check.v1',
    'closureCheckId', v_check.id,
    'runStatus', v_check.status,
    'scanCompleteness', v_check.scan_completeness,
    'certificateValidity', v_check.certificate_status,
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
    'workerJob', case when v_job.id is null then null else jsonb_strip_nulls(jsonb_build_object(
      'jobId', v_job.id,
      'status', v_job.status,
      'phase', v_job.phase,
      'progressFraction', v_job.progress,
      'errorCode', v_job.error_code,
      'blockerCodes', to_jsonb(v_job.blocker_codes),
      'createdAt', v_job.created_at,
      'updatedAt', v_job.updated_at,
      'finishedAt', v_job.finished_at
    )) end
  )));
end;
$$;

create or replace function public.list_lcia_scope_closure_issues(
  p_closure_check_id uuid,
  p_after_issue_id uuid default null,
  p_limit integer default 100
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_limit, 100), 200));
begin
  if v_actor is null then
    return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lcia_scope_closure_is_manager()
     or not exists (
       select 1
       from public.lcia_scope_closure_checks c
       where c.id = p_closure_check_id
         and c.requested_by = v_actor
     ) then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;

  return jsonb_build_object('ok', true, 'data', (
    with page as (
      select i.*
      from public.lcia_scope_closure_issues i
      where i.closure_check_id = p_closure_check_id
        and (p_after_issue_id is null or i.id > p_after_issue_id)
      order by i.id
      limit v_limit + 1
    ), shown as (
      select * from page order by id limit v_limit
    )
    select jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-issues-page.v1',
      'closureCheckId', p_closure_check_id,
      'issues', coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'issueId', id,
        'severity', severity,
        'blocking', blocking,
        'code', issue_code,
        'title', issue_code,
        'summary', message,
        'suggestedAction', suggested_action,
        'occurrenceCount', occurrence_count,
        'affectedRootCount', affected_root_count
      )) order by id), '[]'::jsonb),
      'totalCount', (
        select count(*)
        from public.lcia_scope_closure_issues
        where closure_check_id = p_closure_check_id
      ),
      'nextCursor', case when exists (select 1 from page offset v_limit)
        then (select id from shown order by id desc limit 1)
        else null
      end
    )
    from shown
  ));
end;
$$;

-- Keep download authorization on the same opaque own-check boundary.  Without
-- this early owner check, a second manager could distinguish an existing
-- report from an arbitrary ID through a different error envelope.
create or replace function public.get_lcia_scope_closure_report_download(p_closure_check_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_artifact public.worker_job_artifacts%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lcia_scope_closure_is_manager()
     or not exists (
       select 1
       from public.lcia_scope_closure_checks c
       where c.id = p_closure_check_id
         and c.requested_by = v_actor
     ) then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;

  select a.*
    into v_artifact
  from public.lcia_scope_closure_checks c
  join public.worker_job_artifacts a on a.id = c.report_artifact_id
  where c.id = p_closure_check_id
    and c.requested_by = v_actor
    and c.status in ('passed', 'blocked');

  if v_artifact.id is null then
    return public.lcia_scope_closure_error('closure_report_unavailable', 404, 'Closure report is not available');
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'artifactId', v_artifact.id,
    'bucket', v_artifact.storage_bucket,
    'objectPath', v_artifact.storage_path,
    'mediaType', v_artifact.content_type,
    'size', v_artifact.byte_size,
    'checksumSha256', v_artifact.checksum_sha256
  ));
end;
$$;

revoke all on function public.get_lcia_scope_closure_check(uuid)
  from public, anon, authenticated, service_role;
revoke all on function public.list_lcia_scope_closure_issues(uuid, uuid, integer)
  from public, anon, authenticated, service_role;
revoke all on function public.get_lcia_scope_closure_report_download(uuid)
  from public, anon, authenticated, service_role;

grant execute on function public.get_lcia_scope_closure_check(uuid) to authenticated;
grant execute on function public.list_lcia_scope_closure_issues(uuid, uuid, integer) to authenticated;
grant execute on function public.get_lcia_scope_closure_report_download(uuid) to authenticated;
