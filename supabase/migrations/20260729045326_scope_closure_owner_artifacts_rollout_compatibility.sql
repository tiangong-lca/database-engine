-- Expand the owner-scoped closure-check read contract before Edge migration.
-- The public availability list is deterministic and deliberately locator-free.
-- Keep the selector-less download overload until every Edge consumer has
-- migrated; remove it only through a separately tracked cleanup Issue.

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
  v_artifacts jsonb;
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

  with public_roles (
    ordinal,
    artifact_role,
    artifact_id,
    filename,
    format,
    media_type
  ) as (
    values
      (
        1,
        'closure_report_xlsx'::text,
        v_check.report_artifact_id,
        'scope-closure-' || v_check.id::text || '.xlsx',
        'xlsx'::text,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'::text
      ),
      (
        2,
        'closure_issue_manifest'::text,
        v_check.complete_machine_result_artifact_id,
        'scope-closure-' || v_check.id::text || '-manifest.json',
        'json'::text,
        'application/vnd.tiangong.scope-closure-manifest+json'::text
      )
  ),
  summaries as (
    select
      role.ordinal,
      jsonb_build_object(
        'artifactRole', role.artifact_role,
        'artifactState', case
          when artifact.id is null
               and v_check.status in ('queued', 'running') then 'pending'
          when artifact.id is null then 'failed'
          when not public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then 'failed'
          when artifact.lifecycle_state = 'deleted' then 'deleted'
          when artifact.lifecycle_state = 'expired'
               or artifact.expires_at <= now() then 'expired'
          when v_check.status in ('passed', 'blocked')
               and artifact.lifecycle_state = 'ready'
               and nullif(trim(artifact.storage_bucket), '') is not null
               and nullif(trim(artifact.storage_path), '') is not null
               and artifact.content_type = role.media_type
               and artifact.byte_size is not null
               and artifact.byte_size >= 0
               and artifact.checksum_sha256 ~ '^[0-9a-f]{64}$'
               and artifact.expires_at is not null then 'ready'
          else 'failed'
        end,
        'filename', role.filename,
        'format', role.format,
        'mediaType', role.media_type,
        'size', case
          when public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then artifact.byte_size
        end,
        'checksumSha256', case
          when public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then artifact.checksum_sha256
        end,
        'artifactExpiresAt', case
          when public.lcia_scope_closure_artifact_lineage_eligible(
            v_check,
            artifact,
            role.artifact_role
          ) then artifact.expires_at
        end
      ) as summary
    from public_roles role
    left join public.worker_job_artifacts artifact
      on artifact.id = role.artifact_id
  )
  select jsonb_agg(summary order by ordinal)
  into v_artifacts
  from summaries;

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
    )) || jsonb_build_object('artifacts', v_artifacts)
  );
end;
$$;

create or replace function public.get_lcia_scope_closure_report_download(
  p_closure_check_id uuid
) returns jsonb
language sql
security definer
set search_path = public, pg_temp
as $$
  select public.get_lcia_scope_closure_report_download(
    p_closure_check_id,
    'closure_report_xlsx'::text
  );
$$;

revoke all on function public.get_lcia_scope_closure_check(uuid)
from public, anon, authenticated, service_role;
revoke all on function public.get_lcia_scope_closure_report_download(uuid)
from public, anon, authenticated, service_role;
grant execute on function public.get_lcia_scope_closure_check(uuid)
to authenticated;
grant execute on function public.get_lcia_scope_closure_report_download(uuid)
to authenticated;

comment on function public.get_lcia_scope_closure_check(uuid) is
  'Returns the owner-scoped V1 closure-check DTO with a fixed-order, locator-free XLSX and issue-manifest availability summary.';
comment on function public.get_lcia_scope_closure_report_download(uuid) is
  'Temporary expand-migrate-contract overload forwarding to closure_report_xlsx. Remove in a separate cleanup Issue only after every Edge consumer migrates to the two-argument selector.';
