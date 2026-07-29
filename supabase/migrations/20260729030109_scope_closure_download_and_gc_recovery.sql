-- Reconcile the public closure-artifact descriptor with Edge/Next and make
-- post-tombstone detail cleanup recoverable by a fresh Worker process.
--
-- This is deliberately a follow-up migration.  The Supabase GitHub
-- integration applies only new migration files to an existing Preview branch.

alter table public.worker_job_artifacts
  add column if not exists gc_cleanup_state text not null default 'none';

alter table public.worker_job_artifacts
  drop constraint if exists worker_job_artifacts_gc_cleanup_state_check,
  add constraint worker_job_artifacts_gc_cleanup_state_check
    check (gc_cleanup_state in ('none', 'pending', 'complete'));

update public.worker_job_artifacts artifact
set gc_cleanup_state = case
  when exists (
    select 1
    from public.lcia_scope_closure_checks closure_check
    join public.lcia_scope_closure_issues issue
      on issue.closure_check_id = closure_check.id
    where closure_check.report_artifact_id = artifact.id
       or closure_check.complete_machine_result_artifact_id = artifact.id
       or closure_check.closure_bundle_artifact_id = artifact.id
  ) then 'pending'
  else 'complete'
end
where artifact.lifecycle_state = 'deleted'
  and artifact.artifact_role is not null
  and artifact.gc_cleanup_state = 'none';

alter table public.worker_job_artifacts
  drop constraint if exists worker_job_artifacts_gc_cleanup_lifecycle_check,
  add constraint worker_job_artifacts_gc_cleanup_lifecycle_check
    check (
      (lifecycle_state = 'deleted' and gc_cleanup_state in ('pending', 'complete'))
      or (lifecycle_state is distinct from 'deleted' and gc_cleanup_state = 'none')
      or artifact_role is null
    ) not valid;

alter table public.worker_job_artifacts
  validate constraint worker_job_artifacts_gc_cleanup_lifecycle_check;

drop function if exists public.get_lcia_scope_closure_report_download(uuid);

create or replace function public.get_lcia_scope_closure_report_download(
  p_closure_check_id uuid,
  p_artifact_role text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
  v_source_check public.lcia_scope_closure_checks%rowtype;
  v_artifact public.worker_job_artifacts%rowtype;
  v_artifact_id uuid;
  v_expected_internal_role text;
  v_expected_artifact_type text;
  v_expected_media_type text;
  v_format text;
  v_filename text;
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

  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;

  if p_artifact_role = 'closure_report_xlsx' then
    v_artifact_id := v_check.report_artifact_id;
    v_expected_internal_role := 'closure_report';
    v_expected_artifact_type := 'closure_report_xlsx';
    v_expected_media_type :=
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    v_format := 'xlsx';
    v_filename := 'scope-closure-' || v_check.id::text || '.xlsx';
  elsif p_artifact_role = 'closure_issue_manifest' then
    v_artifact_id := v_check.complete_machine_result_artifact_id;
    v_expected_internal_role := 'complete_machine_result';
    v_expected_artifact_type := 'closure_complete_machine_result';
    v_expected_media_type :=
      'application/vnd.tiangong.scope-closure-manifest+json';
    v_format := 'json';
    v_filename :=
      'scope-closure-' || v_check.id::text || '-manifest.json';
  else
    return public.lcia_scope_closure_error(
      'closure_artifact_role_invalid',
      400,
      'Unsupported closure artifact role'
    );
  end if;

  if v_check.reused_from_check_id is not null then
    select * into v_source_check
    from public.lcia_scope_closure_checks
    where id = v_check.reused_from_check_id;
  end if;

  select * into v_artifact
  from public.worker_job_artifacts
  where id = v_artifact_id
    and artifact_role = v_expected_internal_role
    and artifact_type = v_expected_artifact_type
    and (
      job_id = v_check.worker_job_id
      or (
        p_artifact_role = 'closure_issue_manifest'
        and v_source_check.id is not null
        and job_id = v_source_check.worker_job_id
      )
    );

  if v_artifact.id is not null
     and (
       v_artifact.lifecycle_state = 'expired'
       or (
         v_artifact.lifecycle_state <> 'deleted'
         and v_artifact.expires_at <= now()
       )
     ) then
    return public.lcia_scope_closure_error(
      'closure_report_expired', 410, 'Closure report has expired'
    );
  end if;

  if v_artifact.id is null
     or v_check.status not in ('passed', 'blocked')
     or v_artifact.lifecycle_state <> 'ready'
     or v_artifact.expires_at is null
     or v_artifact.expires_at <= now()
     or nullif(trim(v_artifact.storage_bucket), '') is null
     or nullif(trim(v_artifact.storage_path), '') is null
     or v_artifact.content_type is distinct from v_expected_media_type
     or v_artifact.byte_size is null
     or v_artifact.byte_size < 0
     or v_artifact.checksum_sha256 is null
     or v_artifact.checksum_sha256 !~ '^[a-f0-9]{64}$' then
    return public.lcia_scope_closure_error(
      'closure_report_unavailable', 404, 'Closure report is not available'
    );
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'artifactId', v_artifact.id,
    'artifactRole', p_artifact_role,
    'artifactState', 'ready',
    'filename', v_filename,
    'format', v_format,
    'mediaType', v_expected_media_type,
    'size', v_artifact.byte_size,
    'checksumSha256', v_artifact.checksum_sha256,
    'artifactExpiresAt', v_artifact.expires_at,
    'bucket', v_artifact.storage_bucket,
    'objectPath', v_artifact.storage_path
  ));
end;
$$;

revoke all on function public.get_lcia_scope_closure_report_download(uuid, text)
  from public, anon, authenticated, service_role;
grant execute on function public.get_lcia_scope_closure_report_download(uuid, text)
  to authenticated;

drop index if exists public.worker_job_artifacts_scope_closure_gc_idx;
create index worker_job_artifacts_scope_closure_gc_idx
  on public.worker_job_artifacts (
    gc_cleanup_state,
    gc_claim_expires_at,
    expires_at,
    created_at,
    id
  )
  where artifact_role is not null
    and (
      lifecycle_state in ('ready', 'expired')
      or (lifecycle_state = 'deleted' and gc_cleanup_state = 'pending')
    );

create or replace function public.svc_lcia_scope_closure_artifact_gc_claim(
  p_limit integer default 100,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_claim_token uuid := gen_random_uuid();
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit is null
     or p_limit not between 1 and 500
     or p_lease_seconds is null
     or p_lease_seconds not between 30 and 3600 then
    return public.lcia_scope_closure_error(
      'invalid_gc_claim', 400, 'Invalid GC claim bounds'
    );
  end if;

  with candidates as (
    select id
    from public.worker_job_artifacts
    where artifact_role is not null
      and (
        (
          lifecycle_state in ('ready', 'expired')
          and expires_at <= now()
        )
        or (
          lifecycle_state = 'deleted'
          and gc_cleanup_state = 'pending'
        )
      )
      and (
        gc_claim_token is null
        or gc_claim_expires_at is null
        or gc_claim_expires_at <= now()
      )
    order by
      case
        when lifecycle_state = 'deleted'
             and gc_cleanup_state = 'pending' then 0
        else 1
      end,
      expires_at,
      created_at,
      id
    for update skip locked
    limit p_limit
  ), claimed as (
    update public.worker_job_artifacts artifact
    set lifecycle_state = case
          when artifact.lifecycle_state = 'deleted' then 'deleted'
          else 'expired'
        end,
        gc_claim_token = v_claim_token,
        gc_claimed_at = now(),
        gc_claim_expires_at = v_lease_expires_at,
        gc_last_error = null
    from candidates
    where artifact.id = candidates.id
    returning artifact.*
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'artifactId', id,
    'artifactRole', artifact_role,
    'lifecycleState', lifecycle_state,
    'gcPhase', case
      when lifecycle_state = 'deleted' then 'detail_cleanup'
      else 'object_delete'
    end,
    'objectDeleteRequired', lifecycle_state <> 'deleted',
    'bucket', storage_bucket,
    'objectPath', storage_path,
    'checksumSha256', checksum_sha256,
    'artifactExpiresAt', expires_at
  ) order by
    case when lifecycle_state = 'deleted' then 0 else 1 end,
    expires_at,
    created_at,
    id
  ), '[]'::jsonb)
  into v_items
  from claimed;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'claimToken', v_claim_token,
    'leaseExpiresAt', v_lease_expires_at,
    'items', v_items
  ));
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_gc_complete(
  p_artifact_id uuid,
  p_claim_token uuid,
  p_object_missing boolean default false,
  p_detail_limit integer default 10000
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_artifact public.worker_job_artifacts%rowtype;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_deleted_occurrences integer := 0;
  v_deleted_roots integer := 0;
  v_deleted_issues integer := 0;
  v_remaining bigint := 0;
  v_gc_phase text;
  v_object_delete_required boolean;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_detail_limit is null
     or p_detail_limit not between 1 and 50000 then
    return public.lcia_scope_closure_error(
      'invalid_gc_completion', 400, 'Invalid detail cleanup bound'
    );
  end if;

  select * into v_artifact
  from public.worker_job_artifacts
  where id = p_artifact_id
  for update;
  if v_artifact.id is null then
    return public.lcia_scope_closure_error(
      'artifact_not_found', 404, 'Artifact not found'
    );
  end if;

  if v_artifact.lifecycle_state = 'deleted'
     and v_artifact.gc_cleanup_state = 'complete'
     and v_artifact.gc_claim_token = p_claim_token then
    return jsonb_build_object('ok', true, 'reused', true, 'data',
      jsonb_build_object(
        'artifactId', v_artifact.id,
        'state', 'deleted',
        'lifecycleState', 'deleted',
        'gcPhase', 'detail_cleanup',
        'objectDeleteRequired', false,
        'objectMissing', p_object_missing,
        'detailsRemaining', 0,
        'cleanupPending', false,
        'cleanupComplete', true
      )
    );
  end if;

  if v_artifact.gc_claim_token is distinct from p_claim_token
     or v_artifact.gc_claim_expires_at is null
     or v_artifact.gc_claim_expires_at < now()
     or not (
       v_artifact.lifecycle_state = 'expired'
       or (
         v_artifact.lifecycle_state = 'deleted'
         and v_artifact.gc_cleanup_state = 'pending'
       )
     ) then
    return public.lcia_scope_closure_error(
      'gc_claim_invalid', 409, 'GC claim is not current'
    );
  end if;

  v_object_delete_required := v_artifact.lifecycle_state = 'expired';
  v_gc_phase := case
    when v_object_delete_required then 'object_delete'
    else 'detail_cleanup'
  end;

  select * into v_check
  from public.lcia_scope_closure_checks
  where report_artifact_id = v_artifact.id
     or complete_machine_result_artifact_id = v_artifact.id
     or closure_bundle_artifact_id = v_artifact.id
  order by finished_at nulls last, id
  limit 1
  for update;

  if v_check.id is not null then
    insert into public.lcia_scope_closure_retention_summaries (
      closure_check_id,
      issue_count,
      occurrence_count,
      affected_root_count,
      issue_content_hash,
      compact_result_summary
    )
    select
      v_check.id,
      (
        select count(*)
        from public.lcia_scope_closure_issues issue
        where issue.closure_check_id = v_check.id
      ),
      (
        select count(*)
        from public.lcia_scope_closure_issue_occurrences occurrence
        join public.lcia_scope_closure_issues issue
          on issue.id = occurrence.closure_issue_id
        where issue.closure_check_id = v_check.id
      ),
      (
        select count(*)
        from public.lcia_scope_closure_issue_roots root
        join public.lcia_scope_closure_issues issue
          on issue.id = root.closure_issue_id
        where issue.closure_check_id = v_check.id
      ),
      public.lcia_scope_closure_sha256(coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'issueKey', issue.issue_key,
            'issueCode', issue.issue_code,
            'severity', issue.severity,
            'blocking', issue.blocking,
            'occurrenceCount', issue.occurrence_count,
            'affectedRootCount', issue.affected_root_count
          )
          order by issue.issue_key, issue.id
        )
        from public.lcia_scope_closure_issues issue
        where issue.closure_check_id = v_check.id
      ), '[]'::jsonb)),
      v_check.result_summary
    on conflict (closure_check_id) do nothing;

    with doomed as (
      select occurrence.ctid
      from public.lcia_scope_closure_issue_occurrences occurrence
      join public.lcia_scope_closure_issues issue
        on issue.id = occurrence.closure_issue_id
      where issue.closure_check_id = v_check.id
      limit p_detail_limit
    )
    delete from public.lcia_scope_closure_issue_occurrences occurrence
    using doomed
    where occurrence.ctid = doomed.ctid;
    get diagnostics v_deleted_occurrences = row_count;

    with doomed as (
      select root.ctid
      from public.lcia_scope_closure_issue_roots root
      join public.lcia_scope_closure_issues issue
        on issue.id = root.closure_issue_id
      where issue.closure_check_id = v_check.id
      limit p_detail_limit
    )
    delete from public.lcia_scope_closure_issue_roots root
    using doomed
    where root.ctid = doomed.ctid;
    get diagnostics v_deleted_roots = row_count;

    if not exists (
      select 1
      from public.lcia_scope_closure_issue_occurrences occurrence
      join public.lcia_scope_closure_issues issue
        on issue.id = occurrence.closure_issue_id
      where issue.closure_check_id = v_check.id
    ) and not exists (
      select 1
      from public.lcia_scope_closure_issue_roots root
      join public.lcia_scope_closure_issues issue
        on issue.id = root.closure_issue_id
      where issue.closure_check_id = v_check.id
    ) then
      with doomed as (
        select ctid
        from public.lcia_scope_closure_issues
        where closure_check_id = v_check.id
        limit p_detail_limit
      )
      delete from public.lcia_scope_closure_issues issue
      using doomed
      where issue.ctid = doomed.ctid;
      get diagnostics v_deleted_issues = row_count;
    end if;

    select
      (select count(*)
       from public.lcia_scope_closure_issues
       where closure_check_id = v_check.id)
      + (
        select count(*)
        from public.lcia_scope_closure_issue_occurrences occurrence
        join public.lcia_scope_closure_issues issue
          on issue.id = occurrence.closure_issue_id
        where issue.closure_check_id = v_check.id
      )
      + (
        select count(*)
        from public.lcia_scope_closure_issue_roots root
        join public.lcia_scope_closure_issues issue
          on issue.id = root.closure_issue_id
        where issue.closure_check_id = v_check.id
      )
    into v_remaining;
  end if;

  update public.worker_job_artifacts
  set lifecycle_state = 'deleted',
      deleted_at = coalesce(deleted_at, now()),
      gc_cleanup_state = case
        when v_remaining > 0 then 'pending'
        else 'complete'
      end,
      gc_claim_expires_at = case
        when v_remaining > 0 then gc_claim_expires_at
        else null
      end,
      gc_last_error = null
  where id = v_artifact.id
  returning * into v_artifact;

  return jsonb_build_object('ok', true, 'reused', false, 'data',
    jsonb_build_object(
      'artifactId', v_artifact.id,
      'state', v_artifact.lifecycle_state,
      'lifecycleState', v_artifact.lifecycle_state,
      'gcPhase', v_gc_phase,
      'objectDeleteRequired', v_object_delete_required,
      'objectMissing', p_object_missing,
      'deletedOccurrences', v_deleted_occurrences,
      'deletedAffectedRoots', v_deleted_roots,
      'deletedIssues', v_deleted_issues,
      'detailsRemaining', v_remaining,
      'cleanupPending', v_remaining > 0,
      'cleanupComplete', v_remaining = 0
    )
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_gc_fail(
  p_artifact_id uuid,
  p_claim_token uuid,
  p_error text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_artifact public.worker_job_artifacts%rowtype;
  v_gc_phase text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return public.lcia_scope_closure_error(
      'invalid_gc_failure', 400, 'GC failure reason is required'
    );
  end if;

  select * into v_artifact
  from public.worker_job_artifacts
  where id = p_artifact_id
  for update;
  if v_artifact.id is null then
    return public.lcia_scope_closure_error(
      'artifact_not_found', 404, 'Artifact not found'
    );
  end if;
  if v_artifact.lifecycle_state = 'deleted'
     and v_artifact.gc_cleanup_state = 'complete'
     and v_artifact.gc_claim_token = p_claim_token then
    return jsonb_build_object('ok', true, 'reused', true);
  end if;
  if v_artifact.gc_claim_token is distinct from p_claim_token
     or v_artifact.gc_claim_expires_at is null
     or v_artifact.gc_claim_expires_at < now()
     or not (
       v_artifact.lifecycle_state = 'expired'
       or (
         v_artifact.lifecycle_state = 'deleted'
         and v_artifact.gc_cleanup_state = 'pending'
       )
     ) then
    return public.lcia_scope_closure_error(
      'gc_claim_invalid', 409, 'GC claim is not current'
    );
  end if;

  v_gc_phase := case
    when v_artifact.lifecycle_state = 'deleted' then 'detail_cleanup'
    else 'object_delete'
  end;

  update public.worker_job_artifacts
  set gc_claim_token = null,
      gc_claimed_at = null,
      gc_claim_expires_at = null,
      gc_failure_count = gc_failure_count + 1,
      gc_last_error = left(trim(p_error), 1000)
  where id = v_artifact.id
  returning * into v_artifact;

  return jsonb_build_object('ok', true, 'reused', false, 'data',
    jsonb_build_object(
      'artifactId', v_artifact.id,
      'state', v_artifact.lifecycle_state,
      'lifecycleState', v_artifact.lifecycle_state,
      'gcPhase', v_gc_phase,
      'objectDeleteRequired', v_gc_phase = 'object_delete',
      'failureCount', v_artifact.gc_failure_count
    )
  );
end;
$$;

revoke all on function public.svc_lcia_scope_closure_artifact_gc_claim(
  integer, integer
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_complete(
  uuid, uuid, boolean, integer
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_fail(
  uuid, uuid, text
) from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_claim(
  integer, integer
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_complete(
  uuid, uuid, boolean, integer
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_fail(
  uuid, uuid, text
) to service_role;

comment on column public.worker_job_artifacts.gc_cleanup_state is
  'DB-owned bounded detail-cleanup state: pending rows remain service-claimable after Storage object deletion.';
comment on function public.get_lcia_scope_closure_report_download(uuid, text) is
  'Actor-bound strict projection for closure_report_xlsx and closure_issue_manifest download descriptors.';
