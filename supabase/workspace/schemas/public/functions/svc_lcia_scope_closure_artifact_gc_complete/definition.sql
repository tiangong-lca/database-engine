CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_artifact_gc_complete"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_object_missing" boolean DEFAULT false, "p_detail_limit" integer DEFAULT 10000) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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

ALTER FUNCTION "public"."svc_lcia_scope_closure_artifact_gc_complete"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_object_missing" boolean, "p_detail_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_gc_complete"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_object_missing" boolean, "p_detail_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_gc_complete"("p_artifact_id" "uuid", "p_claim_token" "uuid", "p_object_missing" boolean, "p_detail_limit" integer) TO "service_role";
