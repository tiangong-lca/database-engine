CREATE OR REPLACE FUNCTION "public"."cmd_lca_release_publish"("p_release_run_id" "uuid", "p_approval_id" "uuid", "p_approval_hash" "text", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_credential_fingerprint" "text", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_run public.lca_release_runs%rowtype;
  v_approval public.lca_release_approvals%rowtype;
  v_existing public.lca_release_publications%rowtype;
  v_publication public.lca_release_publications%rowtype;
  v_previous public.lca_release_publications%rowtype;
  v_now timestamptz := clock_timestamp();
begin
  if v_actor is null then
    return public.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if coalesce(p_approval_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_publish_plan_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_credential_fingerprint, '') !~ '^[0-9a-f]{64}$' then
    return public.lca_release_error('invalid_publish_hash', 400, 'Approval, plan, and credential fingerprint must be lowercase SHA-256 values');
  end if;
  if nullif(trim(coalesce(p_idempotency_key, '')), '') is null then
    return public.lca_release_error('idempotency_key_required', 400, 'idempotencyKey is required');
  end if;

  select * into v_run
  from public.lca_release_runs
  where id = p_release_run_id
  for update;

  if v_run.id is null then
    return public.lca_release_error('release_run_not_found', 404, 'Release run not found');
  end if;

  select * into v_existing
  from public.lca_release_publications
  where release_run_id = v_run.id
     or (executed_by = v_actor and idempotency_key = trim(p_idempotency_key))
  order by (release_run_id = v_run.id) desc
  limit 1;

  if v_existing.id is not null then
    if v_existing.release_run_id = v_run.id
       and v_existing.executed_by = v_actor
       and v_existing.approval_id = p_approval_id
       and v_existing.approval_hash = p_approval_hash
       and v_existing.publish_plan_hash = p_publish_plan_hash
       and v_existing.idempotency_key = trim(p_idempotency_key)
       and v_existing.credential_fingerprint = p_credential_fingerprint then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'publicationId', v_existing.id,
          'releaseRunId', v_existing.release_run_id,
          'releaseVersion', v_existing.release_version,
          'status', v_existing.status,
          'publishedAt', v_existing.published_at
        )
      );
    end if;
    return public.lca_release_error('publish_idempotency_conflict', 409, 'Publish idempotency key is bound to different content');
  end if;

  if v_run.status <> 'approved' then
    return public.lca_release_error('release_not_approved', 409, 'Release run must have a durable active approval before publish');
  end if;
  if v_run.publish_plan_hash <> p_publish_plan_hash then
    return public.lca_release_error('publish_plan_hash_mismatch', 409, 'Publish plan hash differs from the prepared release');
  end if;

  select * into v_approval
  from public.lca_release_approvals
  where id = p_approval_id
    and release_run_id = v_run.id
  for update;

  if v_approval.id is null
     or v_approval.status <> 'approved'
     or v_approval.publish_plan_hash <> p_publish_plan_hash
     or v_approval.approval_hash <> p_approval_hash
     or v_approval.expires_at <= v_now then
    return public.lca_release_error('approval_invalid', 409, 'Approval is missing, expired, consumed, or does not bind the exact plan');
  end if;
  if v_run.release_manifest_hash is null
     or v_run.release_manifest->>'publishPlanHash' <> p_publish_plan_hash
     or v_run.release_manifest->>'artifactSetHash' <> v_run.artifact_set_hash
     or (select count(*) from public.lca_release_artifacts where release_run_id = v_run.id and verified_at is not null) <> 4 then
    return public.lca_release_error('release_artifacts_incomplete', 409, 'Verified manifest and all four artifacts are required before publish');
  end if;

  lock table public.lca_release_publications in exclusive mode;

  select * into v_previous
  from public.lca_release_publications
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
  for update;

  if v_previous.id is not null then
    update public.lca_release_publications
    set is_current = false,
        status = 'superseded',
        superseded_at = v_now,
        updated_at = v_now
    where id = v_previous.id;
  end if;

  insert into public.lca_release_publications (
    release_run_id, release_version, publication_series_key,
    publication_channel, visibility_scope, status, is_current,
    approval_id, approval_hash, publish_plan_hash, release_manifest_hash,
    artifact_set_hash, approved_by, executed_by, credential_fingerprint,
    idempotency_key, published_at, reason
  ) values (
    v_run.id, v_run.release_version, 'global', 'public', 'public', 'current', true,
    v_approval.id, v_approval.approval_hash, v_run.publish_plan_hash,
    v_run.release_manifest_hash, v_run.artifact_set_hash,
    v_approval.approved_by, v_actor, p_credential_fingerprint,
    trim(p_idempotency_key), v_now, nullif(trim(coalesce(p_reason, '')), '')
  )
  returning * into v_publication;

  if v_previous.id is not null then
    update public.lca_release_publications
    set superseded_by = v_publication.id,
        updated_at = v_now
    where id = v_previous.id;
  end if;

  update public.lca_release_approvals
  set status = 'consumed', consumed_by = v_actor, consumed_at = v_now
  where id = v_approval.id;

  update public.lca_release_artifacts
  set pinned = true, published_at = v_now
  where release_run_id = v_run.id;

  update public.lca_release_runs
  set status = 'published', published_at = v_now, updated_at = v_now
  where id = v_run.id;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_publish', v_actor, 'lca_release_publications', v_publication.id,
    v_run.release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'releaseRunId', v_run.id,
      'approvalId', v_approval.id,
      'approvalHash', v_approval.approval_hash,
      'publishPlanHash', v_run.publish_plan_hash,
      'releaseManifestHash', v_run.release_manifest_hash,
      'artifactSetHash', v_run.artifact_set_hash,
      'approvedBy', v_approval.approved_by,
      'executedBy', v_actor,
      'credentialFingerprint', p_credential_fingerprint,
      'previousPublicationId', v_previous.id
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'releaseRunId', v_run.id,
      'releaseVersion', v_run.release_version,
      'status', 'current',
      'approvedBy', v_approval.approved_by,
      'executedBy', v_actor,
      'publishedAt', v_now
    )
  );
exception
  when unique_violation then
    return public.lca_release_error('publication_conflict', 409, 'Release version or publish idempotency key already has a publication');
end;
$_$;

ALTER FUNCTION "public"."cmd_lca_release_publish"("p_release_run_id" "uuid", "p_approval_id" "uuid", "p_approval_hash" "text", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_credential_fingerprint" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_lca_release_publish"("p_release_run_id" "uuid", "p_approval_id" "uuid", "p_approval_hash" "text", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_credential_fingerprint" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_lca_release_publish"("p_release_run_id" "uuid", "p_approval_id" "uuid", "p_approval_hash" "text", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_credential_fingerprint" "text", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
