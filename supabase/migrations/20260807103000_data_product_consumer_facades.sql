begin;

create index if not exists lcia_result_publications_list_idx
  on private.lcia_result_publications (
    is_current desc,
    published_at desc nulls last,
    created_at desc
  );

create or replace function api.svc_membership_is_review_admin(
  p_user_id uuid
)
returns jsonb
language sql
stable
security definer
set search_path = ''
as $function$
  select jsonb_build_object(
    'ok', true,
    'data', exists (
      select 1
      from private.roles as membership
      where membership.user_id = p_user_id
        and membership.role = 'review-admin'
    )
  )
$function$;

create or replace function api.svc_data_product_publication_list(
  p_limit integer default 50
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_limit integer := least(greatest(coalesce(p_limit, 50), 1), 200);
  v_rows jsonb;
begin
  if v_actor is null then
    return jsonb_build_object('ok', false, 'code', 'AUTH_REQUIRED', 'status', 401);
  end if;

  if not exists (
    select 1
    from private.roles as role_row
    where role_row.user_id = v_actor
      and role_row.team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role_row.role = 'data_product_manager'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_PRODUCT_MANAGER_REQUIRED',
      'status', 403
    );
  end if;

  select coalesce(
    jsonb_agg(
      candidate.payload
      order by candidate.is_current desc, candidate.published_at desc nulls last,
        candidate.created_at desc, candidate.publication_id
    ),
    '[]'::jsonb
  )
  into v_rows
  from (
    select
      publication.id as publication_id,
      publication.is_current,
      publication.published_at,
      publication.created_at,
      jsonb_strip_nulls(jsonb_build_object(
        'publicationId', publication.id,
        'packageId', publication.package_id,
        'packageName', coalesce(
          worker.payload_json ->> 'name',
          worker.payload_json ->> 'packageName',
          worker.payload_json ->> 'package_name'
        ),
        'packageVersion', package.package_version,
        'status', publication.status,
        'isCurrent', publication.is_current,
        'publicationSeriesKey', publication.publication_series_key,
        'publicationChannel', publication.publication_channel,
        'visibilityScope', publication.visibility_scope,
        'displayDefaultImpactCategory', publication.display_default_impact_category,
        'publishedAt', publication.published_at,
        'unpublishedAt', publication.unpublished_at,
        'reason', publication.reason,
        'eligibleInputCount', package.eligible_input_count,
        'includedInputCount', package.included_input_count,
        'packageStatus', package.status
      )) as payload
    from private.lcia_result_publications as publication
    left join private.lcia_result_packages as package on package.id = publication.package_id
    left join private.worker_jobs as worker on worker.id = package.build_worker_job_id
    order by publication.is_current desc, publication.published_at desc nulls last,
      publication.created_at desc, publication.id
    limit v_limit
  ) as candidate;

  return jsonb_build_object('ok', true, 'data', v_rows);
end
$function$;

create or replace function api.svc_data_product_worker_metadata(
  p_worker_job_ids uuid[]
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_ids uuid[] := coalesce(p_worker_job_ids, '{}'::uuid[]);
  v_worker_rows jsonb;
  v_package_rows jsonb;
begin
  if cardinality(v_ids) > 200 then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LIMIT_EXCEEDED',
      'status', 400
    );
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object('id', worker.id, 'payload_json', worker.payload_json)
      order by array_position(v_ids, worker.id)
    ),
    '[]'::jsonb
  )
  into v_worker_rows
  from private.worker_jobs as worker
  where worker.id = any(v_ids)
    and worker.job_kind = 'lcia_result.package_build';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'build_worker_job_id', package.build_worker_job_id,
        'id', package.id,
        'package_version', package.package_version,
        'status', package.status,
        'eligible_input_count', package.eligible_input_count,
        'included_input_count', package.included_input_count
      )
      order by array_position(v_ids, package.build_worker_job_id)
    ),
    '[]'::jsonb
  )
  into v_package_rows
  from private.lcia_result_packages as package
  where package.build_worker_job_id = any(v_ids);

  return jsonb_build_object(
    'ok', true,
    'worker_rows', v_worker_rows,
    'package_rows', v_package_rows
  );
end
$function$;

create or replace function api.svc_data_product_current_public_package()
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_publication private.lcia_result_publications%rowtype;
  v_package private.lcia_result_packages%rowtype;
begin
  select publication.*
  into v_publication
  from private.lcia_result_publications as publication
  where publication.publication_series_key = 'global'
    and publication.publication_channel = 'public'
    and publication.visibility_scope = 'public'
    and publication.is_current = true
    and publication.status = 'current'
  order by publication.published_at desc nulls last, publication.created_at desc,
    publication.id
  limit 1;

  if v_publication.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select package.*
  into v_package
  from private.lcia_result_packages as package
  where package.id = v_publication.package_id
    and package.status = 'preview_ready';

  if v_package.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publication', jsonb_strip_nulls(jsonb_build_object(
        'id', v_publication.id,
        'package_id', v_publication.package_id,
        'publication_series_key', v_publication.publication_series_key,
        'publication_channel', v_publication.publication_channel,
        'visibility_scope', v_publication.visibility_scope,
        'is_current', v_publication.is_current,
        'status', v_publication.status,
        'display_default_impact_category', v_publication.display_default_impact_category,
        'published_at', v_publication.published_at,
        'created_at', v_publication.created_at
      )),
      'package', jsonb_strip_nulls(jsonb_build_object(
        'id', v_package.id,
        'package_version', v_package.package_version,
        'eligible_input_count', v_package.eligible_input_count,
        'included_input_count', v_package.included_input_count,
        'input_manifest', v_package.input_manifest,
        'snapshot_id', v_package.snapshot_id,
        'result_id', v_package.result_id,
        'result_artifact_ref', v_package.result_artifact_ref,
        'query_artifact_ref', v_package.query_artifact_ref,
        'artifact_manifest', v_package.artifact_manifest,
        'available_impact_categories', v_package.available_impact_categories,
        'default_impact_category', v_package.default_impact_category,
        'status', v_package.status
      ))
    )
  );
end
$function$;

create or replace function api.svc_tidas_package_import_enqueue(
  p_requested_by uuid,
  p_job_id uuid,
  p_source_artifact_id uuid,
  p_artifact_sha256 text,
  p_artifact_byte_size bigint,
  p_filename text,
  p_content_type text default null
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_artifact private.lca_package_artifacts%rowtype;
  v_worker private.worker_jobs%rowtype;
  v_enqueue jsonb;
  v_worker_id uuid;
begin
  select * into v_artifact from private.lca_package_artifacts
  where id = p_source_artifact_id and job_id = p_job_id
  for update;

  if v_artifact.id is null or v_artifact.metadata ->> 'requested_by' <> p_requested_by::text then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_JOB_NOT_FOUND', 'status', 404);
  end if;
  if v_artifact.artifact_kind <> 'import_source' then
    return jsonb_build_object('ok', false, 'code', 'INVALID_IMPORT_SOURCE', 'status', 400);
  end if;
  if v_artifact.status = 'deleted' then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_ARTIFACT_DELETED', 'status', 410);
  end if;
  if v_artifact.expires_at is not null and v_artifact.expires_at <= now() then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_ARTIFACT_EXPIRED', 'status', 410);
  end if;
  if v_artifact.status = 'failed' then
    return jsonb_build_object('ok', false, 'code', 'IMPORT_SOURCE_NOT_USABLE', 'status', 409);
  end if;
  if p_artifact_byte_size is null or p_artifact_byte_size < 0
     or nullif(btrim(coalesce(p_artifact_sha256, '')), '') is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_IMPORT_ARTIFACT', 'status', 400);
  end if;

  if v_artifact.worker_job_id is not null then
    select * into v_worker from private.worker_jobs
    where id = v_artifact.worker_job_id
      and requested_by = p_requested_by
      and job_kind = 'tidas.import_package';
    if v_worker.status = 'completed' then
      return jsonb_build_object('ok', true, 'mode', 'completed', 'job_id', p_job_id, 'worker_job_id', v_worker.id, 'source_artifact_id', v_artifact.id);
    elsif v_worker.status in ('queued', 'running', 'waiting', 'stale') then
      return jsonb_build_object('ok', true, 'mode', 'in_progress', 'job_id', p_job_id, 'worker_job_id', v_worker.id, 'source_artifact_id', v_artifact.id);
    end if;
  end if;

  v_enqueue := private.worker_enqueue_job(
    p_job_kind => 'tidas.import_package',
    p_payload_json => jsonb_build_object(
      'type', 'import_package', 'job_id', p_job_id,
      'requested_by', p_requested_by, 'source_artifact_id', p_source_artifact_id
    ),
    p_payload_schema_version => 'tidas.import_package.request.v1',
    p_subject_type => 'lca_package_job', p_subject_id => p_job_id,
    p_subject_version => p_source_artifact_id::text,
    p_requested_by => p_requested_by, p_requester_type => 'user',
    p_idempotency_key => p_requested_by::text || ':import_package:' || p_job_id::text,
    p_request_hash => p_artifact_sha256, p_queue_key => p_source_artifact_id::text,
    p_visibility => 'user'
  );
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then
    return v_enqueue;
  end if;
  v_worker_id := (v_enqueue #>> '{data,id}')::uuid;

  update private.lca_package_artifacts set
    status = 'ready', artifact_sha256 = p_artifact_sha256,
    artifact_byte_size = p_artifact_byte_size,
    content_type = coalesce(nullif(btrim(p_content_type), ''), content_type),
    worker_job_id = v_worker_id,
    metadata = metadata || jsonb_build_object(
      'filename', coalesce(nullif(btrim(p_filename), ''), metadata ->> 'filename', 'package.zip'),
      'original_filename', coalesce(nullif(btrim(p_filename), ''), metadata ->> 'original_filename', 'package.zip'),
      'upload_state', 'uploaded', 'phase', 'enqueue_import'
    ),
    updated_at = now()
  where id = v_artifact.id;

  return jsonb_build_object('ok', true, 'mode', 'queued', 'job_id', p_job_id, 'worker_job_id', v_worker_id, 'source_artifact_id', v_artifact.id);
end
$function$;

create or replace function api.svc_tidas_package_read(
  p_requested_by uuid,
  p_lookup_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_cache private.lca_package_request_cache%rowtype;
  v_job private.worker_jobs%rowtype;
  v_effective_job_id uuid;
  v_artifacts jsonb;
begin
  select * into v_cache from private.lca_package_request_cache
  where requested_by = p_requested_by
    and (job_id = p_lookup_id or worker_job_id = p_lookup_id)
  order by updated_at desc, id limit 1;
  v_effective_job_id := coalesce(v_cache.job_id, p_lookup_id);

  select * into v_job from private.worker_jobs
  where requested_by = p_requested_by
    and job_kind in ('tidas.export_package', 'tidas.import_package')
    and (id = p_lookup_id or subject_id = v_effective_job_id)
  order by created_at desc, id limit 1;

  select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
    'id', artifact.id,
    'workerJobId', artifact.worker_job_id,
    'artifactKind', artifact.artifact_kind,
    'status', artifact.status,
    'artifactUrl', artifact.artifact_url,
    'artifactSha256', artifact.artifact_sha256,
    'artifactByteSize', artifact.artifact_byte_size,
    'artifactFormat', artifact.artifact_format,
    'contentType', artifact.content_type,
    'metadata', artifact.metadata - 'requested_by' - 'import_prepare_idempotency_key',
    'expiresAt', artifact.expires_at,
    'isPinned', artifact.is_pinned,
    'createdAt', artifact.created_at,
    'updatedAt', artifact.updated_at
  )) order by artifact.created_at, artifact.id), '[]'::jsonb)
  into v_artifacts
  from private.lca_package_artifacts as artifact
  where artifact.job_id = v_effective_job_id
    and artifact.metadata ->> 'requested_by' = p_requested_by::text;

  if v_cache.id is null and v_job.id is null and jsonb_array_length(v_artifacts) = 0 then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_strip_nulls(jsonb_build_object(
    'jobId', v_effective_job_id,
    'workerJobId', coalesce(v_job.id, v_cache.worker_job_id),
    'status', coalesce(v_job.status, v_cache.status),
    'operation', coalesce(
      v_cache.operation,
      case v_job.job_kind
        when 'tidas.export_package' then 'export_package'
        when 'tidas.import_package' then 'import_package'
      end
    ),
    'scope', v_job.payload_json ->> 'scope',
    'rootCount', coalesce(jsonb_array_length(v_job.payload_json -> 'roots'), 0),
    'requestKey', coalesce(v_cache.request_key, v_job.request_hash),
    'payload', v_job.payload_json,
    'diagnostics', v_job.diagnostics,
    'startedAt', v_job.started_at,
    'finishedAt', v_job.finished_at,
    'artifacts', v_artifacts,
    'requestCache', case when v_cache.id is null then null else jsonb_strip_nulls(jsonb_build_object(
      'id', v_cache.id,
      'status', v_cache.status,
      'error_code', v_cache.error_code,
      'error_message', v_cache.error_message,
      'hit_count', v_cache.hit_count,
      'last_accessed_at', v_cache.last_accessed_at,
      'created_at', v_cache.created_at,
      'updated_at', v_cache.updated_at,
      'export_artifact_id', v_cache.export_artifact_id,
      'report_artifact_id', v_cache.report_artifact_id
    )) end,
    'createdAt', coalesce(v_job.created_at, v_cache.created_at),
    'updatedAt', coalesce(v_job.updated_at, v_cache.updated_at)
  )));
end
$function$;

revoke execute on function api.svc_data_product_publication_list(integer)
  from public, anon, service_role;
grant execute on function api.svc_data_product_publication_list(integer) to authenticated;

revoke execute on function api.svc_membership_is_review_admin(uuid)
  from public, anon, authenticated;
revoke execute on function api.svc_data_product_worker_metadata(uuid[])
  from public, anon, authenticated;
revoke execute on function api.svc_data_product_current_public_package()
  from public, anon, authenticated;
grant execute on function api.svc_data_product_worker_metadata(uuid[]) to service_role;
grant execute on function api.svc_data_product_current_public_package() to service_role;
grant execute on function api.svc_membership_is_review_admin(uuid) to service_role;

insert into private.api_capability_grants (
  routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role
)
values
  (
    'api.svc_membership_is_review_admin(uuid)',
    'EDGE-LIFECYCLE-BUNDLE-01',
    false,
    false,
    true
  ),
  (
    'api.svc_data_product_publication_list(integer)',
    'EDGE-DATA-PRODUCT-01',
    false,
    true,
    false
  ),
  (
    'api.svc_data_product_worker_metadata(uuid[])',
    'EDGE-DATA-PRODUCT-01',
    false,
    false,
    true
  ),
  (
    'api.svc_data_product_current_public_package()',
    'EDGE-DATA-PRODUCT-01',
    false,
    false,
    true
  )
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

notify pgrst, 'reload schema';

commit;
