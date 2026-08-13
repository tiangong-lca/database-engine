begin;

set local lock_timeout = '10s';
set local statement_timeout = '10min';

create index if not exists lca_network_snapshots_ready_scope_created_idx
  on private.lca_network_snapshots (scope, created_at desc, id)
  where status = 'ready';

do $import_prepare_idempotency_preflight$
begin
  if exists (
    select 1
    from private.lca_package_artifacts
    where artifact_kind = 'import_source'
      and metadata ? 'import_prepare_idempotency_key'
      and nullif(metadata ->> 'import_prepare_idempotency_key', '') is not null
    group by
      metadata ->> 'requested_by',
      metadata ->> 'import_prepare_idempotency_key'
    having count(*) > 1
  ) then
    raise exception using
      errcode = '23505',
      message = 'duplicate import prepare idempotency keys must be reconciled before contract closure';
  end if;
end
$import_prepare_idempotency_preflight$;

create unique index if not exists lca_package_import_prepare_idempotency_uk
  on private.lca_package_artifacts (
    (metadata ->> 'requested_by'),
    (metadata ->> 'import_prepare_idempotency_key')
  )
  where artifact_kind = 'import_source'
    and metadata ? 'import_prepare_idempotency_key'
    and nullif(metadata ->> 'import_prepare_idempotency_key', '') is not null;

create or replace function api.svc_lca_snapshot_candidates(
  p_scope text,
  p_snapshot_id uuid default null,
  p_process_filter_contains jsonb default null,
  p_limit integer default 100
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_limit integer := greatest(1, least(coalesce(p_limit, 100), 100));
  v_rows jsonb;
begin
  if v_scope not in ('full_library', 'data_product') then
    return jsonb_build_object('ok', false, 'code', 'INVALID_LCA_SCOPE', 'status', 400);
  end if;

  with candidates as (
    select
      snapshot.id,
      snapshot.scope,
      snapshot.process_filter,
      snapshot.source_hash,
      snapshot.created_at,
      artifact.id as artifact_id,
      artifact.artifact_url,
      artifact.artifact_sha256,
      artifact.artifact_byte_size,
      artifact.artifact_format,
      artifact.process_count,
      artifact.flow_count,
      artifact.impact_count,
      artifact.snapshot_index_sha256,
      artifact.snapshot_build_contract_hash,
      artifact.effective_scope_hash,
      artifact.data_snapshot_token,
      artifact.closure_bundle_hash,
      active.snapshot_id is not null as is_active
    from private.lca_network_snapshots as snapshot
    join lateral (
      select candidate_artifact.*
      from private.lca_snapshot_artifacts as candidate_artifact
      where candidate_artifact.snapshot_id = snapshot.id
        and candidate_artifact.status = 'ready'
      order by candidate_artifact.created_at desc, candidate_artifact.id
      limit 1
    ) as artifact on true
    left join private.lca_active_snapshots as active
      on active.scope = v_scope and active.snapshot_id = snapshot.id
    where snapshot.status = 'ready'
      and (
        (p_snapshot_id is not null and snapshot.id = p_snapshot_id)
        or (
          p_snapshot_id is null
          and snapshot.scope in (v_scope, 'full_library')
          and (
            p_process_filter_contains is null
            or coalesce(snapshot.process_filter, '{}'::jsonb) @> p_process_filter_contains
          )
        )
      )
    order by
      case when p_snapshot_id is not null then 0 when active.snapshot_id is not null then 0 else 1 end,
      snapshot.created_at desc,
      snapshot.id
    limit v_limit
  )
  select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
    'snapshotId', id,
    'scope', scope,
    'processFilter', process_filter,
    'sourceHash', source_hash,
    'createdAt', created_at,
    'isActive', is_active,
    'artifact', jsonb_build_object(
      'artifactId', artifact_id,
      'artifactUrl', artifact_url,
      'artifactSha256', artifact_sha256,
      'artifactByteSize', artifact_byte_size,
      'artifactFormat', artifact_format,
      'processCount', process_count,
      'flowCount', flow_count,
      'impactCount', impact_count,
      'snapshotIndexSha256', snapshot_index_sha256,
      'snapshotBuildContractHash', snapshot_build_contract_hash,
      'effectiveScopeHash', effective_scope_hash,
      'dataSnapshotToken', data_snapshot_token,
      'closureBundleHash', closure_bundle_hash
    )
  ))), '[]'::jsonb)
  into v_rows
  from candidates;

  return jsonb_build_object('ok', true, 'data', v_rows);
end
$function$;

create or replace function api.svc_lca_snapshot_build_enqueue(
  p_scope text,
  p_process_filter jsonb,
  p_requested_by uuid,
  p_request_key text,
  p_job_id uuid,
  p_snapshot_id uuid,
  p_payload jsonb,
  p_payload_schema_version text default 'lca.build_snapshot.request.v2'
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_request_key text := nullif(btrim(p_request_key), '');
  v_concurrency_key text;
  v_existing private.worker_jobs%rowtype;
  v_worker jsonb;
  v_worker_id uuid;
  v_resolved_job_id uuid;
  v_resolved_snapshot_id uuid;
begin
  if v_scope not in ('full_library', 'data_product')
     or p_requested_by is null or v_request_key is null
     or p_job_id is null or p_snapshot_id is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_SNAPSHOT_BUILD_REQUEST', 'status', 400);
  end if;
  if p_process_filter is not null and jsonb_typeof(p_process_filter) <> 'object' then
    return jsonb_build_object('ok', false, 'code', 'INVALID_PROCESS_FILTER', 'status', 400);
  end if;

  v_concurrency_key := 'lca.build_snapshot:' || v_scope || ':' || v_request_key;
  perform pg_advisory_xact_lock(hashtextextended(v_concurrency_key, 0));

  select * into v_existing
  from private.worker_jobs
  where concurrency_key = v_concurrency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc, id
  limit 1;
  if found then
    v_resolved_job_id := nullif(v_existing.payload_json ->> 'job_id', '')::uuid;
    v_resolved_snapshot_id := nullif(v_existing.payload_json ->> 'snapshot_id', '')::uuid;
    if v_resolved_job_id is null or v_resolved_snapshot_id is null then
      return jsonb_build_object('ok', false, 'code', 'SNAPSHOT_BUILD_IDENTITY_MISSING', 'status', 409);
    end if;
    return jsonb_build_object(
      'ok', true,
      'mode', case when v_existing.status = 'blocked' then 'blocked' else 'in_progress' end,
      'job_id', v_resolved_job_id,
      'worker_job_id', v_existing.id,
      'snapshot_id', v_resolved_snapshot_id
    );
  end if;

  v_worker := private.worker_enqueue_job(
    p_job_kind => 'lca.build_snapshot',
    p_payload_json => coalesce(p_payload, '{}'::jsonb) || jsonb_build_object(
      'job_id', p_job_id, 'snapshot_id', p_snapshot_id, 'scope', v_scope,
      'process_filter', coalesce(p_process_filter, '{}'::jsonb)
    ),
    p_payload_schema_version => p_payload_schema_version,
    p_subject_type => 'lca_job',
    p_subject_id => p_job_id,
    p_subject_version => p_snapshot_id::text,
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => p_requested_by::text || ':lca.build_snapshot:' || v_request_key,
    p_request_hash => v_request_key,
    p_concurrency_key => v_concurrency_key,
    p_queue_key => v_scope,
    p_visibility => 'user'
  );
  if coalesce((v_worker ->> 'ok')::boolean, false) is false then return v_worker; end if;
  v_worker_id := (v_worker #>> '{data,id}')::uuid;
  v_resolved_job_id := coalesce(
    nullif(v_worker #>> '{data,payload,job_id}', '')::uuid,
    nullif(v_worker #>> '{data,subjectId}', '')::uuid,
    p_job_id
  );
  v_resolved_snapshot_id := coalesce(
    nullif(v_worker #>> '{data,payload,snapshot_id}', '')::uuid,
    nullif(v_worker #>> '{data,subjectVersion}', '')::uuid,
    p_snapshot_id
  );

  insert into private.lca_network_snapshots (
    id, scope, process_filter, status, created_by, created_at, updated_at
  ) values (
    v_resolved_snapshot_id, v_scope, p_process_filter, 'draft', p_requested_by, now(), now()
  ) on conflict (id) do nothing;

  return jsonb_build_object(
    'ok', true,
    'mode', case
      when v_worker #>> '{data,status}' = 'blocked' then 'blocked'
      when coalesce((v_worker ->> 'reused')::boolean, false) then 'in_progress'
      else 'queued'
    end,
    'job_id', v_resolved_job_id,
    'snapshot_id', v_resolved_snapshot_id,
    'worker_job_id', v_worker_id
  );
end
$function$;

create or replace function api.svc_lca_cached_job_enqueue(
  p_scope text,
  p_snapshot_id uuid,
  p_request_key text,
  p_request_payload jsonb,
  p_job_kind text,
  p_job_id uuid,
  p_payload jsonb,
  p_payload_schema_version text,
  p_requested_by uuid,
  p_idempotency_key text,
  p_queue_key text default null
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_scope text := nullif(btrim(p_scope), '');
  v_request_key text := nullif(btrim(p_request_key), '');
  v_cache private.lca_result_cache%rowtype;
  v_worker private.worker_jobs%rowtype;
  v_enqueue jsonb;
  v_worker_id uuid;
  v_resolved_job_id uuid;
  v_resolved_snapshot_id uuid;
  v_worker_status text;
begin
  if p_job_kind not in ('lca.solve_one', 'lca.solve_batch', 'lca.solve_all_unit', 'lca.contribution_path')
     or v_scope is null or p_snapshot_id is null or v_request_key is null
     or p_job_id is null or p_requested_by is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_LCA_JOB_REQUEST', 'status', 400);
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'lca.cache:' || v_scope || ':' || p_snapshot_id::text || ':' || v_request_key, 0
  ));

  select * into v_cache from private.lca_result_cache
  where scope = v_scope and snapshot_id = p_snapshot_id and request_key = v_request_key
  for update;

  if v_cache.id is not null then
    update private.lca_result_cache set
      hit_count = hit_count + 1, last_accessed_at = now(), updated_at = now()
    where id = v_cache.id returning * into v_cache;
    if v_cache.status = 'ready' and v_cache.result_id is not null then
      return jsonb_build_object(
        'ok', true, 'mode', 'cache_hit', 'cache_id', v_cache.id,
        'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id,
        'result_id', v_cache.result_id
      );
    end if;
    if v_cache.worker_job_id is not null then
      select * into v_worker from private.worker_jobs where id = v_cache.worker_job_id;
      if v_worker.status in ('queued', 'running', 'waiting', 'stale') then
        return jsonb_build_object(
          'ok', true, 'mode', 'in_progress', 'cache_id', v_cache.id,
          'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id
        );
      end if;
    end if;
  end if;

  v_enqueue := private.worker_enqueue_job(
    p_job_kind => p_job_kind,
    p_payload_json => coalesce(p_payload, '{}'::jsonb) || jsonb_build_object('job_id', p_job_id),
    p_payload_schema_version => p_payload_schema_version,
    p_subject_type => 'lca_job',
    p_subject_id => p_job_id,
    p_subject_version => p_snapshot_id::text,
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => p_idempotency_key,
    p_request_hash => v_request_key,
    p_concurrency_key => v_scope || ':' || p_snapshot_id::text || ':' || v_request_key,
    p_queue_key => p_queue_key,
    p_visibility => 'user'
  );
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then return v_enqueue; end if;
  v_worker_id := (v_enqueue #>> '{data,id}')::uuid;
  v_resolved_job_id := coalesce(
    nullif(v_enqueue #>> '{data,payload,job_id}', '')::uuid,
    nullif(v_enqueue #>> '{data,subjectId}', '')::uuid,
    p_job_id
  );
  v_resolved_snapshot_id := coalesce(
    nullif(v_enqueue #>> '{data,subjectVersion}', '')::uuid,
    p_snapshot_id
  );
  v_worker_status := v_enqueue #>> '{data,status}';
  if v_resolved_snapshot_id <> p_snapshot_id then
    return jsonb_build_object('ok', false, 'code', 'LCA_JOB_IDEMPOTENCY_CONFLICT', 'status', 409);
  end if;

  insert into private.lca_result_cache as cache (
    scope, snapshot_id, request_key, request_payload, status,
    job_id, worker_job_id, hit_count, last_accessed_at, created_at, updated_at
  ) values (
    v_scope, p_snapshot_id, v_request_key, coalesce(p_request_payload, '{}'::jsonb),
    case when v_worker_status = 'blocked' then 'failed' else 'pending' end,
    v_resolved_job_id, v_worker_id, 1, now(), now(), now()
  ) on conflict (scope, snapshot_id, request_key) do update set
    request_payload = excluded.request_payload,
    status = excluded.status,
    job_id = excluded.job_id,
    worker_job_id = excluded.worker_job_id,
    result_id = null,
    error_code = null,
    error_message = null,
    hit_count = cache.hit_count + 1,
    last_accessed_at = now(),
    updated_at = now()
  returning * into v_cache;

  return jsonb_build_object(
    'ok', true,
    'mode', case
      when v_worker_status = 'blocked' then 'blocked'
      when coalesce((v_enqueue ->> 'reused')::boolean, false) then 'in_progress'
      else 'queued'
    end,
    'cache_id', v_cache.id,
    'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id
  );
end
$function$;

create or replace function api.svc_lca_latest_all_unit_result(
  p_snapshot_id uuid,
  p_requested_by uuid
)
returns jsonb
language sql
stable
security definer
set search_path = ''
as $function$
  select jsonb_build_object(
    'ok', true,
    'data', (
      select jsonb_strip_nulls(jsonb_build_object(
        'id', result.id,
        'snapshotId', result.snapshot_id,
        'jobId', result.job_id,
        'workerJobId', result.worker_job_id,
        'resultId', result.result_id,
        'status', result.status,
        'queryArtifactUrl', result.query_artifact_url,
        'queryArtifactSha256', result.query_artifact_sha256,
        'queryArtifactByteSize', result.query_artifact_byte_size,
        'queryArtifactFormat', result.query_artifact_format,
        'computedAt', result.computed_at,
        'updatedAt', result.updated_at
      ))
      from private.lca_latest_all_unit_results as result
      join private.worker_jobs as worker on worker.id = result.worker_job_id
      where result.snapshot_id = p_snapshot_id
        and result.status = 'ready'
        and worker.job_kind = 'lca.solve_all_unit'
        and worker.requested_by = p_requested_by
      order by result.updated_at desc, result.id
      limit 1
    )
  )
$function$;

create or replace function api.svc_tidas_package_export_enqueue(
  p_requested_by uuid,
  p_scope text,
  p_roots jsonb,
  p_request_key text,
  p_request_payload jsonb,
  p_job_id uuid,
  p_idempotency_key text
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_request_key text := nullif(btrim(p_request_key), '');
  v_roots jsonb := '[]'::jsonb;
  v_root_count integer := 0;
  v_exportable_count integer := 0;
  v_request_payload jsonb;
  v_cache private.lca_package_request_cache%rowtype;
  v_worker private.worker_jobs%rowtype;
  v_enqueue jsonb;
  v_worker_id uuid;
  v_resolved_job_id uuid;
  v_worker_status text;
begin
  if p_requested_by is null or p_job_id is null or v_request_key is null
     or v_scope not in ('current_user', 'open_data', 'current_user_and_open_data', 'selected_roots')
     or jsonb_typeof(coalesce(p_roots, '[]'::jsonb)) <> 'array' then
    return jsonb_build_object('ok', false, 'code', 'INVALID_PACKAGE_EXPORT_REQUEST', 'status', 400);
  end if;
  if jsonb_array_length(coalesce(p_roots, '[]'::jsonb)) > 500 then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_ROOT_LIMIT_EXCEEDED', 'status', 400);
  end if;
  if v_scope not in ('current_user', 'selected_roots') and not exists (
    select 1 from private.roles
    where user_id = p_requested_by
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role in ('owner', 'admin')
  ) then
    return jsonb_build_object('ok', false, 'code', 'EXPORT_SCOPE_FORBIDDEN', 'status', 403);
  end if;

  if exists (
    select 1
    from jsonb_array_elements(coalesce(p_roots, '[]'::jsonb)) as root(value)
    where jsonb_typeof(root.value) <> 'object'
      or root.value ->> 'table' not in (
        'contacts', 'sources', 'unitgroups', 'flowproperties',
        'flows', 'processes', 'lifecyclemodels'
      )
      or coalesce(root.value ->> 'id', '') !~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
      or nullif(btrim(root.value ->> 'version'), '') is null
  ) then
    return jsonb_build_object('ok', false, 'code', 'INVALID_PACKAGE_ROOT', 'status', 400);
  end if;

  with normalized as (
    select distinct
      root.value ->> 'table' as table_name,
      lower(root.value ->> 'id')::uuid as id,
      btrim(root.value ->> 'version') as version
    from jsonb_array_elements(coalesce(p_roots, '[]'::jsonb)) as root(value)
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'table', normalized.table_name,
      'id', normalized.id,
      'version', normalized.version
    ) order by normalized.table_name, normalized.id, normalized.version), '[]'::jsonb),
    count(*)
  into v_roots, v_root_count
  from normalized;

  if (v_scope = 'selected_roots') <> (v_root_count > 0) then
    return jsonb_build_object('ok', false, 'code', 'PACKAGE_SCOPE_ROOTS_MISMATCH', 'status', 400);
  end if;

  if v_scope = 'selected_roots' then
    with requested as (
      select
        root.value ->> 'table' as table_name,
        (root.value ->> 'id')::uuid as id,
        root.value ->> 'version' as version
      from jsonb_array_elements(v_roots) as root(value)
    ), datasets as (
      select 'contacts'::text as table_name, id, version, user_id, state_code from public.contacts
      union all select 'sources', id, version, user_id, state_code from public.sources
      union all select 'unitgroups', id, version, user_id, state_code from public.unitgroups
      union all select 'flowproperties', id, version, user_id, state_code from public.flowproperties
      union all select 'flows', id, version, user_id, state_code from public.flows
      union all select 'processes', id, version, user_id, state_code from public.processes
      union all select 'lifecyclemodels', id, version, user_id, state_code from public.lifecyclemodels
    )
    select count(*)
    into v_exportable_count
    from requested
    join datasets using (table_name, id, version)
    where datasets.user_id = p_requested_by
       or datasets.state_code between 100 and 199;

    if v_exportable_count <> v_root_count then
      return jsonb_build_object('ok', false, 'code', 'ROOT_EXPORT_FORBIDDEN', 'status', 403);
    end if;
  end if;

  v_request_payload := coalesce(p_request_payload, '{}'::jsonb) || jsonb_build_object(
    'scope', v_scope,
    'roots', v_roots
  );

  perform pg_advisory_xact_lock(hashtextextended(
    p_requested_by::text || ':export_package:' || v_request_key, 0
  ));
  select * into v_cache from private.lca_package_request_cache
  where requested_by = p_requested_by and operation = 'export_package' and request_key = v_request_key
  for update;
  if v_cache.id is not null then
    update private.lca_package_request_cache set
      hit_count = hit_count + 1, last_accessed_at = now(), updated_at = now()
    where id = v_cache.id returning * into v_cache;
    if v_cache.status = 'ready' and v_cache.job_id is not null then
      return jsonb_build_object('ok', true, 'mode', 'cache_hit', 'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id);
    end if;
    if v_cache.worker_job_id is not null then
      select * into v_worker from private.worker_jobs where id = v_cache.worker_job_id;
      if v_worker.status in ('queued', 'running', 'waiting', 'stale', 'completed', 'blocked') then
        if v_worker.status = 'blocked' then
          update private.lca_package_request_cache
          set status = 'failed', updated_at = now()
          where id = v_cache.id
          returning * into v_cache;
        end if;
        return jsonb_build_object(
          'ok', true,
          'mode', case
            when v_worker.status = 'completed' then 'cache_hit'
            when v_worker.status = 'blocked' then 'blocked'
            else 'in_progress'
          end,
          'job_id', v_cache.job_id,
          'worker_job_id', v_cache.worker_job_id
        );
      end if;
    end if;
  end if;

  v_enqueue := private.worker_enqueue_job(
    p_job_kind => 'tidas.export_package',
    p_payload_json => jsonb_build_object(
      'type', 'export_package', 'job_id', p_job_id, 'requested_by', p_requested_by,
      'scope', v_scope, 'roots', v_roots
    ),
    p_payload_schema_version => 'tidas.export_package.request.v1',
    p_subject_type => 'lca_package_job',
    p_subject_id => p_job_id,
    p_subject_version => v_scope,
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => p_idempotency_key,
    p_request_hash => v_request_key,
    p_queue_key => v_scope,
    p_visibility => 'user'
  );
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then return v_enqueue; end if;
  v_worker_id := (v_enqueue #>> '{data,id}')::uuid;
  v_resolved_job_id := coalesce(
    nullif(v_enqueue #>> '{data,payload,job_id}', '')::uuid,
    nullif(v_enqueue #>> '{data,subjectId}', '')::uuid,
    p_job_id
  );
  v_worker_status := v_enqueue #>> '{data,status}';

  insert into private.lca_package_request_cache as cache (
    requested_by, operation, request_key, request_payload, status,
    job_id, worker_job_id, hit_count, last_accessed_at, created_at, updated_at
  ) values (
    p_requested_by, 'export_package', v_request_key, v_request_payload,
    case when v_worker_status = 'blocked' then 'failed' else 'pending' end,
    v_resolved_job_id, v_worker_id, 1, now(), now(), now()
  ) on conflict (requested_by, operation, request_key) do update set
    request_payload = excluded.request_payload,
    status = excluded.status, job_id = excluded.job_id, worker_job_id = excluded.worker_job_id,
    error_code = null, error_message = null,
    hit_count = cache.hit_count + 1, last_accessed_at = now(), updated_at = now()
  returning * into v_cache;

  return jsonb_build_object(
    'ok', true,
    'mode', case
      when v_worker_status = 'blocked' then 'blocked'
      when coalesce((v_enqueue ->> 'reused')::boolean, false) then 'in_progress'
      else 'queued'
    end,
    'job_id', v_cache.job_id,
    'worker_job_id', v_cache.worker_job_id,
    'scope', v_scope,
    'root_count', v_root_count
  );
end
$function$;

create or replace function api.svc_tidas_package_import_prepare(
  p_requested_by uuid,
  p_job_id uuid,
  p_source_artifact_id uuid,
  p_artifact_url text,
  p_content_type text,
  p_filename text,
  p_idempotency_key text default null
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_existing private.lca_package_artifacts%rowtype;
  v_idempotency_key text := nullif(btrim(coalesce(p_idempotency_key, '')), '');
begin
  if p_requested_by is null or p_job_id is null or p_source_artifact_id is null
     or nullif(btrim(p_artifact_url), '') is null or nullif(btrim(p_content_type), '') is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_IMPORT_PREPARE_REQUEST', 'status', 400);
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    p_requested_by::text || ':import_prepare:' || coalesce(v_idempotency_key, p_job_id::text), 0
  ));
  if v_idempotency_key is not null then
    select * into v_existing from private.lca_package_artifacts
    where artifact_kind = 'import_source'
      and metadata ->> 'requested_by' = p_requested_by::text
      and metadata ->> 'import_prepare_idempotency_key' = v_idempotency_key
    order by created_at desc, id limit 1;
    if found then
      return jsonb_build_object(
        'ok', true, 'mode', 'reused', 'job_id', v_existing.job_id,
        'source_artifact_id', v_existing.id, 'artifact_url', v_existing.artifact_url
      );
    end if;
  end if;

  insert into private.lca_package_artifacts (
    id, job_id, artifact_kind, status, artifact_url, artifact_format,
    content_type, metadata, created_at, updated_at
  ) values (
    p_source_artifact_id, p_job_id, 'import_source', 'pending', p_artifact_url,
    'tidas-package-zip:v1', p_content_type,
    jsonb_strip_nulls(jsonb_build_object(
      'filename', coalesce(nullif(btrim(p_filename), ''), 'package.zip'),
      'original_filename', coalesce(nullif(btrim(p_filename), ''), 'package.zip'),
      'upload_state', 'prepared',
      'requested_by', p_requested_by,
      'import_prepare_idempotency_key', v_idempotency_key
    )), now(), now()
  );
  return jsonb_build_object(
    'ok', true, 'mode', 'prepared', 'job_id', p_job_id,
    'source_artifact_id', p_source_artifact_id, 'artifact_url', p_artifact_url
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
  if v_artifact.artifact_kind <> 'import_source' or v_artifact.status in ('failed', 'deleted')
     or (v_artifact.expires_at is not null and v_artifact.expires_at <= now()) then
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
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then return v_enqueue; end if;
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
    'operation', v_cache.operation,
    'requestKey', v_cache.request_key,
    'artifacts', v_artifacts,
    'createdAt', coalesce(v_job.created_at, v_cache.created_at),
    'updatedAt', coalesce(v_job.updated_at, v_cache.updated_at)
  )));
end
$function$;

revoke execute on function api.svc_lca_snapshot_candidates(text, uuid, jsonb, integer) from public, anon, authenticated;
revoke execute on function api.svc_lca_snapshot_build_enqueue(text, jsonb, uuid, text, uuid, uuid, jsonb, text) from public, anon, authenticated;
revoke execute on function api.svc_lca_cached_job_enqueue(text, uuid, text, jsonb, text, uuid, jsonb, text, uuid, text, text) from public, anon, authenticated;
revoke execute on function api.svc_lca_latest_all_unit_result(uuid, uuid) from public, anon, authenticated;
revoke execute on function api.svc_tidas_package_export_enqueue(uuid, text, jsonb, text, jsonb, uuid, text) from public, anon, authenticated;
revoke execute on function api.svc_tidas_package_import_prepare(uuid, uuid, uuid, text, text, text, text) from public, anon, authenticated;
revoke execute on function api.svc_tidas_package_import_enqueue(uuid, uuid, uuid, text, bigint, text, text) from public, anon, authenticated;
revoke execute on function api.svc_tidas_package_read(uuid, uuid) from public, anon, authenticated;

grant execute on function api.svc_lca_snapshot_candidates(text, uuid, jsonb, integer) to service_role;
grant execute on function api.svc_lca_snapshot_build_enqueue(text, jsonb, uuid, text, uuid, uuid, jsonb, text) to service_role;
grant execute on function api.svc_lca_cached_job_enqueue(text, uuid, text, jsonb, text, uuid, jsonb, text, uuid, text, text) to service_role;
grant execute on function api.svc_lca_latest_all_unit_result(uuid, uuid) to service_role;
grant execute on function api.svc_tidas_package_export_enqueue(uuid, text, jsonb, text, jsonb, uuid, text) to service_role;
grant execute on function api.svc_tidas_package_import_prepare(uuid, uuid, uuid, text, text, text, text) to service_role;
grant execute on function api.svc_tidas_package_import_enqueue(uuid, uuid, uuid, text, bigint, text, text) to service_role;
grant execute on function api.svc_tidas_package_read(uuid, uuid) to service_role;

insert into private.api_capability_grants (
  routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role
)
select
  format(
    '%I.%I(%s)', namespace.nspname, routine.proname,
    pg_catalog.oidvectortypes(routine.proargtypes)
  ),
  case
    when routine.proname like 'svc_tidas_%' then 'EDGE-PKG-01'
    else 'EDGE-LCA-01'
  end,
  false,
  false,
  true
from pg_catalog.pg_proc as routine
join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
where namespace.nspname = 'api'
  and routine.proname = any(array[
    'svc_lca_snapshot_candidates',
    'svc_lca_snapshot_build_enqueue',
    'svc_lca_cached_job_enqueue',
    'svc_lca_latest_all_unit_result',
    'svc_tidas_package_export_enqueue',
    'svc_tidas_package_import_prepare',
    'svc_tidas_package_import_enqueue',
    'svc_tidas_package_read'
  ])
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

notify pgrst, 'reload schema';

commit;
