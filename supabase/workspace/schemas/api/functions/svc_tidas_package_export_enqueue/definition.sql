CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
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
$_$;

ALTER FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_export_enqueue"("p_requested_by" "uuid", "p_scope" "text", "p_roots" "jsonb", "p_request_key" "text", "p_request_payload" "jsonb", "p_job_id" "uuid", "p_idempotency_key" "text") TO "service_role";
