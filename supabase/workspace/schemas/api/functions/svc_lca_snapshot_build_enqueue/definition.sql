CREATE OR REPLACE FUNCTION "api"."svc_lca_snapshot_build_enqueue"("p_scope" "text", "p_process_filter" "jsonb", "p_requested_by" "uuid", "p_request_key" "text", "p_job_id" "uuid", "p_snapshot_id" "uuid", "p_payload" "jsonb", "p_payload_schema_version" "text" DEFAULT 'lca.build_snapshot.request.v2'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
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
$$;

ALTER FUNCTION "api"."svc_lca_snapshot_build_enqueue"("p_scope" "text", "p_process_filter" "jsonb", "p_requested_by" "uuid", "p_request_key" "text", "p_job_id" "uuid", "p_snapshot_id" "uuid", "p_payload" "jsonb", "p_payload_schema_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_lca_snapshot_build_enqueue"("p_scope" "text", "p_process_filter" "jsonb", "p_requested_by" "uuid", "p_request_key" "text", "p_job_id" "uuid", "p_snapshot_id" "uuid", "p_payload" "jsonb", "p_payload_schema_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_lca_snapshot_build_enqueue"("p_scope" "text", "p_process_filter" "jsonb", "p_requested_by" "uuid", "p_request_key" "text", "p_job_id" "uuid", "p_snapshot_id" "uuid", "p_payload" "jsonb", "p_payload_schema_version" "text") TO "service_role";
