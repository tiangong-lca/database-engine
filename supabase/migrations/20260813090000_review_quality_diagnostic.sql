-- Review submission is an admission command, not a whole-matrix quality Gate.
-- Review Admin quality diagnostics are manual, asynchronous, read-only, and
-- informational. They reuse worker_jobs without introducing a Batch,
-- revision/checksum, waiver, or approval-blocking entity.

drop function if exists api.cmd_review_submit(
  text, uuid, text, jsonb, uuid, text, text, text
);

CREATE OR REPLACE FUNCTION "api"."cmd_review_submit"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_table text := lower(coalesce(p_target_table, ''));
  v_root_row jsonb;
  v_checksum text;
  v_root_review_id uuid := gen_random_uuid();
  v_root_review private.reviews%rowtype;
  v_reference private.reviews%rowtype;
  v_target record;
  v_target_checksum text;
  v_affected jsonb := '[]'::jsonb;
  v_conflict_version text;
  v_event_key text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if v_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review submission'
    );
  end if;

  v_root_row := api.cmd_review_get_dataset_row(
    v_table,
    p_target_id,
    p_target_version,
    true
  );

  if v_root_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if nullif(v_root_row->>'user_id', '')::uuid is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROOT_DATASET_NOT_OWNED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if coalesce((v_root_row->>'state_code')::integer, 0) <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', case
        when coalesce((v_root_row->>'state_code')::integer, 0) = 100
          then 'APPROVED_DATASET_IMMUTABLE'
        else 'DATA_UNDER_REVIEW'
      end,
      'status', 409,
      'message', 'Only a Draft dataset can be submitted'
    );
  end if;

  v_checksum := private.review_revision_fingerprint_v1(v_table, v_root_row);

  if exists (
    select 1
    from private.review_candidate_root_ids_v1(
      v_table,
      p_target_id,
      p_target_version
    ) as candidate
    where private.review_root_currently_references_target_v1(
      candidate.root_review_id,
      v_table,
      p_target_id,
      p_target_version
    )
  ) then
    v_reference := private.review_get_or_create_reference_v1(
      v_table,
      v_root_row,
      v_checksum,
      v_actor
    );

    perform set_config('app.review_controlled_write', 'on', true);
    execute format(
      'update public.%I
          set state_code = 20,
              modified_at = now()
        where id = $1
          and version = $2
          and state_code = 0',
      v_table
    ) using p_target_id, p_target_version;
    perform set_config('app.review_controlled_write', 'off', true);

    insert into private.command_audit_log (
      command, actor_user_id, target_table, target_id, target_version, payload
    ) values (
      'cmd_review_submit', v_actor, v_table, p_target_id, p_target_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_id', v_reference.id,
        'review_kind', 'reference',
        'submission_mode', 'reference_repair'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'reviewId', v_reference.id,
        'reviewKind', 'reference',
        'submissionMode', 'reference_repair'
      )
    );
  end if;

  if exists (
    select 1
    from private.reviews as active_root
    where active_root.review_kind = 'root'
      and active_root.target_table = v_table
      and active_root.data_id = p_target_id
      and btrim(active_root.data_version::text) = p_target_version
      and active_root.state_code in (0, 1)
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'An active Root Review already exists'
    );
  end if;

  create temporary table if not exists review_submit_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;
  truncate table review_submit_targets;

  insert into review_submit_targets
  select *
  from api.cmd_review_collect_dataset_targets(
    jsonb_build_array(jsonb_build_object(
      'table', v_table,
      'id', p_target_id,
      'version', p_target_version,
      'is_root', true
    )),
    true
  );

  if not exists (
    select 1
    from review_submit_targets
    where is_root
      and table_name = v_table
      and dataset_id = p_target_id
      and dataset_version = p_target_version
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  for v_target in
    select *
    from review_submit_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    if nullif(v_target.dataset_row->>'user_id', '') is null then
      return jsonb_build_object(
        'ok', false,
        'code', case when v_target.is_root
          then 'ROOT_OWNER_UNRESOLVED'
          else 'REFERENCE_OWNER_UNRESOLVED'
        end,
        'status', 409,
        'message', 'Dataset owner could not be resolved'
      );
    end if;

    if not v_target.is_root
      and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor
      and not private.review_dataset_can_read_v1(
        v_actor,
        v_target.table_name,
        v_target.dataset_row
      ) then
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCE_ACCESS_DENIED',
        'status', 403,
        'message', 'A referenced dataset is not accessible'
      );
    end if;

    select btrim(active_reference.data_version::text)
    into v_conflict_version
    from private.reviews as active_reference
    where active_reference.review_kind = 'reference'
      and active_reference.target_table = v_target.table_name
      and active_reference.data_id = v_target.dataset_id
      and btrim(active_reference.data_version::text)
        <> v_target.dataset_version
      and active_reference.state_code in (0, 1)
    order by active_reference.created_at desc
    limit 1;

    if v_conflict_version is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCE_REVISION_CONFLICT',
        'status', 409,
        'message', 'Another version of a referenced dataset is under review'
      );
    end if;

    v_target_checksum := private.review_revision_fingerprint_v1(
      v_target.table_name,
      v_target.dataset_row
    );

    if not v_target.is_root then
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_target_checksum,
        v_actor
      );
    end if;
  end loop;

  insert into private.reviews (
    id,
    data_id,
    data_version,
    state_code,
    reviewer_id,
    json,
    review_kind,
    target_table,
    submitted_revision_checksum,
    target_owner_id,
    target_team_id
  )
  values (
    v_root_review_id,
    p_target_id,
    p_target_version,
    0,
    '[]'::jsonb,
    private.review_build_json_v1(
      v_table,
      v_root_row,
      v_actor,
      'submit_review',
      v_actor
    ),
    'root',
    v_table,
    v_checksum,
    v_actor,
    nullif(v_root_row->>'team_id', '')::uuid
  )
  returning * into v_root_review;

  perform set_config('app.review_controlled_write', 'on', true);
  for v_target in
    select *
    from review_submit_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    execute format(
      'update public.%I
          set state_code = case when state_code < 20 then 20 else state_code end,
              reviews = case
                when state_code < 100
                  then api.cmd_review_append_review_ref(reviews, $1)
                else reviews
              end,
              modified_at = now()
        where id = $2
          and version = $3',
      v_target.table_name
    ) using v_root_review_id, v_target.dataset_id, v_target.dataset_version;

    if not v_target.is_root
      and v_target.state_code < 20
      and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor then
      v_event_key := private.review_notify_event_v1(
        'reference_entered_review',
        (
          select reference_review.id
          from private.reviews as reference_review
          where reference_review.review_kind = 'reference'
            and reference_review.target_table = v_target.table_name
            and reference_review.data_id = v_target.dataset_id
            and btrim(reference_review.data_version::text) = v_target.dataset_version
            and reference_review.submitted_revision_checksum =
              private.review_revision_fingerprint_v1(
                v_target.table_name,
                v_target.dataset_row
              )
            and reference_review.state_code in (-1, 0, 1, 2)
          order by case when reference_review.state_code in (0, 1, 2) then 0 else 1 end,
                   reference_review.modified_at desc,
                   reference_review.id
          limit 1
        ),
        nullif(v_target.dataset_row->>'user_id', '')::uuid,
        v_actor,
        v_target.table_name,
        v_target.dataset_id,
        v_target.dataset_version,
        null,
        null,
        null
      );
    end if;

    v_affected := v_affected || jsonb_build_array(jsonb_build_object(
      'table', v_target.table_name,
      'id', v_target.dataset_id,
      'version', v_target.dataset_version,
      'previous_state_code', v_target.state_code,
      'state_code', case
        when v_target.state_code < 20 then 20
        else v_target.state_code
      end
    ));
  end loop;
  perform set_config('app.review_controlled_write', 'off', true);

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_review_submit',
    v_actor,
    v_table,
    p_target_id,
    p_target_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_id', v_root_review_id,
      'review_kind', 'root',
      'submission_mode', 'root',
      'affected_datasets', v_affected
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'reviewId', v_root_review_id,
      'reviewKind', 'root',
      'submissionMode', 'root',
      'review', to_jsonb(v_root_review),
      'affectedDatasets', v_affected
    )
  );
exception
  when others then
    perform set_config('app.review_controlled_write', 'off', true);
    if sqlerrm = 'REFERENCE_OWNER_UNRESOLVED' then
      return jsonb_build_object(
        'ok', false,
        'code', sqlerrm,
        'status', 409,
        'message', 'Review submission could not be completed'
      );
    end if;
    raise;
end;
$_$;

alter function api.cmd_review_submit(text, uuid, text, jsonb)
  owner to postgres;

revoke all on function api.cmd_review_submit(text, uuid, text, jsonb)
  from public, anon;

grant execute on function api.cmd_review_submit(text, uuid, text, jsonb)
  to api_internal_executor;

grant execute on function api.cmd_review_submit(text, uuid, text, jsonb)
  to authenticated;

comment on function api.cmd_review_submit(text, uuid, text, jsonb) is
  'Stable review-submission command. It enforces authentication, ownership, lifecycle, optimistic target, and transactional review invariants but does not run or require upstream completeness or numerical-quality diagnostics.';

create or replace function api.cmd_review_submit_v2(
  p_target_table text,
  p_target_id uuid,
  p_target_version text,
  p_gate_context jsonb default null,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language sql
security definer
set search_path = ''
as $$
  select api.cmd_review_submit(
    p_target_table,
    p_target_id,
    p_target_version,
    coalesce(p_audit, '{}'::jsonb)
      || jsonb_build_object('compatibility_entrypoint', 'cmd_review_submit_v2')
  )
$$;

alter function api.cmd_review_submit_v2(text, uuid, text, jsonb, jsonb)
  owner to postgres;

revoke all on function api.cmd_review_submit_v2(text, uuid, text, jsonb, jsonb)
  from public, anon;

grant execute on function api.cmd_review_submit_v2(text, uuid, text, jsonb, jsonb)
  to api_internal_executor;

grant execute on function api.cmd_review_submit_v2(text, uuid, text, jsonb, jsonb)
  to authenticated;

comment on function api.cmd_review_submit_v2(text, uuid, text, jsonb, jsonb) is
  'Temporary compatibility wrapper for api.cmd_review_submit. Legacy Gate context is accepted but ignored and is not persisted as review authority.';

create or replace function private.cmd_review_submit_from_job(
  p_job_id uuid,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_job private.dataset_review_submit_requests%rowtype;
  v_submit_result jsonb;
  v_error_code text;
  v_error_status integer;
  v_error_message text;
  v_prev_sub text;
  v_prev_role text;
  v_prev_claims text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to submit review from a job'
    );
  end if;

  select request.*
  into v_job
  from private.dataset_review_submit_requests as request
  where request.id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if v_job.status = 'submitted' then
    return jsonb_build_object(
      'ok', true,
      'data', private.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.status = 'cancelled' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_CANCELLED',
      'status', 409,
      'message', 'Review-submit job was cancelled',
      'details', private.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_REQUESTER_REQUIRED',
      'status', 409,
      'message', 'Review-submit job has no requester'
    );
  end if;

  v_prev_sub := current_setting('request.jwt.claim.sub', true);
  v_prev_role := current_setting('request.jwt.claim.role', true);
  v_prev_claims := current_setting('request.jwt.claims', true);

  perform set_config(
    'request.jwt.claim.sub',
    v_job.requested_by::text,
    true
  );
  perform set_config('request.jwt.claim.role', 'authenticated', true);
  perform set_config(
    'request.jwt.claims',
    jsonb_build_object(
      'sub', v_job.requested_by::text,
      'role', 'authenticated'
    )::text,
    true
  );

  v_submit_result := api.cmd_review_submit(
    v_job.dataset_table,
    v_job.dataset_id,
    v_job.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'source', 'cmd_review_submit_from_job',
      'compatibility_entrypoint', true,
      'review_submit_job_id', v_job.id
    )
  );

  perform set_config(
    'request.jwt.claim.sub',
    coalesce(v_prev_sub, ''),
    true
  );
  perform set_config(
    'request.jwt.claim.role',
    coalesce(v_prev_role, ''),
    true
  );
  perform set_config(
    'request.jwt.claims',
    coalesce(v_prev_claims, ''),
    true
  );

  if coalesce((v_submit_result->>'ok')::boolean, false) then
    update private.dataset_review_submit_requests
    set status = 'submitted',
        result = v_submit_result->'data',
        last_error_code = null,
        last_error_message = null,
        last_error_details = null,
        modified_at = now(),
        completed_at = now()
    where id = v_job.id
    returning * into v_job;

    insert into private.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    ) values (
      'cmd_review_submit_from_job',
      v_job.requested_by,
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_submit_job_id', v_job.id,
        'compatibility_entrypoint', true
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', private.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_error_code := coalesce(
    v_submit_result->>'code',
    'REVIEW_SUBMIT_JOB_ERROR'
  );
  v_error_status := coalesce(
    nullif(v_submit_result->>'status', '')::integer,
    500
  );
  v_error_message := coalesce(
    v_submit_result->>'message',
    'Review-submit job failed'
  );

  update private.dataset_review_submit_requests
  set status = 'error',
      last_error_code = v_error_code,
      last_error_message = v_error_message,
      last_error_details = jsonb_build_object(
        'submitResult', v_submit_result
      ),
      modified_at = now(),
      completed_at = now()
  where id = v_job.id
  returning * into v_job;

  return jsonb_build_object(
    'ok', false,
    'code', v_error_code,
    'status', v_error_status,
    'message', v_error_message,
    'details', private.cmd_dataset_review_submit_job_payload(v_job)
  );
exception
  when others then
    perform set_config(
      'request.jwt.claim.sub',
      coalesce(v_prev_sub, ''),
      true
    );
    perform set_config(
      'request.jwt.claim.role',
      coalesce(v_prev_role, ''),
      true
    );
    perform set_config(
      'request.jwt.claims',
      coalesce(v_prev_claims, ''),
      true
    );
    raise;
end;
$$;

alter function private.cmd_review_submit_from_job(uuid, jsonb)
  owner to postgres;

revoke all on function private.cmd_review_submit_from_job(uuid, jsonb)
  from public, anon, authenticated;

grant execute on function private.cmd_review_submit_from_job(uuid, jsonb)
  to service_role, api_internal_executor;

comment on function private.cmd_review_submit_from_job(uuid, jsonb) is
  'Temporary service-only compatibility adapter. It submits through the stable command as the original requester and never inspects or requires legacy Gate results.';

alter table private.worker_job_kinds
  drop constraint if exists worker_job_kinds_queue_check;

alter table private.worker_job_kinds
  add constraint worker_job_kinds_queue_check
  check (
    worker_queue in (
      'solver',
      'review_submit',
      'review_submit_gate',
      'review_quality',
      'package',
      'maintenance'
    )
  );

alter table private.worker_jobs
  drop constraint if exists worker_jobs_queue_check;

alter table private.worker_jobs
  add constraint worker_jobs_queue_check
  check (
    worker_queue in (
      'solver',
      'review_submit',
      'review_submit_gate',
      'review_quality',
      'package',
      'maintenance'
    )
  );

CREATE OR REPLACE FUNCTION "private"."worker_claim_jobs"("p_worker_queue" "text", "p_worker_id" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 10, "p_lease_seconds" integer DEFAULT NULL::integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_worker_queue text := lower(trim(coalesce(p_worker_queue, '')));
  v_worker_id text := nullif(trim(p_worker_id), '');
  v_limit integer := greatest(1, least(coalesce(p_limit, 10), 50));
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim worker jobs'
    );
  end if;

  if v_worker_queue not in ('solver', 'review_submit', 'review_submit_gate', 'review_quality', 'package', 'maintenance') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_QUEUE',
      'status', 400,
      'message', 'workerQueue must be solver, review_submit, review_submit_gate, review_quality, package, or maintenance'
    );
  end if;

  with expired_candidates as (
    select id
    from private.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = v_worker_queue
      and status = 'running'
      and lease_expires_at < now()
      and attempt_count >= max_attempts
    order by lease_expires_at asc, created_at asc
    limit v_limit
    for update skip locked
  ),
  expired as (
    update private.worker_jobs as j
      set status = 'failed',
          error_code = coalesce(j.error_code, 'lease_expired_max_attempts'),
          error_message = coalesce(j.error_message, 'Worker job lease expired after the maximum attempt count'),
          error_details = coalesce(j.error_details, '{}'::jsonb) || jsonb_build_object(
            'leasedBy', j.leased_by,
            'leaseExpiresAt', j.lease_expires_at,
            'attemptCount', j.attempt_count,
            'maxAttempts', j.max_attempts
          ),
          leased_by = null,
          lease_token = null,
          lease_expires_at = null,
          heartbeat_at = coalesce(j.heartbeat_at, now()),
          updated_at = now(),
          finished_at = now()
    from expired_candidates
    where j.id = expired_candidates.id
    returning j.*
  ),
  expired_events as (
    insert into private.worker_job_events (
      job_id,
      event_type,
      status,
      worker_id,
      message,
      details
    )
    select
      expired.id,
      'failed',
      expired.status,
      expired.leased_by,
      'Worker job lease expired after the maximum attempt count',
      jsonb_build_object(
        'errorCode', expired.error_code,
        'attemptCount', expired.attempt_count,
        'maxAttempts', expired.max_attempts
      )
    from expired
    returning id
  ),
  candidate as (
    select id
    from private.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = v_worker_queue
      and run_after <= now()
      and attempt_count < max_attempts
      and (
        status in ('queued', 'stale')
        or (status = 'running' and lease_expires_at < now())
      )
    order by priority desc, run_after asc, created_at asc
    limit v_limit
    for update skip locked
  ),
  updated as (
    update private.worker_jobs as j
      set status = 'running',
          attempt_count = j.attempt_count + 1,
          leased_by = v_worker_id,
          lease_token = gen_random_uuid(),
          lease_expires_at = now() + make_interval(secs => v_lease_seconds),
          heartbeat_at = now(),
          started_at = coalesce(j.started_at, now()),
          updated_at = now(),
          diagnostics = case
            when j.attempt_count > 0 then '{}'::jsonb
            else j.diagnostics
          end,
          error_code = null,
          error_message = null,
          error_details = null
    from candidate
    where j.id = candidate.id
    returning j.*
  ),
  claim_events as (
    insert into private.worker_job_events (
      job_id,
      event_type,
      status,
      phase,
      progress,
      worker_id,
      lease_token,
      details
    )
    select
      updated.id,
      'claimed',
      updated.status,
      updated.phase,
      updated.progress,
      updated.leased_by,
      updated.lease_token,
      jsonb_build_object(
        'attemptCount', updated.attempt_count,
        'leaseExpiresAt', updated.lease_expires_at
      )
    from updated
    returning id
  )
  select coalesce(jsonb_agg(private.worker_job_payload(updated, true)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

alter function private.worker_claim_jobs(text, text, integer, integer)
  owner to postgres;

revoke all on function private.worker_claim_jobs(text, text, integer, integer)
  from public, anon, authenticated;

grant execute on function private.worker_claim_jobs(text, text, integer, integer)
  to service_role, api_internal_executor;

insert into private.worker_job_kinds (
  job_kind,
  worker_runtime,
  worker_queue,
  default_visibility,
  default_priority,
  default_max_attempts,
  default_lease_seconds,
  payload_schema_version,
  result_schema_version,
  user_visible,
  description,
  task_center_category,
  task_center_surface,
  presenter_key
) values (
  'review.quality_diagnostic',
  'calculator',
  'review_quality',
  'operator',
  20,
  3,
  3600,
  'review.quality_diagnostic.request.v1',
  'review.quality_diagnostic.report.v1',
  false,
  'Manual Review Admin diagnostic for pending-review completeness and numerical evaluability.',
  null,
  null,
  null
) on conflict (job_kind) do update
set worker_runtime = excluded.worker_runtime,
    worker_queue = excluded.worker_queue,
    default_visibility = excluded.default_visibility,
    default_priority = excluded.default_priority,
    default_max_attempts = excluded.default_max_attempts,
    default_lease_seconds = excluded.default_lease_seconds,
    payload_schema_version = excluded.payload_schema_version,
    result_schema_version = excluded.result_schema_version,
    user_visible = excluded.user_visible,
    description = excluded.description,
    task_center_category = excluded.task_center_category,
    task_center_surface = excluded.task_center_surface,
    presenter_key = excluded.presenter_key,
    updated_at = now();

alter table private.worker_jobs
  drop constraint if exists worker_jobs_review_quality_diagnostic_semantics_check;

alter table private.worker_jobs
  add constraint worker_jobs_review_quality_diagnostic_semantics_check
  check (
    job_kind <> 'review.quality_diagnostic'
    or (
      worker_queue = 'review_quality'
      and visibility = 'operator'
      and status <> 'blocked'
      and cardinality(blocker_codes) = 0
      and resolution_scope is null
      and (
        status <> 'completed'
        or (
          result_schema_version = 'review.quality_diagnostic.report.v1'
          and jsonb_typeof(result_json) = 'object'
          and result_json->>'outcome' in (
            'clear',
            'findings',
            'not_evaluable'
          )
        )
      )
    )
  );

create index if not exists worker_jobs_review_quality_diagnostic_updated_idx
  on private.worker_jobs (updated_at desc, id desc)
  where job_kind = 'review.quality_diagnostic';

create or replace function private.review_quality_diagnostic_projection(
  p_job private.worker_jobs
) returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  select jsonb_strip_nulls(jsonb_build_object(
    'runId', (p_job).id,
    'status', (p_job).status,
    'outcome', (p_job).result_json->>'outcome',
    'requestedBy', (p_job).requested_by,
    'requestedAt', (p_job).created_at,
    'startedAt', (p_job).started_at,
    'finishedAt', (p_job).finished_at,
    'updatedAt', (p_job).updated_at,
    'reportSchemaVersion', (p_job).result_schema_version,
    'report', (p_job).result_json,
    'error', case
      when (p_job).error_code is null and (p_job).error_message is null
        then null
      else jsonb_strip_nulls(jsonb_build_object(
        'code', (p_job).error_code,
        'message', (p_job).error_message
      ))
    end
  ))
$$;

alter function private.review_quality_diagnostic_projection(private.worker_jobs)
  owner to postgres;

revoke all on function private.review_quality_diagnostic_projection(private.worker_jobs)
  from public, anon, authenticated, service_role;

grant execute on function private.review_quality_diagnostic_projection(private.worker_jobs)
  to api_internal_executor;

create or replace function api.cmd_review_quality_diagnostic_start()
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_job private.worker_jobs%rowtype;
  v_concurrency_key constant text := 'review.quality_diagnostic.pending_review';
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not api.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Review Admin role is required'
    );
  end if;

  select job.*
  into v_job
  from private.worker_jobs as job
  where job.job_kind = 'review.quality_diagnostic'
    and job.concurrency_key = v_concurrency_key
    and job.status in ('queued', 'running', 'waiting', 'stale')
  order by job.created_at desc, job.id desc
  limit 1;

  if found then
    return jsonb_build_object(
      'ok', true,
      'data', private.review_quality_diagnostic_projection(v_job),
      'reused', true
    );
  end if;

  insert into private.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    queue_key,
    subject_type,
    requester_type,
    requested_by,
    concurrency_key,
    status,
    phase,
    progress,
    visibility,
    max_attempts,
    payload_schema_version,
    payload_json,
    result_schema_version
  ) values (
    'review.quality_diagnostic',
    'calculator',
    'review_quality',
    20,
    'review-admin',
    'pending_review',
    'user',
    v_actor,
    v_concurrency_key,
    'queued',
    'pending_review_snapshot',
    0,
    'operator',
    3,
    'review.quality_diagnostic.request.v1',
    jsonb_build_object(
      'scope', jsonb_build_object(
        'kind', 'pending_review',
        'reviewStates', jsonb_build_array(0, 1)
      ),
      'requestedAt', now()
    ),
    'review.quality_diagnostic.report.v1'
  )
  returning * into v_job;

  insert into private.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    details
  ) values (
    v_job.id,
    'enqueued',
    v_job.status,
    v_job.phase,
    v_job.progress,
    jsonb_build_object(
      'jobKind', v_job.job_kind,
      'scopeKind', 'pending_review',
      'requestedByReviewAdmin', true
    )
  );

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    'cmd_review_quality_diagnostic_start',
    v_actor,
    'reviews',
    null,
    null,
    jsonb_build_object(
      'run_id', v_job.id,
      'scope_kind', 'pending_review'
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', private.review_quality_diagnostic_projection(v_job),
    'reused', false
  );
exception
  when unique_violation then
    select job.*
    into v_job
    from private.worker_jobs as job
    where job.job_kind = 'review.quality_diagnostic'
      and job.concurrency_key = v_concurrency_key
      and job.status in ('queued', 'running', 'waiting', 'stale')
    order by job.created_at desc, job.id desc
    limit 1;

    if found then
      return jsonb_build_object(
        'ok', true,
        'data', private.review_quality_diagnostic_projection(v_job),
        'reused', true
      );
    end if;

    raise;
end;
$$;

alter function api.cmd_review_quality_diagnostic_start()
  owner to postgres;

revoke all on function api.cmd_review_quality_diagnostic_start()
  from public, anon;

grant execute on function api.cmd_review_quality_diagnostic_start()
  to api_internal_executor, authenticated;

comment on function api.cmd_review_quality_diagnostic_start() is
  'Manually starts or reuses the active pending-review quality diagnostic. Review Admin only; the run is informational and cannot block review workflow actions.';

create or replace function api.qry_review_quality_diagnostic(
  p_run_id uuid default null
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_job private.worker_jobs%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not api.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Review Admin role is required'
    );
  end if;

  select job.*
  into v_job
  from private.worker_jobs as job
  where job.job_kind = 'review.quality_diagnostic'
    and (p_run_id is null or job.id = p_run_id)
  order by job.updated_at desc, job.id desc
  limit 1;

  if not found then
    if p_run_id is null then
      return jsonb_build_object('ok', true, 'data', null);
    end if;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_QUALITY_DIAGNOSTIC_NOT_FOUND',
      'status', 404,
      'message', 'Review quality diagnostic run not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', private.review_quality_diagnostic_projection(v_job)
  );
end;
$$;

alter function api.qry_review_quality_diagnostic(uuid)
  owner to postgres;

revoke all on function api.qry_review_quality_diagnostic(uuid)
  from public, anon;

grant execute on function api.qry_review_quality_diagnostic(uuid)
  to api_internal_executor, authenticated;

comment on function api.qry_review_quality_diagnostic(uuid) is
  'Returns one or the latest Review Admin quality diagnostic report. Findings, not-evaluable outcomes, and failures are informational only.';

delete from private.api_capability_grants
where routine_identity =
  'api.cmd_review_submit(text, uuid, text, jsonb, uuid, text, text, text)';

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
) values
  (
    'api.cmd_review_submit(text, uuid, text, jsonb)',
    'NX-CMD-01',
    false,
    true,
    false
  ),
  (
    'api.cmd_review_quality_diagnostic_start()',
    'NX-REV-01',
    false,
    true,
    false
  ),
  (
    'api.qry_review_quality_diagnostic(uuid)',
    'NX-REV-01',
    false,
    true,
    false
  )
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;
