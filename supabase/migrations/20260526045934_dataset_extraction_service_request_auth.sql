create or replace function util.is_service_request()
returns boolean
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_role text;
  v_claims jsonb;
  v_headers jsonb;
  v_api_key text;
  v_authorization text;
  v_bearer_token text;
  v_project_secret_key text;
begin
  v_role := nullif(current_setting('request.jwt.claim.role', true), '');

  if v_role = 'service_role' then
    return true;
  end if;

  begin
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  exception when others then
    v_claims := null;
  end;

  if v_claims->>'role' = 'service_role' then
    return true;
  end if;

  begin
    v_headers := nullif(current_setting('request.headers', true), '')::jsonb;
  exception when others then
    v_headers := null;
  end;

  if v_headers is null then
    return false;
  end if;

  select h.value #>> '{}'
  into v_api_key
  from jsonb_each(v_headers) as h(key, value)
  where lower(h.key) = 'apikey'
  limit 1;

  select h.value #>> '{}'
  into v_authorization
  from jsonb_each(v_headers) as h(key, value)
  where lower(h.key) = 'authorization'
  limit 1;

  if v_authorization ~* '^bearer[[:space:]]+' then
    v_bearer_token := regexp_replace(v_authorization, '^bearer[[:space:]]+', '', 'i');
  end if;

  if nullif(v_api_key, '') is null and nullif(v_bearer_token, '') is null then
    return false;
  end if;

  begin
    v_project_secret_key := util.project_secret_key();
  exception when others then
    return false;
  end;

  return nullif(v_project_secret_key, '') is not null
    and (
      coalesce(v_api_key = v_project_secret_key, false)
      or coalesce(v_bearer_token = v_project_secret_key, false)
    );
end;
$$;

alter function util.is_service_request() owner to postgres;
revoke all on function util.is_service_request() from public;

comment on function util.is_service_request() is
  'Returns true for service-role Data API requests, including legacy JWT claim GUCs, request.jwt.claims, and branch-local project_secret_key headers.';

create or replace function public.cmd_dataset_extraction_claim(
  p_qty integer default 10,
  p_vt_seconds integer default 300,
  p_max_read_count integer default 5
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_qty integer;
  v_vt_seconds integer;
  v_max_read_count integer;
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  v_qty := least(greatest(coalesce(p_qty, 10), 1), 50);
  v_vt_seconds := least(greatest(coalesce(p_vt_seconds, 300), 1), 3600);
  v_max_read_count := least(greatest(coalesce(p_max_read_count, 5), 1), 100);

  with expired_jobs as (
    select
      q.msg_id,
      q.message,
      q.read_ct
    from pgmq.q_dataset_extraction_jobs q
    where q.vt <= clock_timestamp()
      and q.read_ct >= v_max_read_count
    order by q.msg_id
    limit greatest(v_qty, 100)
  ),
  recorded_failures as (
    insert into util.dataset_extraction_job_failures (
      queue_name,
      msg_id,
      read_count,
      reason,
      message
    )
    select
      'dataset_extraction_jobs',
      e.msg_id,
      e.read_ct,
      format('read_ct reached retry cap %s', v_max_read_count),
      e.message
    from expired_jobs e
    on conflict (queue_name, msg_id) do update
    set
      read_count = excluded.read_count,
      reason = excluded.reason,
      message = excluded.message,
      created_at = now()
    returning msg_id
  )
  delete from pgmq.q_dataset_extraction_jobs q
  using recorded_failures f
  where q.msg_id = f.msg_id;

  with claimed_jobs as (
    select *
    from pgmq.read('dataset_extraction_jobs', v_vt_seconds, v_qty)
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'msg_id', msg_id,
        'read_ct', read_ct,
        'enqueued_at', enqueued_at,
        'vt', vt,
        'message', message
      )
      order by msg_id
    ),
    '[]'::jsonb
  )
  into v_jobs
  from claimed_jobs;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

alter function public.cmd_dataset_extraction_claim(integer, integer, integer) owner to postgres;
revoke all on function public.cmd_dataset_extraction_claim(integer, integer, integer) from public;
grant execute on function public.cmd_dataset_extraction_claim(integer, integer, integer) to service_role;

create or replace function public.cmd_dataset_extraction_ack(
  p_msg_ids bigint[]
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_deleted jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  if coalesce(array_length(p_msg_ids, 1), 0) = 0 then
    return jsonb_build_object('ok', true, 'data', jsonb_build_object('deleted_msg_ids', v_deleted));
  end if;

  select coalesce(jsonb_agg(deleted_msg_id order by deleted_msg_id), '[]'::jsonb)
  into v_deleted
  from pgmq.delete('dataset_extraction_jobs', p_msg_ids) as deleted_msg_id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object('deleted_msg_ids', v_deleted)
  );
end;
$$;

alter function public.cmd_dataset_extraction_ack(bigint[]) owner to postgres;
revoke all on function public.cmd_dataset_extraction_ack(bigint[]) from public;
grant execute on function public.cmd_dataset_extraction_ack(bigint[]) to service_role;

create or replace function public.cmd_dataset_extraction_record_failure(
  p_msg_id bigint,
  p_read_count integer,
  p_reason text,
  p_message jsonb,
  p_last_error text default null,
  p_delete boolean default true
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  insert into util.dataset_extraction_job_failures (
    queue_name,
    msg_id,
    read_count,
    reason,
    message,
    last_error
  )
  values (
    'dataset_extraction_jobs',
    p_msg_id,
    coalesce(p_read_count, 0),
    coalesce(nullif(p_reason, ''), 'worker failure'),
    coalesce(p_message, '{}'::jsonb),
    p_last_error
  )
  on conflict (queue_name, msg_id) do update
  set
    read_count = excluded.read_count,
    reason = excluded.reason,
    message = excluded.message,
    last_error = excluded.last_error,
    created_at = now();

  if coalesce(p_delete, true) then
    perform pgmq.delete('dataset_extraction_jobs', p_msg_id);
  end if;

  return jsonb_build_object('ok', true);
end;
$$;

alter function public.cmd_dataset_extraction_record_failure(bigint, integer, text, jsonb, text, boolean) owner to postgres;
revoke all on function public.cmd_dataset_extraction_record_failure(bigint, integer, text, jsonb, text, boolean) from public;
grant execute on function public.cmd_dataset_extraction_record_failure(bigint, integer, text, jsonb, text, boolean) to service_role;
