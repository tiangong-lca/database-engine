do $$
begin
  if to_regclass('pgmq.q_dataset_extraction_jobs') is null then
    perform pgmq.create('dataset_extraction_jobs');
  end if;
end
$$;

create table if not exists util.dataset_extraction_job_failures (
  id bigserial primary key,
  queue_name text not null default 'dataset_extraction_jobs',
  msg_id bigint not null,
  read_count integer not null default 0,
  reason text not null,
  message jsonb not null,
  last_error text,
  created_at timestamptz not null default now(),
  unique (queue_name, msg_id)
);

alter table util.dataset_extraction_job_failures owner to postgres;

grant select, insert, update on util.dataset_extraction_job_failures to service_role;
grant usage, select on sequence util.dataset_extraction_job_failures_id_seq to service_role;

create or replace function util.queue_dataset_extraction_jobs() returns trigger
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_entity_kind text;
  v_message_base jsonb;
begin
  if TG_TABLE_SCHEMA <> 'public' then
    raise exception 'dataset extraction jobs only support public schema, got %', TG_TABLE_SCHEMA;
  end if;

  v_entity_kind := case TG_TABLE_NAME
    when 'flows' then 'flow'
    when 'processes' then 'process'
    else null
  end;

  if v_entity_kind is null then
    raise exception 'unsupported dataset extraction table %', TG_TABLE_NAME;
  end if;

  v_message_base := jsonb_build_object(
    'schema', TG_TABLE_SCHEMA,
    'table', TG_TABLE_NAME,
    'id', NEW.id,
    'version', NEW.version,
    'entity_kind', v_entity_kind,
    'created_at', now()
  );

  perform pgmq.send(
    queue_name => 'dataset_extraction_jobs',
    msg => v_message_base || jsonb_build_object('extraction_kind', 'extracted_md')
  );

  perform pgmq.send(
    queue_name => 'dataset_extraction_jobs',
    msg => v_message_base || jsonb_build_object('extraction_kind', 'extracted_text')
  );

  return NEW;
end;
$$;

alter function util.queue_dataset_extraction_jobs() owner to postgres;

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
  if coalesce(current_setting('request.jwt.claim.role', true), '') <> 'service_role' then
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
  if coalesce(current_setting('request.jwt.claim.role', true), '') <> 'service_role' then
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
  if coalesce(current_setting('request.jwt.claim.role', true), '') <> 'service_role' then
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

drop trigger if exists flow_extract_md_trigger_insert on public.flows;
drop trigger if exists flow_extract_text_trigger_insert on public.flows;
drop trigger if exists flow_dataset_extraction_trigger_insert on public.flows;

create trigger flow_dataset_extraction_trigger_insert
after insert on public.flows
for each row
execute function util.queue_dataset_extraction_jobs();

create or replace function public.cmd_dataset_create(
  p_table text,
  p_id uuid,
  p_json_ordered jsonb,
  p_model_id uuid default null::uuid,
  p_rule_verification boolean default null::boolean,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $$
declare
  v_actor uuid := auth.uid();
  v_created_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table = 'lifecyclemodels' then
    return jsonb_build_object(
      'ok', false,
      'code', 'LIFECYCLEMODEL_BUNDLE_REQUIRED',
      'status', 400,
      'message', 'Lifecycle models must use bundle create and delete commands'
    );
  end if;

  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table'
    );
  end if;

  if p_json_ordered is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'JSON_ORDERED_REQUIRED',
      'status', 400,
      'message', 'jsonOrdered is required'
    );
  end if;

  if p_table <> 'processes' and p_model_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_ID_NOT_ALLOWED',
      'status', 400,
      'message', 'modelId is only allowed for process dataset creation'
    );
  end if;

  if p_table = 'flows' then
    perform set_config('lock_timeout', '2s', true);
    perform set_config('statement_timeout', '8s', true);
  end if;

  begin
    if p_table = 'processes' then
      execute format(
        'insert into public.%I as t (id, json_ordered, model_id, rule_verification)
         values ($1, $2::json, $3, $4)
         returning jsonb_build_object(
           ''id'', t.id,
           ''version'', t.version,
           ''state_code'', t.state_code,
           ''user_id'', t.user_id,
           ''team_id'', t.team_id,
           ''model_id'', t.model_id,
           ''rule_verification'', t.rule_verification
         )',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_model_id, p_rule_verification;
    else
      execute format(
        'insert into public.%I as t (id, json_ordered, rule_verification)
         values ($1, $2::json, $3)
         returning jsonb_build_object(
           ''id'', t.id,
           ''version'', t.version,
           ''state_code'', t.state_code,
           ''user_id'', t.user_id,
           ''team_id'', t.team_id,
           ''model_id'', null,
           ''rule_verification'', t.rule_verification
         )',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_rule_verification;
    end if;
  exception
    when lock_not_available then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_CREATE_LOCK_TIMEOUT',
        'status', 503,
        'message', 'Dataset creation is temporarily blocked by concurrent database work'
      );
    when query_canceled then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_CREATE_TIMEOUT',
        'status', 503,
        'message', 'Dataset creation exceeded the database timeout'
      );
    when unique_violation then
      return jsonb_build_object(
        'ok', false,
        'code', '23505',
        'status', 409,
        'message', 'Dataset with the same id and version already exists'
      );
    when not_null_violation then
      return jsonb_build_object(
        'ok', false,
        'code', '23502',
        'status', 400,
        'message', 'Dataset creation requires a valid id, version, and jsonOrdered payload'
      );
    when check_violation then
      return jsonb_build_object(
        'ok', false,
        'code', sqlstate,
        'status', 400,
        'message', sqlerrm
      );
  end;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_create',
    v_actor,
    p_table,
    p_id,
    nullif(v_created_row->>'version', ''),
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_created_row
  );
end;
$$;

alter function public.cmd_dataset_create(text, uuid, jsonb, uuid, boolean, jsonb) owner to postgres;
revoke all on function public.cmd_dataset_create(text, uuid, jsonb, uuid, boolean, jsonb) from public;
grant execute on function public.cmd_dataset_create(text, uuid, jsonb, uuid, boolean, jsonb) to anon;
grant execute on function public.cmd_dataset_create(text, uuid, jsonb, uuid, boolean, jsonb) to authenticated;
grant execute on function public.cmd_dataset_create(text, uuid, jsonb, uuid, boolean, jsonb) to service_role;

create or replace function util.process_dataset_extraction_jobs(
  batch_size integer default 5,
  visibility_timeout_seconds integer default 300,
  max_read_count integer default 5,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
set search_path to ''
as $$
begin
  if batch_size <= 0 then
    return;
  end if;

  if not pg_try_advisory_xact_lock(hashtext('util.process_dataset_extraction_jobs')) then
    return;
  end if;

  perform util.invoke_edge_function(
    name => 'process_dataset_extraction_jobs',
    body => jsonb_build_object(
      'batchSize', least(batch_size, 50),
      'visibilityTimeoutSeconds', least(greatest(visibility_timeout_seconds, 1), 3600),
      'maxReadCount', least(greatest(max_read_count, 1), 100)
    ),
    timeout_milliseconds => timeout_milliseconds
  );
end;
$$;

alter function util.process_dataset_extraction_jobs(integer, integer, integer, integer) owner to postgres;

do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('process-dataset-extraction-jobs');
    exception when others then
      null;
    end;

    perform cron.schedule(
      'process-dataset-extraction-jobs',
      '* * * * *',
      'select util.process_dataset_extraction_jobs();'
    );
  end if;
end
$$;

comment on table util.dataset_extraction_job_failures is
  'Records compact dataset extraction jobs that exceeded retry caps or were marked terminal by the Edge worker.';

comment on function util.queue_dataset_extraction_jobs() is
  'Queues compact dataset extraction jobs for asynchronous extracted_md/extracted_text generation without carrying json/json_ordered in the transaction webhook payload.';

comment on function util.process_dataset_extraction_jobs(integer, integer, integer, integer) is
  'Invokes the Edge dataset extraction worker that claims and acknowledges compact dataset extraction jobs.';
