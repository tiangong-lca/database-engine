-- Generalize guarded derivative rebuilds to owner-draft flows and processes.
--
-- The public v1 one-process admission remains compatible.  The new batch
-- primitive is private: a later protected alias executor calls it inside the
-- same transaction that commits the desired primary rows.  It validates every
-- target before quarantining work or creating any child request.

alter table util.dataset_derivative_rebuild_requests
  drop constraint dataset_derivative_rebuild_request_table_check;

alter table util.dataset_derivative_rebuild_requests
  add constraint dataset_derivative_rebuild_request_table_check
    check (target_table in ('flows', 'processes')),
  add column batch_id uuid,
  add column batch_ordinal smallint,
  add column batch_target_count smallint,
  add column source_baseline_snapshot_sha256 text,
  add constraint dataset_derivative_rebuild_request_batch_check
    check (
      (
        batch_id is null
        and batch_ordinal is null
        and batch_target_count is null
        and source_baseline_snapshot_sha256 is null
      )
      or (
        batch_id is not null
        and batch_ordinal between 1 and 50
        and batch_target_count between 1 and 50
        and batch_ordinal <= batch_target_count
        and source_baseline_snapshot_sha256 ~ '^[a-f0-9]{64}$'
      )
    );

drop index util.dataset_derivative_rebuild_actor_plan_uidx;
create unique index dataset_derivative_rebuild_actor_plan_uidx
  on util.dataset_derivative_rebuild_requests (
    actor_user_id,
    plan_request_sha256
  )
  where batch_id is null;

create unique index dataset_derivative_rebuild_batch_ordinal_uidx
  on util.dataset_derivative_rebuild_requests (
    batch_id,
    batch_ordinal
  )
  where batch_id is not null;

create unique index dataset_derivative_rebuild_batch_target_uidx
  on util.dataset_derivative_rebuild_requests (
    batch_id,
    target_table,
    target_id,
    target_version
  )
  where batch_id is not null;

create index dataset_derivative_rebuild_batch_read_idx
  on util.dataset_derivative_rebuild_requests (
    actor_user_id,
    batch_id,
    batch_ordinal
  )
  include (
    target_table,
    target_id,
    target_version,
    status,
    completed_snapshot_sha256
  )
  where batch_id is not null;

comment on table util.dataset_derivative_rebuild_requests is
  'Private durable coordinator state for owner-draft flow/process derivative rebuilds. Nonterminal rows are target write fences; batch_id binds protected alias child sets.';

drop index public.command_audit_log_derivative_rebuild_action_uidx;
create unique index command_audit_log_derivative_rebuild_action_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'plan_request_sha256'),
    (payload ->> 'action_request_sha256')
  )
  where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
    and target_table in ('flows', 'processes')
    and payload ->> 'record_type' = 'action';

drop index public.command_audit_log_derivative_rebuild_terminal_uidx;
create unique index command_audit_log_derivative_rebuild_terminal_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'request_id')
  )
  where command = 'cmd_dataset_derivative_rebuild_terminal'
    and target_table in ('flows', 'processes')
    and payload ->> 'record_type' = 'terminal';

create or replace function util.dataset_derivative_rebuild_snapshot(
  p_flow public.flows
) returns jsonb
language plpgsql
stable
set search_path = ''
as $$
declare
  v_json_sha256 text;
  v_json_ordered_sha256 text;
  v_extracted_text_sha256 text;
  v_extracted_md_sha256 text;
  v_embedding_ft_sha256 text;
  v_snapshot jsonb;
begin
  if p_flow.json is null
    or p_flow.json_ordered is null
    or p_flow.extracted_text is null
    or p_flow.modified_at is null then
    raise exception using
      errcode = '22004',
      message = 'Derivative rebuild primary snapshot fields must be non-null';
  end if;

  v_json_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.json::jsonb::text
  );
  v_json_ordered_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.json_ordered::jsonb::text
  );
  v_extracted_text_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.extracted_text
  );
  v_extracted_md_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.extracted_md
  );
  v_embedding_ft_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.embedding_ft::text
  );

  v_snapshot := jsonb_build_object(
    'schema_version', 'dataset-derivative-snapshot.v1',
    'table', 'flows',
    'id', p_flow.id,
    'version', btrim(p_flow.version::text),
    'user_id', p_flow.user_id,
    'state_code', p_flow.state_code,
    'modified_at', p_flow.modified_at,
    'json_sha256', v_json_sha256,
    'json_ordered_sha256', v_json_ordered_sha256,
    'extracted_text_sha256', v_extracted_text_sha256,
    'extracted_md_sha256', v_extracted_md_sha256,
    'embedding_ft_sha256', v_embedding_ft_sha256,
    'embedding_ft_at', p_flow.embedding_ft_at
  );

  return v_snapshot || jsonb_build_object(
    'snapshot_sha256',
    util.dataset_derivative_rebuild_sha256(v_snapshot::text)
  );
end;
$$;

alter function util.dataset_derivative_rebuild_snapshot(public.flows)
  owner to postgres;
revoke all on function util.dataset_derivative_rebuild_snapshot(public.flows)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_snapshot(
  p_table text,
  p_id uuid,
  p_version text
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
begin
  if p_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = p_id
      and btrim(flow.version::text) = p_version;
    if v_flow.id is null then
      return null;
    end if;
    return util.dataset_derivative_rebuild_snapshot(v_flow);
  elsif p_table = 'processes' then
    select process.*
    into v_process
    from public.processes as process
    where process.id = p_id
      and btrim(process.version::text) = p_version;
    if v_process.id is null then
      return null;
    end if;
    return util.dataset_derivative_rebuild_snapshot(v_process);
  end if;

  raise exception using
    errcode = '22023',
    message = 'Derivative rebuild target table must be flows or processes';
end;
$$;

alter function util.dataset_derivative_rebuild_snapshot(text, uuid, text)
  owner to postgres;
revoke all on function util.dataset_derivative_rebuild_snapshot(text, uuid, text)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_primary_matches(
  p_request util.dataset_derivative_rebuild_requests,
  p_process public.processes
) returns boolean
language sql
stable
set search_path = ''
as $$
  select coalesce(
    p_request.target_table = 'processes'
    and p_process.id is not null
    and p_process.user_id is not null
    and p_process.state_code is not null
    and p_process.modified_at is not null
    and p_process.id = p_request.target_id
    and btrim(p_process.version::text) = p_request.target_version
    and p_process.user_id = p_request.actor_user_id
    and p_process.state_code = 0
    and p_process.modified_at = p_request.expected_modified_at
    and p_process.json is not null
    and p_process.json_ordered is not null
    and p_process.extracted_text is not null
    and util.dataset_derivative_rebuild_sha256(
      p_process.json::jsonb::text
    ) = p_request.expected_json_sha256
    and util.dataset_derivative_rebuild_sha256(
      p_process.json_ordered::jsonb::text
    ) = p_request.expected_json_ordered_sha256
    and util.dataset_derivative_rebuild_sha256(
      p_process.extracted_text
    ) = p_request.expected_extracted_text_sha256,
    false
  )
$$;

alter function util.dataset_derivative_rebuild_primary_matches(
  util.dataset_derivative_rebuild_requests,
  public.processes
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_primary_matches(
  util.dataset_derivative_rebuild_requests,
  public.processes
) from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_primary_matches(
  p_request util.dataset_derivative_rebuild_requests,
  p_flow public.flows
) returns boolean
language sql
stable
set search_path = ''
as $$
  select coalesce(
    p_request.target_table = 'flows'
    and p_flow.id is not null
    and p_flow.user_id is not null
    and p_flow.state_code is not null
    and p_flow.modified_at is not null
    and p_flow.id = p_request.target_id
    and btrim(p_flow.version::text) = p_request.target_version
    and p_flow.user_id = p_request.actor_user_id
    and p_flow.state_code = 0
    and p_flow.modified_at = p_request.expected_modified_at
    and p_flow.json is not null
    and p_flow.json_ordered is not null
    and p_flow.extracted_text is not null
    and util.dataset_derivative_rebuild_sha256(
      p_flow.json::jsonb::text
    ) = p_request.expected_json_sha256
    and util.dataset_derivative_rebuild_sha256(
      p_flow.json_ordered::jsonb::text
    ) = p_request.expected_json_ordered_sha256
    and util.dataset_derivative_rebuild_sha256(
      p_flow.extracted_text
    ) = p_request.expected_extracted_text_sha256,
    false
  )
$$;

alter function util.dataset_derivative_rebuild_primary_matches(
  util.dataset_derivative_rebuild_requests,
  public.flows
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_primary_matches(
  util.dataset_derivative_rebuild_requests,
  public.flows
) from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_primary_matches(
  p_request util.dataset_derivative_rebuild_requests
) returns boolean
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
begin
  if p_request.target_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = p_request.target_id
      and btrim(flow.version::text) = p_request.target_version;
    return util.dataset_derivative_rebuild_primary_matches(
      p_request,
      v_flow
    );
  elsif p_request.target_table = 'processes' then
    select process.*
    into v_process
    from public.processes as process
    where process.id = p_request.target_id
      and btrim(process.version::text) = p_request.target_version;
    return util.dataset_derivative_rebuild_primary_matches(
      p_request,
      v_process
    );
  end if;

  return false;
end;
$$;

alter function util.dataset_derivative_rebuild_primary_matches(
  util.dataset_derivative_rebuild_requests
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_primary_matches(
  util.dataset_derivative_rebuild_requests
) from public, anon, authenticated, service_role;

create or replace function util.guard_dataset_derivative_rebuild_primary()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_request_id uuid;
begin
  if tg_table_schema <> 'public'
    or tg_table_name not in ('flows', 'processes') then
    raise exception using
      errcode = '22023',
      message = 'Unsupported derivative rebuild fence target';
  end if;

  select request.id
  into v_request_id
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = tg_table_name
    and request.target_id = old.id
    and request.target_version = btrim(old.version::text)
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_request_id is not null then
    raise exception using
      errcode = '55006',
      message = case tg_table_name
        when 'processes'
          then 'Process primary row is fenced by an active derivative rebuild'
        else 'Flow primary row is fenced by an active derivative rebuild'
      end,
      detail = v_request_id::text;
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;

alter function util.guard_dataset_derivative_rebuild_primary()
  owner to postgres;
revoke all on function util.guard_dataset_derivative_rebuild_primary()
  from public, anon, authenticated, service_role;

drop trigger if exists flow_derivative_rebuild_primary_update_fence
  on public.flows;
create trigger flow_derivative_rebuild_primary_update_fence
before update of
  id,
  json,
  created_at,
  json_ordered,
  user_id,
  state_code,
  version,
  modified_at,
  team_id,
  extracted_text,
  embedding_at,
  review_id,
  rule_verification,
  reviews,
  embedding_flag
on public.flows
for each row
execute function util.guard_dataset_derivative_rebuild_primary();

drop trigger if exists flow_derivative_rebuild_primary_delete_fence
  on public.flows;
create trigger flow_derivative_rebuild_primary_delete_fence
before delete on public.flows
for each row
execute function util.guard_dataset_derivative_rebuild_primary();

create or replace function util.stage_dataset_derivative_rebuild_write()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_kind text := tg_argv[0];
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_proposal util.dataset_derivative_rebuild_proposals%rowtype;
  v_permitted boolean := false;
begin
  if tg_table_schema <> 'public'
    or tg_table_name not in ('flows', 'processes') then
    raise exception using
      errcode = '22023',
      message = 'Unsupported derivative rebuild staging target';
  end if;

  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = tg_table_name
    and request.target_id = old.id
    and request.target_version = btrim(old.version::text)
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_request.id is null then
    return new;
  end if;

  if not util.dataset_derivative_rebuild_primary_matches(v_request) then
    raise exception using
      errcode = '40001',
      message = 'Derivative rebuild primary fingerprint drifted';
  end if;

  select proposal.*
  into v_proposal
  from util.dataset_derivative_rebuild_permits as permit
  join util.dataset_derivative_rebuild_proposals as proposal
    on proposal.id = permit.proposal_id
   and proposal.request_id = permit.request_id
  where permit.request_id = v_request.id
    and permit.permit_kind = v_kind
    and permit.backend_pid = pg_catalog.pg_backend_pid()
    and permit.transaction_id = pg_catalog.txid_current()
    and proposal.proposal_kind = v_kind
    and (
      (v_kind = 'markdown' and proposal.status = 'accepted')
      or (v_kind = 'embedding' and proposal.status = 'captured')
    )
  limit 1;

  if v_proposal.id is not null then
    if v_kind = 'markdown' then
      v_permitted := new.extracted_md is not null
        and util.dataset_derivative_rebuild_sha256(new.extracted_md)
          = v_proposal.extracted_md_sha256;
    elsif v_kind = 'embedding' then
      v_permitted := new.embedding_ft is not null
        and new.embedding_ft_at is not null
        and util.dataset_derivative_rebuild_sha256(new.embedding_ft::text)
          = v_proposal.embedding_ft_sha256
        and util.dataset_derivative_rebuild_sha256(new.extracted_md)
          = v_proposal.source_extracted_md_sha256;
    end if;

    if not v_permitted then
      raise exception using
        errcode = '22000',
        message = 'Derivative rebuild permit does not match staged proposal';
    end if;
    return new;
  end if;

  if v_kind = 'markdown' then
    if new.extracted_md is null then
      raise exception using
        errcode = '23502',
        message = 'Fenced Markdown proposal must be non-null';
    end if;

    insert into util.dataset_derivative_rebuild_proposals (
      request_id,
      proposal_kind,
      extracted_md,
      extracted_md_sha256
    ) values (
      v_request.id,
      'markdown',
      new.extracted_md,
      util.dataset_derivative_rebuild_sha256(new.extracted_md)
    );
    new.extracted_md := old.extracted_md;
  elsif v_kind = 'embedding' then
    if new.embedding_ft is null or new.embedding_ft_at is null then
      raise exception using
        errcode = '23502',
        message = 'Fenced embedding proposal must carry vector and timestamp';
    end if;

    insert into util.dataset_derivative_rebuild_proposals (
      request_id,
      proposal_kind,
      embedding_ft,
      embedding_ft_sha256,
      embedding_ft_at,
      source_extracted_md_sha256
    ) values (
      v_request.id,
      'embedding',
      new.embedding_ft,
      util.dataset_derivative_rebuild_sha256(new.embedding_ft::text),
      new.embedding_ft_at,
      coalesce(
        v_request.accepted_extracted_md_sha256,
        util.dataset_derivative_rebuild_sha256(old.extracted_md)
      )
    );
    new.embedding_ft := old.embedding_ft;
    new.embedding_ft_at := old.embedding_ft_at;
  else
    raise exception using
      errcode = '22023',
      message = 'Unsupported derivative staging kind';
  end if;

  return new;
end;
$$;

alter function util.stage_dataset_derivative_rebuild_write()
  owner to postgres;
revoke all on function util.stage_dataset_derivative_rebuild_write()
  from public, anon, authenticated, service_role;

drop trigger if exists flow_derivative_rebuild_stage_markdown
  on public.flows;
create trigger flow_derivative_rebuild_stage_markdown
before update of extracted_md on public.flows
for each row
execute function util.stage_dataset_derivative_rebuild_write('markdown');

drop trigger if exists flow_derivative_rebuild_stage_embedding
  on public.flows;
create trigger flow_derivative_rebuild_stage_embedding
before update of embedding_ft, embedding_ft_at on public.flows
for each row
execute function util.stage_dataset_derivative_rebuild_write('embedding');

create or replace function public.flows_derivative_rebuild_embedding_input(
  p_flow public.flows
) returns text
language sql
stable
security definer
set search_path = ''
as $$
  select proposal.extracted_md
  from util.dataset_derivative_rebuild_requests as request
  join util.dataset_derivative_rebuild_proposals as proposal
    on proposal.id = request.markdown_proposal_id
   and proposal.request_id = request.id
   and proposal.proposal_kind = 'markdown'
   and proposal.status = 'accepted'
  where request.target_table = 'flows'
    and request.target_id = p_flow.id
    and request.target_version = btrim(p_flow.version::text)
    and request.status = 'embedding_pending'
    and request.accepted_extracted_md_sha256 = proposal.extracted_md_sha256
  limit 1
$$;

alter function public.flows_derivative_rebuild_embedding_input(public.flows)
  owner to postgres;
revoke all on function public.flows_derivative_rebuild_embedding_input(public.flows)
  from public, anon, authenticated, service_role;

create or replace function util.queue_embeddings() returns trigger
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_content_function text = tg_argv[0];
  v_embedding_column text = tg_argv[1];
  v_edge_function text := coalesce(tg_argv[2], 'embedding');
  v_queue_mode text;
  v_message jsonb;
begin
  if tg_table_schema = 'public'
    and tg_table_name in ('flows', 'processes')
    and exists (
      select 1
      from util.dataset_derivative_rebuild_permits as permit
      join util.dataset_derivative_rebuild_requests as request
        on request.id = permit.request_id
      where permit.permit_kind = 'markdown'
        and permit.backend_pid = pg_catalog.pg_backend_pid()
        and permit.transaction_id = pg_catalog.txid_current()
        and request.target_table = tg_table_name
        and request.target_id = new.id
        and request.target_version = btrim(new.version::text)
    ) then
    return new;
  end if;

  v_message := jsonb_build_object(
    'id', new.id,
    'version', new.version,
    'schema', tg_table_schema,
    'table', tg_table_name,
    'contentFunction', v_content_function,
    'embeddingColumn', v_embedding_column,
    'edgeFunction', v_edge_function
  );

  select policy.mode
  into v_queue_mode
  from util.embedding_queue_policy_for(
    tg_table_schema,
    tg_table_name,
    v_edge_function,
    v_embedding_column
  ) as policy;

  if v_queue_mode in ('deferred', 'paused') then
    insert into util.pending_embedding_jobs (
      schema_name,
      table_name,
      record_id,
      record_version,
      content_function,
      embedding_column,
      edge_function,
      message
    ) values (
      tg_table_schema,
      tg_table_name,
      new.id::text,
      new.version::text,
      v_content_function,
      v_embedding_column,
      v_edge_function,
      v_message
    )
    on conflict (
      schema_name,
      table_name,
      record_id,
      record_version,
      embedding_column,
      edge_function
    ) where status = 'pending'
    do update set
      content_function = excluded.content_function,
      message = excluded.message,
      updated_at = pg_catalog.clock_timestamp();
    return new;
  end if;

  perform pgmq.send(
    queue_name => 'embedding_jobs',
    msg => v_message
  );
  return new;
end;
$$;

alter function util.queue_embeddings() owner to postgres;
revoke all on function util.queue_embeddings()
  from public, anon, authenticated, service_role;

create or replace function util.guard_dataset_derivative_rebuild_embedding_visibility()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.id::text = new.message->>'requestId'
      and request.target_table = new.message->>'table'
      and request.target_table in ('flows', 'processes')
      and request.target_id::text = new.message->>'id'
      and request.target_version = btrim(new.message->>'version')
      and new.message->>'schema' = 'public'
      and new.message->>'embeddingColumn' = 'embedding_ft'
      and request.status not in ('completed', 'stale', 'failed')
  ) then
    new.vt := greatest(
      new.vt,
      pg_catalog.clock_timestamp() + interval '420 seconds'
    );
  end if;

  return new;
end;
$$;

alter function util.guard_dataset_derivative_rebuild_embedding_visibility()
  owner to postgres;
revoke all on function util.guard_dataset_derivative_rebuild_embedding_visibility()
  from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_http_body_matches(
  p_body bytea,
  p_table text,
  p_id uuid,
  p_version text
) returns boolean
language plpgsql
stable
set search_path = ''
as $$
declare
  v_body jsonb;
begin
  if p_body is null
    or p_table is null
    or p_table not in ('flows', 'processes') then
    return false;
  end if;
  v_body := pg_catalog.convert_from(p_body, 'UTF8')::jsonb;
  if jsonb_typeof(v_body) = 'object' then
    return v_body #>> '{record,id}' = p_id::text
      and btrim(v_body #>> '{record,version}') = p_version
      and coalesce(v_body->>'table', 'processes') = p_table;
  end if;

  if jsonb_typeof(v_body) = 'array' then
    return exists (
      select 1
      from jsonb_array_elements(v_body) as job(value)
      where job.value->>'id' = p_id::text
        and btrim(job.value->>'version') = p_version
        and job.value->>'schema' = 'public'
        and job.value->>'table' = p_table
        and job.value->>'embeddingColumn' = 'embedding_ft'
    );
  end if;

  return false;
exception
  when others then
    return false;
end;
$$;

alter function util.dataset_derivative_rebuild_http_body_matches(
  bytea,
  text,
  uuid,
  text
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_http_body_matches(
  bytea,
  text,
  uuid,
  text
) from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_http_body_matches(
  p_body bytea,
  p_id uuid,
  p_version text
) returns boolean
language sql
stable
set search_path = ''
as $$
  select util.dataset_derivative_rebuild_http_body_matches(
    p_body,
    'processes',
    p_id,
    p_version
  )
$$;

alter function util.dataset_derivative_rebuild_http_body_matches(
  bytea,
  uuid,
  text
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_http_body_matches(
  bytea,
  uuid,
  text
) from public, anon, authenticated, service_role;

create or replace function util.quarantine_dataset_derivative_rebuild_target(
  p_table text,
  p_id uuid,
  p_version text
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_http integer := 0;
  v_embedding integer := 0;
  v_pending integer := 0;
begin
  if p_table is null or p_table not in ('flows', 'processes') then
    raise exception using
      errcode = '22023',
      message = 'Derivative quarantine target table must be flows or processes';
  end if;

  delete from net.http_request_queue as request
  where (
      request.url like '%/functions/v1/webhook_process_embedding_ft'
      or request.url like '%/functions/v1/webhook_flow_embedding_ft'
      or request.url like '%/functions/v1/embedding_ft'
    )
    and util.dataset_derivative_rebuild_http_body_matches(
      request.body,
      p_table,
      p_id,
      p_version
    );
  get diagnostics v_http = row_count;

  delete from pgmq.q_embedding_jobs as job
  where job.message->>'id' = p_id::text
    and btrim(job.message->>'version') = p_version
    and job.message->>'schema' = 'public'
    and job.message->>'table' = p_table
    and job.message->>'embeddingColumn' = 'embedding_ft';
  get diagnostics v_embedding = row_count;

  delete from util.pending_embedding_jobs as pending
  where pending.schema_name = 'public'
    and pending.table_name = p_table
    and pending.record_id = p_id::text
    and btrim(pending.record_version) = p_version
    and pending.embedding_column = 'embedding_ft';
  get diagnostics v_pending = row_count;

  return jsonb_build_object(
    'http_requests', v_http,
    'embedding_jobs', v_embedding,
    'pending_jobs', v_pending
  );
end;
$$;

alter function util.quarantine_dataset_derivative_rebuild_target(
  text,
  uuid,
  text
) owner to postgres;
revoke all on function util.quarantine_dataset_derivative_rebuild_target(
  text,
  uuid,
  text
) from public, anon, authenticated, service_role;

create or replace function util.quarantine_dataset_derivative_rebuild_target(
  p_id uuid,
  p_version text
) returns jsonb
language sql
security definer
set search_path = ''
as $$
  select util.quarantine_dataset_derivative_rebuild_target(
    'processes',
    p_id,
    p_version
  )
$$;

alter function util.quarantine_dataset_derivative_rebuild_target(uuid, text)
  owner to postgres;
revoke all on function util.quarantine_dataset_derivative_rebuild_target(uuid, text)
  from public, anon, authenticated, service_role;

create or replace function util.commit_dataset_derivative_rebuild_proposal(
  p_request_id uuid,
  p_markdown_proposal_id bigint,
  p_embedding_proposal_id bigint
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_markdown util.dataset_derivative_rebuild_proposals%rowtype;
  v_embedding util.dataset_derivative_rebuild_proposals%rowtype;
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
  v_primary_matches boolean := false;
begin
  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.status not in ('completed', 'stale', 'failed')
  for update;

  if v_request.id is null then
    raise exception using
      errcode = 'P0002',
      message = 'Active derivative rebuild request not found';
  end if;

  select proposal.*
  into v_markdown
  from util.dataset_derivative_rebuild_proposals as proposal
  where proposal.id = p_markdown_proposal_id
    and proposal.request_id = p_request_id
    and proposal.proposal_kind = 'markdown'
    and proposal.status = 'accepted'
  for update;

  select proposal.*
  into v_embedding
  from util.dataset_derivative_rebuild_proposals as proposal
  where proposal.id = p_embedding_proposal_id
    and proposal.request_id = p_request_id
    and proposal.proposal_kind = 'embedding'
    and proposal.status = 'captured'
  for update;

  if v_markdown.id is null or v_embedding.id is null then
    raise exception using
      errcode = 'P0002',
      message = 'Accepted Markdown and captured embedding proposals are required';
  end if;

  if v_request.target_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = v_request.target_id
      and btrim(flow.version::text) = v_request.target_version
    for update;
    v_primary_matches := util.dataset_derivative_rebuild_primary_matches(
      v_request,
      v_flow
    );
  elsif v_request.target_table = 'processes' then
    select process.*
    into v_process
    from public.processes as process
    where process.id = v_request.target_id
      and btrim(process.version::text) = v_request.target_version
    for update;
    v_primary_matches := util.dataset_derivative_rebuild_primary_matches(
      v_request,
      v_process
    );
  end if;

  if not v_primary_matches then
    raise exception using
      errcode = '40001',
      message = 'Derivative rebuild primary changed before proposal commit';
  end if;

  if v_request.markdown_proposal_id is distinct from v_markdown.id
    or v_request.accepted_extracted_md_sha256
      is distinct from v_markdown.extracted_md_sha256
    or v_embedding.source_extracted_md_sha256
      is distinct from v_markdown.extracted_md_sha256 then
    raise exception using
      errcode = '40001',
      message = 'Embedding proposal is not bound to the accepted Markdown';
  end if;

  if v_embedding.embedding_ft_at
    <= coalesce(
      v_request.before_embedding_ft_at,
      '-infinity'::timestamp with time zone
    ) then
    raise exception using
      errcode = '22023',
      message = 'Embedding proposal is not newer than the frozen derivative baseline';
  end if;

  insert into util.dataset_derivative_rebuild_permits (
    request_id,
    proposal_id,
    permit_kind,
    backend_pid,
    transaction_id
  ) values
    (
      v_request.id,
      v_markdown.id,
      'markdown',
      pg_catalog.pg_backend_pid(),
      pg_catalog.txid_current()
    ),
    (
      v_request.id,
      v_embedding.id,
      'embedding',
      pg_catalog.pg_backend_pid(),
      pg_catalog.txid_current()
    );

  if v_request.target_table = 'flows' then
    update public.flows as flow
    set
      extracted_md = v_markdown.extracted_md,
      embedding_ft = v_embedding.embedding_ft,
      embedding_ft_at = v_embedding.embedding_ft_at
    where flow.id = v_request.target_id
      and btrim(flow.version::text) = v_request.target_version;
  else
    update public.processes as process
    set
      extracted_md = v_markdown.extracted_md,
      embedding_ft = v_embedding.embedding_ft,
      embedding_ft_at = v_embedding.embedding_ft_at
    where process.id = v_request.target_id
      and btrim(process.version::text) = v_request.target_version;
  end if;

  update util.dataset_derivative_rebuild_requests
  set
    embedding_proposal_id = v_embedding.id,
    updated_at = pg_catalog.clock_timestamp()
  where id = v_request.id;

  delete from util.dataset_derivative_rebuild_permits
  where request_id = v_request.id
    and proposal_id in (v_markdown.id, v_embedding.id);

  update util.dataset_derivative_rebuild_proposals
  set
    status = 'committed',
    committed_at = pg_catalog.clock_timestamp()
  where id in (v_markdown.id, v_embedding.id);
end;
$$;

alter function util.commit_dataset_derivative_rebuild_proposal(uuid, bigint, bigint)
  owner to postgres;
revoke all on function util.commit_dataset_derivative_rebuild_proposal(uuid, bigint, bigint)
  from public, anon, authenticated, service_role;

create or replace function public.cmd_dataset_derivative_rebuild_snapshot(
  p_table text,
  p_id uuid,
  p_version text
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
  v_snapshot jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table is null
    or p_table not in ('flows', 'processes')
    or p_id is null
    or p_version is null
    or btrim(p_version) !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_INVALID_REQUEST',
      'status', 400,
      'message', 'A flows or processes id and canonical version are required'
    );
  end if;

  if p_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = p_id
      and btrim(flow.version::text) = btrim(p_version)
      and flow.user_id = v_actor
      and flow.state_code = 0;
    if v_flow.id is not null then
      begin
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_flow);
      exception
        when others then
          v_snapshot := null;
      end;
    end if;
  else
    select process.*
    into v_process
    from public.processes as process
    where process.id = p_id
      and btrim(process.version::text) = btrim(p_version)
      and process.user_id = v_actor
      and process.state_code = 0;
    if v_process.id is not null then
      begin
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
      exception
        when others then
          v_snapshot := null;
      end;
    end if;
  end if;

  if (p_table = 'flows' and v_flow.id is null)
    or (p_table = 'processes' and v_process.id is null) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_NOT_AVAILABLE',
      'status', 404,
      'message', 'Owner-draft dataset snapshot is not available'
    );
  end if;

  if v_snapshot is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_NOT_READY',
      'status', 409,
      'message', 'Owner-draft dataset primary snapshot is incomplete'
    );
  end if;

  if v_snapshot->>'json_sha256'
    is distinct from v_snapshot->>'json_ordered_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_PRIMARY_MISMATCH',
      'status', 409,
      'message', 'json and json_ordered are not synchronized'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_derivative_rebuild_snapshot'
  ) || v_snapshot;
end;
$$;

alter function public.cmd_dataset_derivative_rebuild_snapshot(text, uuid, text)
  owner to postgres;
revoke all on function public.cmd_dataset_derivative_rebuild_snapshot(text, uuid, text)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_derivative_rebuild_snapshot(text, uuid, text)
  to authenticated;

comment on function public.cmd_dataset_derivative_rebuild_snapshot(text, uuid, text) is
  'Returns one current-actor state_code=0 flow or process primary/derivative fingerprint. The original one-process v1 request remains compatible.';

create or replace function util.admit_dataset_derivative_rebuild_batch(
  p_actor_user_id uuid,
  p_batch_id uuid,
  p_plan_sha256 text,
  p_operation_id text,
  p_reason_code text,
  p_targets jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
as $$
declare
  v_command constant text := 'cmd_dataset_derivative_rebuild_plan_guarded';
  v_schema_version constant text := 'dataset-derivative-rebuild-batch.v1';
  v_target jsonb;
  v_snapshot jsonb;
  v_quarantine jsonb;
  v_action jsonb;
  v_table text;
  v_id uuid;
  v_version text;
  v_expected_json_ordered_sha256 text;
  v_baseline_snapshot_sha256 text;
  v_target_count integer;
  v_flow_count integer;
  v_process_count integer;
  v_ordinal integer;
  v_action_id text;
  v_action_request_sha256 text;
  v_plan_request_sha256 text;
  v_summary_audit_id bigint;
  v_action_audit_id bigint;
  v_request_id uuid;
  v_now timestamp with time zone := pg_catalog.clock_timestamp();
  v_child_ids jsonb := '[]'::jsonb;
  v_normalized_targets jsonb;
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
begin
  if p_actor_user_id is null
    or p_batch_id is null
    or p_plan_sha256 is null
    or p_plan_sha256 !~ '^[a-f0-9]{64}$'
    or nullif(btrim(p_operation_id), '') is null
    or octet_length(p_operation_id) > 512
    or nullif(btrim(p_reason_code), '') is null
    or octet_length(p_reason_code) > 512
    or jsonb_typeof(p_targets) is distinct from 'array'
    or jsonb_array_length(p_targets) not between 1 and 50
    or pg_column_size(p_targets) > 131072 then
    raise exception using
      errcode = '22023',
      message = 'Invalid bounded derivative rebuild batch request';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_targets) as target(value)
    where jsonb_typeof(target.value) is distinct from 'object'
      or not (target.value ?& array[
        'table',
        'id',
        'version',
        'expected_json_ordered_sha256',
        'baseline_snapshot_sha256'
      ])
      or exists (
        select 1
        from jsonb_object_keys(target.value) as target_key(key)
        where target_key.key <> all (array[
          'table',
          'id',
          'version',
          'expected_json_ordered_sha256',
          'baseline_snapshot_sha256'
        ])
      )
      or jsonb_typeof(target.value->'table') is distinct from 'string'
      or target.value->>'table' not in ('flows', 'processes')
      or jsonb_typeof(target.value->'id') is distinct from 'string'
      or (target.value->>'id')
        !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or jsonb_typeof(target.value->'version') is distinct from 'string'
      or (target.value->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
      or jsonb_typeof(target.value->'expected_json_ordered_sha256')
        is distinct from 'string'
      or (target.value->>'expected_json_ordered_sha256')
        !~ '^[a-f0-9]{64}$'
      or jsonb_typeof(target.value->'baseline_snapshot_sha256')
        is distinct from 'string'
      or (target.value->>'baseline_snapshot_sha256')
        !~ '^[a-f0-9]{64}$'
  ) then
    raise exception using
      errcode = '22023',
      message = 'Derivative rebuild batch targets must match the exact schema';
  end if;

  select
    count(*)::integer,
    count(*) filter (where target.value->>'table' = 'flows')::integer,
    count(*) filter (where target.value->>'table' = 'processes')::integer
  into v_target_count, v_flow_count, v_process_count
  from jsonb_array_elements(p_targets) as target(value);

  if (
    select count(*)
    from (
      select distinct
        target.value->>'table' as target_table,
        (target.value->>'id')::uuid as target_id,
        btrim(target.value->>'version') as target_version
      from jsonb_array_elements(p_targets) as target(value)
    ) as unique_target
  ) <> v_target_count then
    raise exception using
      errcode = '22023',
      message = 'Derivative rebuild batch targets must be unique';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      p_batch_id::text,
      0
    )
  );

  if exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.batch_id = p_batch_id
  ) then
    raise exception using
      errcode = '23505',
      message = 'Derivative rebuild batch id has already been admitted';
  end if;

  -- Full validation pass.  No quarantine, audit, proposal, request, or queue
  -- effect happens until every target has passed this loop.  Stable ordered
  -- row locks below avoid adding a database-wide write lock; the protected
  -- alias caller already owns any stronger closure locks required by its own
  -- primary mutation transaction.
  for v_target in
    select target.value || jsonb_build_object('ordinal', target.ordinality)
    from jsonb_array_elements(p_targets) with ordinality as target(value, ordinality)
    order by
      target.value->>'table',
      (target.value->>'id')::uuid,
      btrim(target.value->>'version')
  loop
    v_table := v_target->>'table';
    v_id := (v_target->>'id')::uuid;
    v_version := btrim(v_target->>'version');
    v_expected_json_ordered_sha256 :=
      v_target->>'expected_json_ordered_sha256';

    if v_table = 'flows' then
      v_flow := null;
      select flow.*
      into v_flow
      from public.flows as flow
      where flow.id = v_id
        and btrim(flow.version::text) = v_version
        and flow.user_id = p_actor_user_id
        and flow.state_code = 0
      for update;
      if v_flow.id is not null then
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_flow);
      else
        v_snapshot := null;
      end if;
    else
      v_process := null;
      select process.*
      into v_process
      from public.processes as process
      where process.id = v_id
        and btrim(process.version::text) = v_version
        and process.user_id = p_actor_user_id
        and process.state_code = 0
      for update;
      if v_process.id is not null then
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
      else
        v_snapshot := null;
      end if;
    end if;

    if v_snapshot is null then
      raise exception using
        errcode = 'P0002',
        message = 'Derivative rebuild batch target is not an owner draft',
        detail = v_table || ':' || v_id::text || '@' || v_version;
    end if;

    if v_snapshot->>'json_sha256'
        is distinct from v_snapshot->>'json_ordered_sha256'
      or v_snapshot->>'json_ordered_sha256'
        is distinct from v_expected_json_ordered_sha256 then
      raise exception using
        errcode = '40001',
        message = 'Derivative rebuild batch desired primary hash drifted',
        detail = v_table || ':' || v_id::text || '@' || v_version;
    end if;

    if exists (
      select 1
      from util.dataset_derivative_rebuild_requests as request
      where request.target_table = v_table
        and request.target_id = v_id
        and request.target_version = v_version
        and request.status not in ('completed', 'stale', 'failed')
    ) then
      raise exception using
        errcode = '55006',
        message = 'Derivative rebuild batch target already has an active fence',
        detail = v_table || ':' || v_id::text || '@' || v_version;
    end if;
  end loop;

  select jsonb_agg(
    jsonb_build_object(
      'table', target.value->>'table',
      'id', (target.value->>'id')::uuid,
      'version', btrim(target.value->>'version'),
      'expected_json_ordered_sha256',
        target.value->>'expected_json_ordered_sha256',
      'baseline_snapshot_sha256',
        target.value->>'baseline_snapshot_sha256'
    )
    order by
      target.value->>'table',
      (target.value->>'id')::uuid,
      btrim(target.value->>'version')
  )
  into v_normalized_targets
  from jsonb_array_elements(p_targets) as target(value);

  v_plan_request_sha256 := util.dataset_derivative_rebuild_sha256(
    jsonb_build_object(
      'schema_version', v_schema_version,
      'batch_id', p_batch_id,
      'plan_sha256', p_plan_sha256,
      'operation_id', btrim(p_operation_id),
      'reason_code', btrim(p_reason_code),
      'targets', v_normalized_targets
    )::text
  );

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    v_command,
    p_actor_user_id,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', 'plan_summary',
      'schema_version', v_schema_version,
      'batch_id', p_batch_id,
      'plan_sha256', p_plan_sha256,
      'operation_id', btrim(p_operation_id),
      'target_visibility', 'owner_draft',
      'plan_request_sha256', v_plan_request_sha256,
      'action_count', v_target_count,
      'accepted_count', v_target_count,
      'flows', v_flow_count,
      'processes', v_process_count,
      'reason_code', btrim(p_reason_code),
      'hash_algorithm', 'postgres-jsonb-text-sha256'
    )
  )
  returning id into v_summary_audit_id;

  for v_target in
    select target.value || jsonb_build_object('ordinal', target.ordinality)
    from jsonb_array_elements(p_targets) with ordinality as target(value, ordinality)
    order by target.ordinality
  loop
    v_table := v_target->>'table';
    v_id := (v_target->>'id')::uuid;
    v_version := btrim(v_target->>'version');
    v_ordinal := (v_target->>'ordinal')::integer;
    v_expected_json_ordered_sha256 :=
      v_target->>'expected_json_ordered_sha256';
    v_baseline_snapshot_sha256 := v_target->>'baseline_snapshot_sha256';
    v_snapshot := util.dataset_derivative_rebuild_snapshot(
      v_table,
      v_id,
      v_version
    );

    if v_snapshot is null
      or v_snapshot->>'user_id' is distinct from p_actor_user_id::text
      or v_snapshot->>'state_code' is distinct from '0'
      or v_snapshot->>'json_sha256'
        is distinct from v_expected_json_ordered_sha256
      or v_snapshot->>'json_ordered_sha256'
        is distinct from v_expected_json_ordered_sha256 then
      raise exception using
        errcode = '40001',
        message = 'Derivative rebuild batch primary changed after validation';
    end if;

    v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
      v_table,
      v_id,
      v_version
    );
    v_request_id := pg_catalog.gen_random_uuid();
    v_action_id := 'batch:' || v_ordinal::text || ':'
      || v_table || ':' || v_id::text || '@' || v_version;
    v_action := jsonb_build_object(
      'schema_version', 'dataset-derivative-rebuild-batch-action.v1',
      'batch_id', p_batch_id,
      'batch_ordinal', v_ordinal,
      'action_id', v_action_id,
      'action', 'rebuild_derivatives',
      'table', v_table,
      'id', v_id,
      'version', v_version,
      'expected_state_code', 0,
      'expected_json_ordered_sha256', v_expected_json_ordered_sha256,
      'baseline_snapshot_sha256', v_baseline_snapshot_sha256,
      'post_write_snapshot_sha256', v_snapshot->>'snapshot_sha256',
      'components', jsonb_build_array('extracted_md', 'embedding_ft'),
      'reason_code', btrim(p_reason_code)
    );
    v_action_request_sha256 := util.dataset_derivative_rebuild_sha256(
      v_action::text
    );

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    ) values (
      v_command,
      p_actor_user_id,
      v_table,
      v_id,
      v_version,
      jsonb_build_object(
        'record_type', 'action',
        'schema_version', v_schema_version,
        'batch_id', p_batch_id,
        'batch_ordinal', v_ordinal,
        'batch_target_count', v_target_count,
        'request_id', v_request_id,
        'plan_sha256', p_plan_sha256,
        'operation_id', btrim(p_operation_id),
        'action_id', v_action_id,
        'target_visibility', 'owner_draft',
        'expected_snapshot_sha256', v_snapshot->>'snapshot_sha256',
        'expected_json_ordered_sha256', v_expected_json_ordered_sha256,
        'baseline_snapshot_sha256', v_baseline_snapshot_sha256,
        'plan_request_sha256', v_plan_request_sha256,
        'action_request_sha256', v_action_request_sha256,
        'reason_code', btrim(p_reason_code),
        'components', jsonb_build_array('extracted_md', 'embedding_ft'),
        'hash_algorithm', 'postgres-jsonb-text-sha256'
      )
    )
    returning id into v_action_audit_id;

    insert into util.dataset_derivative_rebuild_requests (
      id,
      actor_user_id,
      plan_sha256,
      operation_id,
      action_id,
      target_table,
      target_id,
      target_version,
      expected_snapshot_sha256,
      expected_modified_at,
      expected_json_sha256,
      expected_json_ordered_sha256,
      expected_extracted_text_sha256,
      before_extracted_md_sha256,
      before_embedding_ft_sha256,
      before_embedding_ft_at,
      plan_request_sha256,
      action_request_sha256,
      reason_code,
      status,
      phase,
      admitted_at,
      drain_not_before,
      action_audit_id,
      summary_audit_id,
      quarantined_http_requests,
      quarantined_embedding_jobs,
      quarantined_pending_jobs,
      batch_id,
      batch_ordinal,
      batch_target_count,
      source_baseline_snapshot_sha256
    ) values (
      v_request_id,
      p_actor_user_id,
      p_plan_sha256,
      btrim(p_operation_id),
      v_action_id,
      v_table,
      v_id,
      v_version,
      v_snapshot->>'snapshot_sha256',
      (v_snapshot->>'modified_at')::timestamp with time zone,
      v_snapshot->>'json_sha256',
      v_snapshot->>'json_ordered_sha256',
      v_snapshot->>'extracted_text_sha256',
      v_snapshot->>'extracted_md_sha256',
      v_snapshot->>'embedding_ft_sha256',
      (v_snapshot->>'embedding_ft_at')::timestamp with time zone,
      v_plan_request_sha256,
      v_action_request_sha256,
      btrim(p_reason_code),
      'queued',
      'admitted',
      v_now,
      v_now + interval '420 seconds',
      v_action_audit_id,
      v_summary_audit_id,
      coalesce((v_quarantine->>'http_requests')::integer, 0),
      coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
      coalesce((v_quarantine->>'pending_jobs')::integer, 0),
      p_batch_id,
      v_ordinal,
      v_target_count,
      v_baseline_snapshot_sha256
    );

    v_child_ids := v_child_ids || jsonb_build_array(
      jsonb_build_object(
        'ordinal', v_ordinal,
        'table', v_table,
        'id', v_id,
        'version', v_version,
        'request_id', v_request_id,
        'expected_snapshot_sha256', v_snapshot->>'snapshot_sha256'
      )
    );
  end loop;

  return jsonb_build_object(
    'ok', true,
    'schema_version', v_schema_version,
    'batch_id', p_batch_id,
    'plan_sha256', p_plan_sha256,
    'operation_id', btrim(p_operation_id),
    'plan_request_sha256', v_plan_request_sha256,
    'target_count', v_target_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'flows', v_flow_count,
    'processes', v_process_count,
    'summary_audit_id', v_summary_audit_id::text,
    'child_request_ids', v_child_ids
  );
exception
  when lock_not_available then
    raise exception using
      errcode = '55P03',
      message = 'Derivative rebuild batch write fence could not be acquired';
end;
$$;

alter function util.admit_dataset_derivative_rebuild_batch(
  uuid,
  uuid,
  text,
  text,
  text,
  jsonb
) owner to postgres;
revoke all on function util.admit_dataset_derivative_rebuild_batch(
  uuid,
  uuid,
  text,
  text,
  text,
  jsonb
) from public, anon, authenticated, service_role;

comment on function util.admit_dataset_derivative_rebuild_batch(
  uuid,
  uuid,
  text,
  text,
  text,
  jsonb
) is
  'Private atomic admission for 1..50 unique owner-draft flow/process derivative children. Every target is validated before any quarantine/audit/request write; batch ids are non-replayable.';

create or replace function util.process_dataset_derivative_rebuilds(
  p_limit integer default 5
) returns integer
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_json_ordered jsonb;
  v_response net._http_response%rowtype;
  v_snapshot jsonb;
  v_quarantine jsonb;
  v_service_key text;
  v_http_request_id bigint;
  v_proposal_count integer;
  v_proposal_id bigint;
  v_queue_mode text;
  v_queue_msg_id bigint;
  v_pending_job_id bigint;
  v_pending_status text;
  v_pending_msg_id bigint;
  v_pending_enqueued_at timestamp with time zone;
  v_embedding_failure jsonb;
  v_processed integer := 0;
  v_now timestamp with time zone;
  v_error_state text;
  v_error_message text;
  v_error_detail text;
begin
  if coalesce(p_limit, 0) <= 0 then
    return 0;
  end if;

  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtext('util.process_dataset_derivative_rebuilds')
  ) then
    return 0;
  end if;

  for v_request in
    select request.*
    from util.dataset_derivative_rebuild_requests as request
    where request.status not in ('completed', 'stale', 'failed')
    order by request.updated_at, request.admitted_at, request.id
    for update skip locked
    limit least(greatest(p_limit, 1), 25)
  loop
    v_processed := v_processed + 1;
    v_now := pg_catalog.clock_timestamp();
    begin

    if v_request.status = 'dispatching'
      and v_request.phase = 'failure_draining' then
      if v_request.failure_release_not_before is null
        or v_now < v_request.failure_release_not_before then
        update util.dataset_derivative_rebuild_requests
        set updated_at = v_now
        where id = v_request.id;
        continue;
      end if;

      v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );
      update util.dataset_derivative_rebuild_proposals
      set
        status = 'discarded',
        discarded_at = v_now
      where request_id = v_request.id
        and status in ('captured', 'accepted');

      update util.dataset_derivative_rebuild_requests
      set
        status = 'failed',
        phase = 'failed_drained',
        terminal_at = v_now,
        drained_at = v_now,
        quarantined_http_requests = quarantined_http_requests
          + coalesce((v_quarantine->>'http_requests')::integer, 0),
        quarantined_embedding_jobs = quarantined_embedding_jobs
          + coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
        quarantined_pending_jobs = quarantined_pending_jobs
          + coalesce((v_quarantine->>'pending_jobs')::integer, 0),
        updated_at = v_now
      where id = v_request.id;
      perform util.record_dataset_derivative_rebuild_terminal(v_request.id);
      continue;
    end if;

    v_json_ordered := null;
    if v_request.target_table = 'flows' then
      select flow.json_ordered::jsonb
      into v_json_ordered
      from public.flows as flow
      where flow.id = v_request.target_id
        and btrim(flow.version::text) = v_request.target_version;
    elsif v_request.target_table = 'processes' then
      select process.json_ordered::jsonb
      into v_json_ordered
      from public.processes as process
      where process.id = v_request.target_id
        and btrim(process.version::text) = v_request.target_version;
    end if;

    if v_json_ordered is null
      or not util.dataset_derivative_rebuild_primary_matches(v_request) then
      perform util.fail_dataset_derivative_rebuild_after_drain(
        v_request.id,
        'DERIVATIVE_PRIMARY_DRIFT',
        'Frozen dataset primary fingerprint is no longer present',
        '{}'::jsonb
      );
      continue;
    end if;

    if v_request.status = 'queued' then
      update util.dataset_derivative_rebuild_requests
      set
        status = 'dispatching',
        phase = 'quarantining',
        updated_at = v_now
      where id = v_request.id;
      continue;
    end if;

    if v_request.status = 'dispatching'
      and v_request.phase = 'quarantining' then
      if v_now < v_request.drain_not_before then
        update util.dataset_derivative_rebuild_requests
        set updated_at = v_now
        where id = v_request.id;
        continue;
      end if;

      v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );
      update util.dataset_derivative_rebuild_proposals
      set
        status = 'discarded',
        discarded_at = v_now
      where request_id = v_request.id
        and status in ('captured', 'accepted');

      v_service_key := util.project_secret_key();
      v_http_request_id := net.http_post(
        url => util.project_url()
          || case v_request.target_table
            when 'flows' then '/functions/v1/webhook_flow_embedding_ft'
            else '/functions/v1/webhook_process_embedding_ft'
          end,
        headers => jsonb_build_object(
          'Content-Type', 'application/json',
          'Authorization', 'Bearer ' || v_service_key,
          'apikey', v_service_key,
          'x_region', 'us-east-1'
        ),
        body => jsonb_build_object(
          'type', 'UPDATE',
          'schema', 'public',
          'table', v_request.target_table,
          'record', jsonb_build_object(
            'id', v_request.target_id,
            'version', v_request.target_version,
            'json_ordered', v_json_ordered
          ),
          'old_record', jsonb_build_object(
            'id', v_request.target_id,
            'version', v_request.target_version,
            'json_ordered', v_json_ordered
          ),
          'derivativeRebuildRequestId', v_request.id
        ),
        timeout_milliseconds => 120000
      );

      update util.dataset_derivative_rebuild_requests
      set
        status = 'markdown_pending',
        phase = 'markdown_dispatched',
        markdown_request_id = v_http_request_id,
        markdown_dispatched_at = v_now,
        markdown_deadline_at = v_now + interval '10 minutes',
        quarantined_http_requests = quarantined_http_requests
          + coalesce((v_quarantine->>'http_requests')::integer, 0),
        quarantined_embedding_jobs = quarantined_embedding_jobs
          + coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
        quarantined_pending_jobs = quarantined_pending_jobs
          + coalesce((v_quarantine->>'pending_jobs')::integer, 0),
        updated_at = v_now
      where id = v_request.id;
      continue;
    end if;

    if v_request.status = 'markdown_pending' then
      v_response := null;
      select response.*
      into v_response
      from net._http_response as response
      where response.id = v_request.markdown_request_id;

      if v_response.id is null then
        if v_request.markdown_deadline_at is not null
          and v_now >= v_request.markdown_deadline_at then
          delete from net.http_request_queue
          where id = v_request.markdown_request_id;
          perform util.fail_dataset_derivative_rebuild_after_drain(
            v_request.id,
            'DERIVATIVE_MARKDOWN_RESPONSE_TIMEOUT',
            'Markdown webhook did not produce a durable response before deadline',
            jsonb_build_object(
              'markdown_request_id',
              v_request.markdown_request_id::text
            )
          );
        end if;
        update util.dataset_derivative_rebuild_requests
        set updated_at = v_now
        where id = v_request.id;
        continue;
      end if;

      update util.dataset_derivative_rebuild_requests
      set
        markdown_response_status = v_response.status_code,
        markdown_response_received_at = v_response.created,
        updated_at = v_now
      where id = v_request.id;

      if coalesce(v_response.timed_out, false)
        or v_response.error_msg is not null
        or v_response.status_code not between 200 and 299
        or not util.dataset_derivative_rebuild_markdown_response_matches(
          v_response.content,
          v_request.target_table,
          v_request.target_id,
          v_request.target_version
        ) then
        perform util.fail_dataset_derivative_rebuild_after_drain(
          v_request.id,
          'DERIVATIVE_MARKDOWN_RESPONSE_INVALID',
          'Markdown webhook response was timed out, non-2xx, or malformed',
          jsonb_build_object(
            'markdown_request_id', v_request.markdown_request_id::text,
            'status_code', v_response.status_code,
            'timed_out', v_response.timed_out,
            'error_msg', v_response.error_msg
          )
        );
        continue;
      end if;

      select count(*), min(proposal.id)
      into v_proposal_count, v_proposal_id
      from util.dataset_derivative_rebuild_proposals as proposal
      where proposal.request_id = v_request.id
        and proposal.proposal_kind = 'markdown'
        and proposal.status = 'captured'
        and proposal.captured_at >= v_request.markdown_dispatched_at;

      if v_proposal_count <> 1 then
        perform util.fail_dataset_derivative_rebuild_after_drain(
          v_request.id,
          'DERIVATIVE_MARKDOWN_PROPOSAL_MISMATCH',
          'Markdown response did not correlate to exactly one staged proposal',
          jsonb_build_object('proposal_count', v_proposal_count)
        );
        continue;
      end if;

      update util.dataset_derivative_rebuild_proposals
      set status = 'accepted'
      where id = v_proposal_id
        and request_id = v_request.id
        and proposal_kind = 'markdown'
        and status = 'captured';

      update util.dataset_derivative_rebuild_requests
      set
        markdown_proposal_id = v_proposal_id,
        accepted_extracted_md_sha256 = (
          select proposal.extracted_md_sha256
          from util.dataset_derivative_rebuild_proposals as proposal
          where proposal.id = v_proposal_id
        ),
        updated_at = v_now
      where id = v_request.id;

      select policy.mode
      into v_queue_mode
      from util.embedding_queue_policy_for(
        'public',
        v_request.target_table,
        'embedding_ft',
        'embedding_ft'
      ) as policy;

      if v_queue_mode = 'normal' then
        select pgmq.send(
          queue_name => 'embedding_jobs',
          msg => jsonb_build_object(
            'id', v_request.target_id,
            'version', v_request.target_version,
            'schema', 'public',
            'table', v_request.target_table,
            'contentFunction',
              case v_request.target_table
                when 'flows'
                  then 'flows_derivative_rebuild_embedding_input'
                else 'processes_derivative_rebuild_embedding_input'
              end,
            'embeddingColumn', 'embedding_ft',
            'edgeFunction', 'embedding_ft',
            'requestId', v_request.id,
            'expectedPrimarySnapshotSha256',
              v_request.expected_snapshot_sha256,
            'expectedMarkdownSha256', (
              select proposal.extracted_md_sha256
              from util.dataset_derivative_rebuild_proposals as proposal
              where proposal.id = v_proposal_id
            )
          )
        ) into v_queue_msg_id;

        update util.dataset_derivative_rebuild_requests
        set
          status = 'embedding_pending',
          phase = 'embedding_queued',
          embedding_queue_msg_id = v_queue_msg_id,
          embedding_queued_at = v_now,
          embedding_deadline_at = v_now + interval '7 days',
          updated_at = v_now
        where id = v_request.id;
      else
        insert into util.pending_embedding_jobs (
          schema_name,
          table_name,
          record_id,
          record_version,
          content_function,
          embedding_column,
          edge_function,
          message
        ) values (
          'public',
          v_request.target_table,
          v_request.target_id::text,
          v_request.target_version,
          case v_request.target_table
                when 'flows'
                  then 'flows_derivative_rebuild_embedding_input'
                else 'processes_derivative_rebuild_embedding_input'
              end,
          'embedding_ft',
          'embedding_ft',
          jsonb_build_object(
            'id', v_request.target_id,
            'version', v_request.target_version,
            'schema', 'public',
            'table', v_request.target_table,
            'contentFunction',
              case v_request.target_table
                when 'flows'
                  then 'flows_derivative_rebuild_embedding_input'
                else 'processes_derivative_rebuild_embedding_input'
              end,
            'embeddingColumn', 'embedding_ft',
            'edgeFunction', 'embedding_ft',
            'requestId', v_request.id,
            'expectedPrimarySnapshotSha256',
              v_request.expected_snapshot_sha256,
            'expectedMarkdownSha256', (
              select proposal.extracted_md_sha256
              from util.dataset_derivative_rebuild_proposals as proposal
              where proposal.id = v_proposal_id
            )
          )
        )
        on conflict (
          schema_name,
          table_name,
          record_id,
          record_version,
          embedding_column,
          edge_function
        ) where status = 'pending'
        do update set
          message = excluded.message,
          updated_at = v_now
        returning id into v_pending_job_id;

        update util.dataset_derivative_rebuild_requests
        set
          status = 'embedding_pending',
          phase = case
            when v_queue_mode = 'paused'
              then 'embedding_policy_paused'
            else 'embedding_policy_deferred'
          end,
          embedding_pending_job_id = v_pending_job_id,
          embedding_queued_at = v_now,
          embedding_deadline_at = v_now + interval '7 days',
          updated_at = v_now
        where id = v_request.id;
      end if;
      continue;
    end if;

    if v_request.status = 'embedding_pending' then
      v_queue_msg_id := v_request.embedding_queue_msg_id;
      v_pending_status := null;
      v_pending_msg_id := null;
      v_pending_enqueued_at := null;
      v_embedding_failure := null;

      if v_queue_msg_id is null
        and v_request.embedding_pending_job_id is not null then
        select pending.status, pending.queue_msg_id, pending.enqueued_at
        into v_pending_status, v_pending_msg_id, v_pending_enqueued_at
        from util.pending_embedding_jobs as pending
        where pending.id = v_request.embedding_pending_job_id;

        if v_pending_status = 'pending' then
          if v_request.embedding_deadline_at is not null
            and v_now >= v_request.embedding_deadline_at then
            delete from util.pending_embedding_jobs
            where id = v_request.embedding_pending_job_id;
            perform util.fail_dataset_derivative_rebuild_after_drain(
              v_request.id,
              'DERIVATIVE_EMBEDDING_POLICY_TIMEOUT',
              'Embedding policy did not release the request before deadline',
              '{}'::jsonb
            );
          end if;
          update util.dataset_derivative_rebuild_requests
          set updated_at = v_now
          where id = v_request.id;
          continue;
        end if;

        if v_pending_status = 'enqueued'
          and v_pending_msg_id is not null
          and v_pending_enqueued_at is not null then
          v_queue_msg_id := v_pending_msg_id;
          update util.dataset_derivative_rebuild_requests
          set
            phase = 'embedding_queued',
            embedding_queue_msg_id = v_queue_msg_id,
            embedding_queued_at = v_pending_enqueued_at,
            updated_at = v_now
          where id = v_request.id;
        else
          perform util.fail_dataset_derivative_rebuild_after_drain(
            v_request.id,
            'DERIVATIVE_EMBEDDING_PENDING_JOB_LOST',
            'Request-specific pending embedding job is missing or discarded',
            '{}'::jsonb
          );
          continue;
        end if;
      end if;

      if v_queue_msg_id is null then
        perform util.fail_dataset_derivative_rebuild_after_drain(
          v_request.id,
          'DERIVATIVE_EMBEDDING_QUEUE_PROOF_MISSING',
          'Request-specific embedding queue id is missing',
          '{}'::jsonb
        );
        continue;
      end if;

      if exists (
        select 1
        from pgmq.q_embedding_jobs as job
        where job.msg_id = v_queue_msg_id
      ) then
        if v_request.embedding_deadline_at is not null
          and v_now >= v_request.embedding_deadline_at then
          delete from pgmq.q_embedding_jobs
          where msg_id = v_queue_msg_id;
          perform util.fail_dataset_derivative_rebuild_after_drain(
            v_request.id,
            'DERIVATIVE_EMBEDDING_QUEUE_TIMEOUT',
            'Embedding job did not finish before deadline',
            jsonb_build_object('queue_msg_id', v_queue_msg_id::text)
          );
        end if;
        update util.dataset_derivative_rebuild_requests
        set updated_at = v_now
        where id = v_request.id;
        continue;
      end if;

      select jsonb_build_object(
        'failure_id', failure.id::text,
        'reason', failure.reason,
        'failed_at', failure.failed_at
      )
      into v_embedding_failure
      from util.embedding_job_failures as failure
      where failure.msg_id = v_queue_msg_id
      order by failure.id desc
      limit 1;

      if v_embedding_failure is not null then
        perform util.fail_dataset_derivative_rebuild_after_drain(
          v_request.id,
          'DERIVATIVE_EMBEDDING_JOB_FAILED',
          'Request-specific embedding job reached the failure ledger',
          v_embedding_failure
        );
        continue;
      end if;

      select count(*), min(proposal.id)
      into v_proposal_count, v_proposal_id
      from util.dataset_derivative_rebuild_proposals as proposal
      where proposal.request_id = v_request.id
        and proposal.proposal_kind = 'embedding'
        and proposal.status = 'captured'
        and proposal.captured_at >= v_request.embedding_queued_at;

      if v_proposal_count <> 1 then
        perform util.fail_dataset_derivative_rebuild_after_drain(
          v_request.id,
          'DERIVATIVE_EMBEDDING_PROPOSAL_MISMATCH',
          'Embedding ACK did not correlate to exactly one staged proposal',
          jsonb_build_object(
            'queue_msg_id', v_queue_msg_id::text,
            'proposal_count', v_proposal_count
          )
        );
        continue;
      end if;

      perform util.commit_dataset_derivative_rebuild_proposal(
        v_request.id,
        v_request.markdown_proposal_id,
        v_proposal_id
      );

      v_snapshot := util.dataset_derivative_rebuild_snapshot(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );

      if v_snapshot->>'embedding_ft_sha256' is null
        or v_snapshot->>'extracted_md_sha256' is null
        or (v_snapshot->>'embedding_ft_at')::timestamp with time zone
          <= coalesce(
            v_request.before_embedding_ft_at,
            '-infinity'::timestamp with time zone
          ) then
        raise exception using
          errcode = '40001',
          message = 'Committed derivative snapshot is not fresh and complete';
      end if;

      update util.dataset_derivative_rebuild_requests
      set
        status = 'completed',
        phase = 'completed',
        embedding_proposal_id = v_proposal_id,
        completed_snapshot_sha256 = v_snapshot->>'snapshot_sha256',
        completed_at = v_now,
        terminal_at = v_now,
        drained_at = v_now,
        updated_at = v_now
      where id = v_request.id;
      perform util.record_dataset_derivative_rebuild_terminal(v_request.id);
    end if;
    exception
      when others then
        get stacked diagnostics
          v_error_state = returned_sqlstate,
          v_error_message = message_text,
          v_error_detail = pg_exception_detail;
        perform util.fail_dataset_derivative_rebuild_after_drain(
          v_request.id,
          'DERIVATIVE_COORDINATOR_ERROR',
          'Unexpected request-scoped coordinator error',
          jsonb_build_object(
            'sqlstate', v_error_state,
            'message', v_error_message,
            'detail', v_error_detail
          )
        );
    end;
  end loop;

  return v_processed;
end;
$$;

alter function util.process_dataset_derivative_rebuilds(integer)
  owner to postgres;
revoke all on function util.process_dataset_derivative_rebuilds(integer)
  from public, anon, authenticated, service_role;

comment on function util.process_dataset_derivative_rebuilds(integer) is
  'Private bounded coordinator for fenced flow/process derivative rebuilds. It never waits on network calls in-transaction; pg_net/PGMQ work is correlated through durable ids and staged proposals.';



create or replace function util.dataset_derivative_rebuild_markdown_response_matches(
  p_content text,
  p_table text,
  p_id uuid,
  p_version text
) returns boolean
language plpgsql
immutable
set search_path = ''
as $$
declare
  v_content jsonb;
begin
  if p_table is null or p_table not in ('flows', 'processes') then
    return false;
  end if;
  v_content := p_content::jsonb;
  return v_content->>'success' = 'true'
    and jsonb_typeof(v_content->'results') = 'array'
    and jsonb_array_length(v_content->'results') = 1
    and v_content #>> '{results,0,id}' = p_id::text
    and btrim(v_content #>> '{results,0,version}') = p_version
    and coalesce(v_content #>> '{results,0,table}', 'processes') = p_table
    and v_content #>> '{results,0,status}' = 'success';
exception
  when others then
    return false;
end;
$$;

alter function util.dataset_derivative_rebuild_markdown_response_matches(
  text,
  text,
  uuid,
  text
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_markdown_response_matches(
  text,
  text,
  uuid,
  text
) from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_markdown_response_matches(
  p_content text,
  p_id uuid,
  p_version text
) returns boolean
language sql
immutable
set search_path = ''
as $$
  select util.dataset_derivative_rebuild_markdown_response_matches(
    p_content,
    'processes',
    p_id,
    p_version
  )
$$;

alter function util.dataset_derivative_rebuild_markdown_response_matches(
  text,
  uuid,
  text
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_markdown_response_matches(
  text,
  uuid,
  text
) from public, anon, authenticated, service_role;

create or replace function util.fail_dataset_derivative_rebuild_after_drain(
  p_request_id uuid,
  p_code text,
  p_message text,
  p_details jsonb default '{}'::jsonb
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_quarantine jsonb;
  v_now timestamp with time zone := pg_catalog.clock_timestamp();
begin
  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.status not in ('completed', 'stale', 'failed')
  for update;

  if v_request.id is null then
    return;
  end if;

  v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
    v_request.target_table,
    v_request.target_id,
    v_request.target_version
  );

  update util.dataset_derivative_rebuild_proposals
  set
    status = 'discarded',
    discarded_at = v_now
  where request_id = v_request.id
    and status in ('captured', 'accepted');

  update util.dataset_derivative_rebuild_requests
  set
    status = 'dispatching',
    phase = 'failure_draining',
    failure_release_not_before = coalesce(
      v_request.failure_release_not_before,
      v_now + interval '420 seconds'
    ),
    last_error = coalesce(
      v_request.last_error,
      jsonb_build_object(
        'code', p_code,
        'message', p_message,
        'details', coalesce(p_details, '{}'::jsonb)
      )
    ),
    quarantined_http_requests = quarantined_http_requests
      + coalesce((v_quarantine->>'http_requests')::integer, 0),
    quarantined_embedding_jobs = quarantined_embedding_jobs
      + coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
    quarantined_pending_jobs = quarantined_pending_jobs
      + coalesce((v_quarantine->>'pending_jobs')::integer, 0),
    updated_at = v_now
  where id = p_request_id
    and status not in ('completed', 'stale', 'failed');
end;
$$;

alter function util.fail_dataset_derivative_rebuild_after_drain(
  uuid,
  text,
  text,
  jsonb
) owner to postgres;
revoke all on function util.fail_dataset_derivative_rebuild_after_drain(
  uuid,
  text,
  text,
  jsonb
) from public, anon, authenticated, service_role;

create or replace function util.record_dataset_derivative_rebuild_terminal(
  p_request_id uuid
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  select
    'cmd_dataset_derivative_rebuild_terminal',
    request.actor_user_id,
    request.target_table,
    request.target_id,
    request.target_version,
    jsonb_build_object(
      'record_type', 'terminal',
      'schema_version', 'dataset-derivative-rebuild-status.v1',
      'request_id', request.id,
      'batch_id', request.batch_id,
      'batch_ordinal', request.batch_ordinal,
      'batch_target_count', request.batch_target_count,
      'source_baseline_snapshot_sha256',
        request.source_baseline_snapshot_sha256,
      'plan_sha256', request.plan_sha256,
      'operation_id', request.operation_id,
      'action_id', request.action_id,
      'status', request.status,
      'phase', request.phase,
      'plan_request_sha256', request.plan_request_sha256,
      'action_request_sha256', request.action_request_sha256,
      'completed_snapshot_sha256', request.completed_snapshot_sha256,
      'terminal_at', request.terminal_at,
      'drained_at', request.drained_at,
      'error', request.last_error,
      'quarantine', jsonb_build_object(
        'http_requests', request.quarantined_http_requests,
        'embedding_jobs', request.quarantined_embedding_jobs,
        'pending_jobs', request.quarantined_pending_jobs
      )
    )
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.status in ('completed', 'stale', 'failed')
  on conflict do nothing;
end;
$$;

alter function util.record_dataset_derivative_rebuild_terminal(uuid)
  owner to postgres;
revoke all on function util.record_dataset_derivative_rebuild_terminal(uuid)
  from public, anon, authenticated, service_role;

create or replace function util.read_dataset_derivative_rebuild_batch(
  p_actor_user_id uuid,
  p_batch_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_snapshot jsonb;
  v_targets jsonb := '[]'::jsonb;
  v_target_count integer := 0;
  v_flow_count integer := 0;
  v_process_count integer := 0;
  v_completed_count integer := 0;
  v_nonterminal_count integer := 0;
  v_failed_count integer := 0;
  v_invalid_proof_count integer := 0;
  v_completed_invalid_proof_count integer := 0;
  v_distinct_ordinal_count integer := 0;
  v_distinct_target_count integer := 0;
  v_distinct_plan_count integer := 0;
  v_distinct_summary_count integer := 0;
  v_min_ordinal integer;
  v_max_ordinal integer;
  v_batch_sizes_ok boolean := false;
  v_primary_ok boolean;
  v_terminal_snapshot_ok boolean;
  v_proposals_ok boolean;
  v_terminal_audit_ok boolean;
  v_derivative_fresh boolean;
  v_lifecycle_ok boolean;
  v_http_residue integer;
  v_embedding_residue integer;
  v_pending_residue integer;
  v_failure_residue integer;
  v_fence_residue integer;
  v_target_ok boolean;
  v_batch_integrity_ok boolean;
  v_status text;
  v_code text;
begin
  if p_actor_user_id is null or p_batch_id is null then
    return jsonb_build_object(
      'ok', false,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_READ_INVALID_REQUEST',
      'causal_terminal_proof', false
    );
  end if;

  select
    count(*)::integer,
    count(*) filter (where target_table = 'flows')::integer,
    count(*) filter (where target_table = 'processes')::integer,
    count(*) filter (where status = 'completed')::integer,
    count(*) filter (
      where status not in ('completed', 'stale', 'failed')
    )::integer,
    count(*) filter (where status in ('stale', 'failed'))::integer,
    count(distinct batch_ordinal)::integer,
    count(distinct (
      target_table || ':' || target_id::text || '@' || target_version
    ))::integer,
    count(distinct plan_request_sha256)::integer,
    count(distinct summary_audit_id)::integer,
    min(batch_ordinal)::integer,
    max(batch_ordinal)::integer,
    coalesce(bool_and(batch_target_count = 50), false)
  into
    v_target_count,
    v_flow_count,
    v_process_count,
    v_completed_count,
    v_nonterminal_count,
    v_failed_count,
    v_distinct_ordinal_count,
    v_distinct_target_count,
    v_distinct_plan_count,
    v_distinct_summary_count,
    v_min_ordinal,
    v_max_ordinal,
    v_batch_sizes_ok
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = p_actor_user_id
    and request.batch_id = p_batch_id;

  v_batch_integrity_ok := v_target_count = 50
    and v_flow_count = 23
    and v_process_count = 27
    and v_distinct_ordinal_count = 50
    and v_distinct_target_count = 50
    and v_distinct_plan_count = 1
    and v_distinct_summary_count = 1
    and v_min_ordinal = 1
    and v_max_ordinal = 50
    and v_batch_sizes_ok;

  if not v_batch_integrity_ok then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
      'batch_id', p_batch_id,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_TARGET_SET_MISMATCH',
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'completed_count', v_completed_count,
      'nonterminal_count', v_nonterminal_count,
      'failed_count', v_failed_count
    );
  end if;

  -- Polling must stay cheap while any child is still moving.  A causal proof
  -- is meaningful only after all 50 children are terminal; doing the full
  -- per-target snapshot/queue/proposal/audit closure on every pending poll
  -- would multiply hundreds of reads without improving the decision.
  if v_nonterminal_count > 0 or v_failed_count > 0 then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'ordinal', request.batch_ordinal,
          'request_id', request.id,
          'table', request.target_table,
          'id', request.target_id,
          'version', request.target_version,
          'status', request.status,
          'phase', request.phase,
          'error', request.last_error,
          'causal_terminal_proof', false
        ) order by request.batch_ordinal
      ),
      '[]'::jsonb
    )
    into v_targets
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and request.batch_id = p_batch_id;

    return jsonb_build_object(
      'ok', v_failed_count = 0,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
      'batch_id', p_batch_id,
      'status', case
        when v_failed_count > 0 then 'failed'
        else 'pending'
      end,
      'code', case
        when v_failed_count > 0 then 'DERIVATIVE_BATCH_CHILD_FAILED'
        else 'DERIVATIVE_BATCH_PENDING'
      end,
      'proof_level', 'status_only',
      'proof_deferred', v_failed_count = 0,
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'completed_count', v_completed_count,
      'nonterminal_count', v_nonterminal_count,
      'failed_count', v_failed_count,
      'invalid_proof_count', null,
      'completed_invalid_proof_count', null,
      'targets', v_targets
    );
  end if;

  for v_request in
    select request.*
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and request.batch_id = p_batch_id
    order by request.batch_ordinal
  loop
    begin
      v_snapshot := util.dataset_derivative_rebuild_snapshot(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );
    exception
      when others then
        v_snapshot := null;
    end;

    v_primary_ok := v_snapshot is not null
      and util.dataset_derivative_rebuild_primary_matches(v_request);
    v_terminal_snapshot_ok := coalesce(
      v_request.status = 'completed'
      and v_request.completed_snapshot_sha256 is not null
      and v_snapshot->>'snapshot_sha256'
        is not distinct from v_request.completed_snapshot_sha256,
      false
    );
    v_derivative_fresh := coalesce(
      v_snapshot->>'extracted_md_sha256' is not null
      and v_snapshot->>'embedding_ft_sha256' is not null
      and (v_snapshot->>'embedding_ft_at')::timestamp with time zone
        > coalesce(
          v_request.before_embedding_ft_at,
          '-infinity'::timestamp with time zone
        ),
      false
    );
    v_lifecycle_ok := coalesce(
      v_request.phase = 'completed'
      and v_request.markdown_request_id is not null
      and v_request.markdown_dispatched_at is not null
      and v_request.markdown_response_status between 200 and 299
      and v_request.markdown_response_received_at
        >= v_request.markdown_dispatched_at
      and v_request.markdown_proposal_id is not null
      and v_request.accepted_extracted_md_sha256 is not null
      and v_request.embedding_queue_msg_id is not null
      and v_request.embedding_queued_at
        >= v_request.markdown_response_received_at
      and v_request.embedding_proposal_id is not null
      and v_request.completed_at >= v_request.embedding_queued_at
      and v_request.terminal_at >= v_request.completed_at
      and v_request.drained_at >= v_request.completed_at,
      false
    );

    select
      (
        select count(*) = 2
        from util.dataset_derivative_rebuild_proposals as proposal
        where proposal.request_id = v_request.id
          and proposal.status <> 'discarded'
      )
      and exists (
        select 1
        from util.dataset_derivative_rebuild_proposals as proposal
        where proposal.id = v_request.markdown_proposal_id
          and proposal.request_id = v_request.id
          and proposal.proposal_kind = 'markdown'
          and proposal.status = 'committed'
          and proposal.captured_at >= v_request.markdown_dispatched_at
          and proposal.extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.extracted_md_sha256
            = v_snapshot->>'extracted_md_sha256'
      )
      and exists (
        select 1
        from util.dataset_derivative_rebuild_proposals as proposal
        where proposal.id = v_request.embedding_proposal_id
          and proposal.request_id = v_request.id
          and proposal.proposal_kind = 'embedding'
          and proposal.status = 'committed'
          and proposal.captured_at >= v_request.embedding_queued_at
          and proposal.source_extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.embedding_ft_sha256
            = v_snapshot->>'embedding_ft_sha256'
          and proposal.embedding_ft_at
            = (v_snapshot->>'embedding_ft_at')::timestamp with time zone
      )
    into v_proposals_ok;

    select count(*)::integer
    into v_http_residue
    from net.http_request_queue as request
    where util.dataset_derivative_rebuild_http_body_matches(
      request.body,
      v_request.target_table,
      v_request.target_id,
      v_request.target_version
    );

    select count(*)::integer
    into v_embedding_residue
    from pgmq.q_embedding_jobs as job
    where job.message->>'id' = v_request.target_id::text
      and btrim(job.message->>'version') = v_request.target_version
      and job.message->>'schema' = 'public'
      and job.message->>'table' = v_request.target_table
      and job.message->>'embeddingColumn' = 'embedding_ft';

    select count(*)::integer
    into v_pending_residue
    from util.pending_embedding_jobs as pending
    where pending.schema_name = 'public'
      and pending.table_name = v_request.target_table
      and pending.record_id = v_request.target_id::text
      and btrim(pending.record_version) = v_request.target_version
      and pending.embedding_column = 'embedding_ft'
      and pending.status = 'pending';

    select count(*)::integer
    into v_failure_residue
    from util.embedding_job_failures as failure
    where failure.msg_id = v_request.embedding_queue_msg_id;

    select count(*)::integer
    into v_fence_residue
    from util.dataset_derivative_rebuild_requests as active_request
    where active_request.id <> v_request.id
      and active_request.target_table = v_request.target_table
      and active_request.target_id = v_request.target_id
      and active_request.target_version = v_request.target_version
      and active_request.status not in ('completed', 'stale', 'failed');

    select count(*) = 1
    into v_terminal_audit_ok
    from public.command_audit_log as audit
    where audit.command = 'cmd_dataset_derivative_rebuild_terminal'
      and audit.actor_user_id = p_actor_user_id
      and audit.target_table = v_request.target_table
      and audit.target_id = v_request.target_id
      and audit.target_version = v_request.target_version
      and audit.payload->>'request_id' = v_request.id::text
      and audit.payload->>'status' = 'completed';

    v_target_ok := coalesce(v_primary_ok
      and v_terminal_snapshot_ok
      and v_derivative_fresh
      and v_lifecycle_ok
      and v_proposals_ok
      and v_terminal_audit_ok
      and v_http_residue = 0
      and v_embedding_residue = 0
      and v_pending_residue = 0
      and v_failure_residue = 0
      and v_fence_residue = 0, false);

    if not v_target_ok then
      v_invalid_proof_count := v_invalid_proof_count + 1;
      if v_request.status = 'completed' then
        v_completed_invalid_proof_count :=
          v_completed_invalid_proof_count + 1;
      end if;
    end if;

    v_targets := v_targets || jsonb_build_array(
      jsonb_build_object(
        'ordinal', v_request.batch_ordinal,
        'request_id', v_request.id,
        'table', v_request.target_table,
        'id', v_request.target_id,
        'version', v_request.target_version,
        'status', v_request.status,
        'phase', v_request.phase,
        'source_baseline_snapshot_sha256',
          v_request.source_baseline_snapshot_sha256,
        'expected_snapshot_sha256', v_request.expected_snapshot_sha256,
        'completed_snapshot_sha256',
          v_request.completed_snapshot_sha256,
        'primary_matches', v_primary_ok,
        'terminal_snapshot_matches', v_terminal_snapshot_ok,
        'proposals_committed', v_proposals_ok,
        'derivative_fresh', v_derivative_fresh,
        'lifecycle_complete', v_lifecycle_ok,
        'terminal_audit_present', v_terminal_audit_ok,
        'residue', jsonb_build_object(
          'http_requests', v_http_residue,
          'embedding_jobs', v_embedding_residue,
          'pending_jobs', v_pending_residue,
          'failure_rows', v_failure_residue,
          'other_active_fences', v_fence_residue
        ),
        'causal_terminal_proof', v_target_ok
      )
    );
  end loop;

  if v_failed_count > 0 then
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CHILD_FAILED';
  elsif v_completed_invalid_proof_count > 0 then
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH';
  elsif v_nonterminal_count > 0 then
    v_status := 'pending';
    v_code := 'DERIVATIVE_BATCH_PENDING';
  elsif v_completed_count = 50 and v_invalid_proof_count = 0 then
    v_status := 'completed';
    v_code := 'DERIVATIVE_BATCH_COMPLETED';
  else
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH';
  end if;

  return jsonb_build_object(
    'ok', v_status <> 'failed',
    'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
    'batch_id', p_batch_id,
    'status', v_status,
    'code', v_code,
    'proof_level', 'causal_terminal',
    'proof_deferred', false,
    'causal_terminal_proof',
      v_status = 'completed' and v_invalid_proof_count = 0,
    'target_count', v_target_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'completed_count', v_completed_count,
    'nonterminal_count', v_nonterminal_count,
    'failed_count', v_failed_count,
    'invalid_proof_count', v_invalid_proof_count,
    'completed_invalid_proof_count', v_completed_invalid_proof_count,
    'targets', v_targets
  );
end;
$$;

alter function util.read_dataset_derivative_rebuild_batch(uuid, uuid)
  owner to postgres;
revoke all on function util.read_dataset_derivative_rebuild_batch(uuid, uuid)
  from public, anon, authenticated, service_role;

comment on function util.read_dataset_derivative_rebuild_batch(uuid, uuid) is
  'Private fail-closed aggregate proof for the exact protected 23-flow + 27-process derivative batch. Pending/failed polls use one status-only aggregate; the expensive 50-target causal proof runs only after all children are terminal. It never advances or retries work.';
