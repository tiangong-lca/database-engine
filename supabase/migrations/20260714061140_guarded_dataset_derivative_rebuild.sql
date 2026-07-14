-- Guarded, owner-draft process derivative rebuilds.
--
-- The released Markdown and embedding workers write by process id + version
-- only.  A rebuild therefore cannot be made stale-write safe by polling the
-- primary row after dispatch.  This migration keeps a target write fence for
-- the whole asynchronous request, stages every external derivative write, and
-- lets only the private coordinator commit the exact phase proposal after the
-- frozen primary fingerprint has been rechecked.

create table util.dataset_derivative_rebuild_requests (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  actor_user_id uuid not null,
  plan_sha256 text not null,
  operation_id text not null,
  action_id text not null,
  target_table text not null default 'processes',
  target_id uuid not null,
  target_version text not null,
  target_visibility text not null default 'owner_draft',
  expected_snapshot_sha256 text not null,
  expected_modified_at timestamp with time zone not null,
  expected_json_sha256 text not null,
  expected_json_ordered_sha256 text not null,
  expected_extracted_text_sha256 text not null,
  before_extracted_md_sha256 text,
  before_embedding_ft_sha256 text,
  before_embedding_ft_at timestamp with time zone,
  plan_request_sha256 text not null,
  action_request_sha256 text not null,
  reason_code text not null,
  status text not null default 'queued',
  phase text not null default 'admitted',
  admitted_at timestamp with time zone not null default pg_catalog.clock_timestamp(),
  drain_not_before timestamp with time zone not null,
  markdown_request_id bigint,
  markdown_dispatched_at timestamp with time zone,
  markdown_deadline_at timestamp with time zone,
  markdown_response_status integer,
  markdown_response_received_at timestamp with time zone,
  markdown_proposal_id bigint,
  accepted_extracted_md_sha256 text,
  embedding_pending_job_id bigint,
  embedding_queue_msg_id bigint,
  embedding_queued_at timestamp with time zone,
  embedding_deadline_at timestamp with time zone,
  embedding_proposal_id bigint,
  completed_snapshot_sha256 text,
  completed_at timestamp with time zone,
  terminal_at timestamp with time zone,
  drained_at timestamp with time zone,
  failure_release_not_before timestamp with time zone,
  last_error jsonb,
  action_audit_id bigint not null,
  summary_audit_id bigint not null,
  quarantined_http_requests integer not null default 0,
  quarantined_embedding_jobs integer not null default 0,
  quarantined_pending_jobs integer not null default 0,
  created_at timestamp with time zone not null default pg_catalog.clock_timestamp(),
  updated_at timestamp with time zone not null default pg_catalog.clock_timestamp(),
  constraint dataset_derivative_rebuild_request_table_check
    check (target_table = 'processes'),
  constraint dataset_derivative_rebuild_request_visibility_check
    check (target_visibility = 'owner_draft'),
  constraint dataset_derivative_rebuild_request_hashes_check
    check (
      plan_sha256 ~ '^[a-f0-9]{64}$'
      and expected_snapshot_sha256 ~ '^[a-f0-9]{64}$'
      and expected_json_sha256 ~ '^[a-f0-9]{64}$'
      and expected_json_ordered_sha256 ~ '^[a-f0-9]{64}$'
      and expected_extracted_text_sha256 ~ '^[a-f0-9]{64}$'
      and plan_request_sha256 ~ '^[a-f0-9]{64}$'
      and action_request_sha256 ~ '^[a-f0-9]{64}$'
      and (
        before_extracted_md_sha256 is null
        or before_extracted_md_sha256 ~ '^[a-f0-9]{64}$'
      )
      and (
        before_embedding_ft_sha256 is null
        or before_embedding_ft_sha256 ~ '^[a-f0-9]{64}$'
      )
    ),
  constraint dataset_derivative_rebuild_request_status_check
    check (status in (
      'queued',
      'dispatching',
      'markdown_pending',
      'embedding_pending',
      'completed',
      'stale',
      'failed'
    )),
  constraint dataset_derivative_rebuild_request_terminal_check
    check (
      (status in ('completed', 'stale', 'failed') and terminal_at is not null)
      or (status not in ('completed', 'stale', 'failed') and terminal_at is null)
    ),
  constraint dataset_derivative_rebuild_request_counts_check
    check (
      quarantined_http_requests >= 0
      and quarantined_embedding_jobs >= 0
      and quarantined_pending_jobs >= 0
    )
);

create unique index dataset_derivative_rebuild_actor_plan_uidx
  on util.dataset_derivative_rebuild_requests (
    actor_user_id,
    plan_request_sha256
  );

create unique index dataset_derivative_rebuild_actor_action_uidx
  on util.dataset_derivative_rebuild_requests (
    actor_user_id,
    plan_request_sha256,
    action_request_sha256
  );

create unique index dataset_derivative_rebuild_active_target_uidx
  on util.dataset_derivative_rebuild_requests (
    target_table,
    target_id,
    target_version
  )
  where status not in ('completed', 'stale', 'failed');

create index dataset_derivative_rebuild_coordinator_idx
  on util.dataset_derivative_rebuild_requests (
    status,
    updated_at,
    admitted_at
  )
  where status not in ('completed', 'stale', 'failed');

create index dataset_derivative_rebuild_actor_read_idx
  on util.dataset_derivative_rebuild_requests (
    actor_user_id,
    created_at desc
  );

create table util.dataset_derivative_rebuild_proposals (
  id bigint generated always as identity primary key,
  request_id uuid not null
    references util.dataset_derivative_rebuild_requests(id) on delete cascade,
  proposal_kind text not null,
  extracted_md text,
  extracted_md_sha256 text,
  embedding_ft extensions.vector(1024),
  embedding_ft_sha256 text,
  embedding_ft_at timestamp with time zone,
  source_extracted_md_sha256 text,
  source_role text not null default current_user,
  source_backend_pid integer not null default pg_catalog.pg_backend_pid(),
  status text not null default 'captured',
  captured_at timestamp with time zone not null default pg_catalog.clock_timestamp(),
  committed_at timestamp with time zone,
  discarded_at timestamp with time zone,
  constraint dataset_derivative_rebuild_proposal_kind_check
    check (proposal_kind in ('markdown', 'embedding')),
  constraint dataset_derivative_rebuild_proposal_status_check
    check (status in ('captured', 'accepted', 'committed', 'discarded')),
  constraint dataset_derivative_rebuild_proposal_payload_check
    check (
      (
        proposal_kind = 'markdown'
        and extracted_md is not null
        and extracted_md_sha256 ~ '^[a-f0-9]{64}$'
        and embedding_ft is null
        and embedding_ft_sha256 is null
        and embedding_ft_at is null
      )
      or (
        proposal_kind = 'embedding'
        and extracted_md is null
        and extracted_md_sha256 is null
        and embedding_ft is not null
        and embedding_ft_sha256 ~ '^[a-f0-9]{64}$'
        and embedding_ft_at is not null
        and source_extracted_md_sha256 ~ '^[a-f0-9]{64}$'
      )
    )
);

create index dataset_derivative_rebuild_proposal_phase_idx
  on util.dataset_derivative_rebuild_proposals (
    request_id,
    proposal_kind,
    status,
    captured_at,
    id
  );

create table util.dataset_derivative_rebuild_permits (
  request_id uuid not null
    references util.dataset_derivative_rebuild_requests(id) on delete cascade,
  proposal_id bigint not null
    references util.dataset_derivative_rebuild_proposals(id) on delete cascade,
  permit_kind text not null,
  backend_pid integer not null,
  transaction_id bigint not null,
  created_at timestamp with time zone not null default pg_catalog.clock_timestamp(),
  primary key (request_id, proposal_id, permit_kind),
  constraint dataset_derivative_rebuild_permit_kind_check
    check (permit_kind in ('markdown', 'embedding'))
);

revoke all on table util.dataset_derivative_rebuild_requests
  from public, anon, authenticated, service_role;
revoke all on table util.dataset_derivative_rebuild_proposals
  from public, anon, authenticated, service_role;
revoke all on table util.dataset_derivative_rebuild_permits
  from public, anon, authenticated, service_role;
revoke all on sequence util.dataset_derivative_rebuild_proposals_id_seq
  from public, anon, authenticated, service_role;

create unique index command_audit_log_derivative_rebuild_plan_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'plan_request_sha256')
  )
  where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
    and target_table is null
    and payload ->> 'record_type' = 'plan_summary';

create unique index command_audit_log_derivative_rebuild_action_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'plan_request_sha256'),
    (payload ->> 'action_request_sha256')
  )
  where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
    and target_table = 'processes'
    and payload ->> 'record_type' = 'action';

create unique index command_audit_log_derivative_rebuild_terminal_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'request_id')
  )
  where command = 'cmd_dataset_derivative_rebuild_terminal'
    and target_table = 'processes'
    and payload ->> 'record_type' = 'terminal';

create or replace function util.dataset_derivative_rebuild_sha256(
  p_value text
) returns text
language sql
immutable
strict
set search_path = ''
as $$
  select pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(p_value, 'UTF8'), 'sha256'),
    'hex'
  )
$$;

alter function util.dataset_derivative_rebuild_sha256(text)
  owner to postgres;
revoke all on function util.dataset_derivative_rebuild_sha256(text)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_derivative_rebuild_snapshot(
  p_process public.processes
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
  if p_process.json is null
    or p_process.json_ordered is null
    or p_process.extracted_text is null
    or p_process.modified_at is null then
    raise exception using
      errcode = '22004',
      message = 'Derivative rebuild primary snapshot fields must be non-null';
  end if;

  v_json_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.json::jsonb::text
  );
  v_json_ordered_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.json_ordered::jsonb::text
  );
  v_extracted_text_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.extracted_text
  );
  v_extracted_md_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.extracted_md
  );
  v_embedding_ft_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.embedding_ft::text
  );

  v_snapshot := jsonb_build_object(
    'schema_version', 'dataset-derivative-snapshot.v1',
    'table', 'processes',
    'id', p_process.id,
    'version', btrim(p_process.version::text),
    'user_id', p_process.user_id,
    'state_code', p_process.state_code,
    'modified_at', p_process.modified_at,
    'json_sha256', v_json_sha256,
    'json_ordered_sha256', v_json_ordered_sha256,
    'extracted_text_sha256', v_extracted_text_sha256,
    'extracted_md_sha256', v_extracted_md_sha256,
    'embedding_ft_sha256', v_embedding_ft_sha256,
    'embedding_ft_at', p_process.embedding_ft_at
  );

  return v_snapshot || jsonb_build_object(
    'snapshot_sha256',
    util.dataset_derivative_rebuild_sha256(v_snapshot::text)
  );
end;
$$;

alter function util.dataset_derivative_rebuild_snapshot(public.processes)
  owner to postgres;
revoke all on function util.dataset_derivative_rebuild_snapshot(public.processes)
  from public, anon, authenticated, service_role;

comment on table util.dataset_derivative_rebuild_requests is
  'Private durable coordinator state for one-action owner-draft process derivative rebuilds. Nonterminal rows are target write fences.';
comment on table util.dataset_derivative_rebuild_proposals is
  'Private staging area for external Markdown/vector writes captured while a derivative rebuild target is fenced.';
comment on table util.dataset_derivative_rebuild_permits is
  'Transaction- and backend-bound permits used only by the private coordinator to commit one staged derivative proposal.';

create or replace function util.dataset_derivative_rebuild_primary_matches(
  p_request util.dataset_derivative_rebuild_requests,
  p_process public.processes
) returns boolean
language sql
stable
set search_path = ''
as $$
  select coalesce(
    p_process.id is not null
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

create or replace function util.guard_dataset_derivative_rebuild_primary()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_request_id uuid;
begin
  select request.id
  into v_request_id
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = 'processes'
    and request.target_id = old.id
    and request.target_version = btrim(old.version::text)
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_request_id is not null then
    raise exception using
      errcode = '55006',
      message = 'Process primary row is fenced by an active derivative rebuild',
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

drop trigger if exists process_derivative_rebuild_primary_update_fence
  on public.processes;
create trigger process_derivative_rebuild_primary_update_fence
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
  embedding_flag,
  model_id
on public.processes
for each row
execute function util.guard_dataset_derivative_rebuild_primary();

drop trigger if exists process_derivative_rebuild_primary_delete_fence
  on public.processes;
create trigger process_derivative_rebuild_primary_delete_fence
before delete on public.processes
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
  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = 'processes'
    and request.target_id = old.id
    and request.target_version = btrim(old.version::text)
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_request.id is null then
    return new;
  end if;

  if not util.dataset_derivative_rebuild_primary_matches(v_request, old) then
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

drop trigger if exists process_derivative_rebuild_stage_markdown
  on public.processes;
create trigger process_derivative_rebuild_stage_markdown
before update of extracted_md on public.processes
for each row
execute function util.stage_dataset_derivative_rebuild_write('markdown');

drop trigger if exists process_derivative_rebuild_stage_embedding
  on public.processes;
create trigger process_derivative_rebuild_stage_embedding
before update of embedding_ft, embedding_ft_at on public.processes
for each row
execute function util.stage_dataset_derivative_rebuild_write('embedding');

-- The generic embedding worker reads its content through the function named
-- in the queue message.  For a guarded rebuild it must embed the accepted
-- staged Markdown, not the still-visible old Markdown.  This lets the final
-- Markdown/vector pair be switched in one database UPDATE.
create or replace function public.processes_derivative_rebuild_embedding_input(
  p_process public.processes
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
  where request.target_table = 'processes'
    and request.target_id = p_process.id
    and request.target_version = btrim(p_process.version::text)
    and request.status = 'embedding_pending'
    and request.accepted_extracted_md_sha256 = proposal.extracted_md_sha256
  limit 1
$$;

alter function public.processes_derivative_rebuild_embedding_input(public.processes)
  owner to postgres;
revoke all on function public.processes_derivative_rebuild_embedding_input(public.processes)
  from public, anon, authenticated, service_role;

-- Preserve the global embedding queue policy, but suppress the ordinary
-- extracted_md trigger only while the private coordinator is committing its
-- staged Markdown.  The coordinator enqueues one request-tagged job itself,
-- including when the regenerated Markdown is byte-identical.
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
    and tg_table_name = 'processes'
    and exists (
      select 1
      from util.dataset_derivative_rebuild_permits as permit
      join util.dataset_derivative_rebuild_requests as request
        on request.id = permit.request_id
      where permit.permit_kind = 'markdown'
        and permit.backend_pid = pg_catalog.pg_backend_pid()
        and permit.transaction_id = pg_catalog.txid_current()
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

-- The generic embedding dispatcher claims jobs for 300 seconds, while a
-- hosted Edge invocation may legitimately run for up to 400 seconds.  Keep a
-- request-tagged rebuild job invisible for the full fenced worker window so a
-- slow first invocation cannot be dispatched a second time and write after
-- the request has completed.
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
      and request.target_table = 'processes'
      and request.target_id::text = new.message->>'id'
      and request.target_version = btrim(new.message->>'version')
      and new.message->>'schema' = 'public'
      and new.message->>'table' = 'processes'
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

drop trigger if exists dataset_derivative_rebuild_embedding_visibility_fence
  on pgmq.q_embedding_jobs;
create trigger dataset_derivative_rebuild_embedding_visibility_fence
before update of vt on pgmq.q_embedding_jobs
for each row
execute function util.guard_dataset_derivative_rebuild_embedding_visibility();

create or replace function util.dataset_derivative_rebuild_http_body_matches(
  p_body bytea,
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
  if p_body is null then
    return false;
  end if;
  v_body := pg_catalog.convert_from(p_body, 'UTF8')::jsonb;
  if jsonb_typeof(v_body) = 'object' then
    return v_body #>> '{record,id}' = p_id::text
      and btrim(v_body #>> '{record,version}') = p_version
      and coalesce(v_body->>'table', 'processes') = 'processes';
  end if;

  if jsonb_typeof(v_body) = 'array' then
    return exists (
      select 1
      from jsonb_array_elements(v_body) as job(value)
      where job.value->>'id' = p_id::text
        and btrim(job.value->>'version') = p_version
        and job.value->>'schema' = 'public'
        and job.value->>'table' = 'processes'
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
  uuid,
  text
) owner to postgres;
revoke all on function util.dataset_derivative_rebuild_http_body_matches(
  bytea,
  uuid,
  text
) from public, anon, authenticated, service_role;

create or replace function util.quarantine_dataset_derivative_rebuild_target(
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
  delete from net.http_request_queue as request
  where (
      request.url like '%/functions/v1/webhook_process_embedding_ft'
      or request.url like '%/functions/v1/embedding_ft'
    )
    and util.dataset_derivative_rebuild_http_body_matches(
      request.body,
      p_id,
      p_version
    );
  get diagnostics v_http = row_count;

  delete from pgmq.q_embedding_jobs as job
  where job.message->>'id' = p_id::text
    and btrim(job.message->>'version') = p_version
    and job.message->>'schema' = 'public'
    and job.message->>'table' = 'processes'
    and job.message->>'embeddingColumn' = 'embedding_ft';
  get diagnostics v_embedding = row_count;

  delete from util.pending_embedding_jobs as pending
  where pending.schema_name = 'public'
    and pending.table_name = 'processes'
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
  v_process public.processes%rowtype;
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

  select process.*
  into v_process
  from public.processes as process
  where process.id = v_request.target_id
    and btrim(process.version::text) = v_request.target_version
  for update;

  if v_process.id is null
    or not util.dataset_derivative_rebuild_primary_matches(v_request, v_process) then
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

  update public.processes as process
  set
    extracted_md = v_markdown.extracted_md,
    embedding_ft = v_embedding.embedding_ft,
    embedding_ft_at = v_embedding.embedding_ft_at
  where process.id = v_request.target_id
    and btrim(process.version::text) = v_request.target_version;

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

  if p_table is distinct from 'processes'
    or p_id is null
    or p_version is null
    or btrim(p_version) !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_INVALID_REQUEST',
      'status', 400,
      'message', 'A processes id and canonical version are required'
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = p_id
    and btrim(process.version::text) = btrim(p_version)
    and process.user_id = v_actor
    and process.state_code = 0;

  if v_process.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_NOT_AVAILABLE',
      'status', 404,
      'message', 'Owner-draft process snapshot is not available'
    );
  end if;

  begin
    v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception
    when others then
      return jsonb_build_object(
        'ok', false,
        'code', 'DERIVATIVE_SNAPSHOT_NOT_READY',
        'status', 409,
        'message', 'Owner-draft process primary snapshot is incomplete'
      );
  end;

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
  'Returns one current-actor state_code=0 process primary/derivative fingerprint. It never scans vectors for unrelated account rows and performs no mutation.';

create or replace function public.cmd_dataset_derivative_rebuild_plan_guarded(
  p_plan jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
as $$
declare
  v_actor uuid := auth.uid();
  v_command constant text := 'cmd_dataset_derivative_rebuild_plan_guarded';
  v_schema_version constant text := 'dataset-derivative-rebuild-plan.v1';
  v_plan_sha256 text;
  v_operation_id text;
  v_action jsonb;
  v_action_id text;
  v_target_id uuid;
  v_target_version text;
  v_expected_snapshot_sha256 text;
  v_reason_code text;
  v_plan_request_sha256 text;
  v_action_request_sha256 text;
  v_existing util.dataset_derivative_rebuild_requests%rowtype;
  v_active_request_id uuid;
  v_process public.processes%rowtype;
  v_snapshot jsonb;
  v_quarantine jsonb;
  v_request_id uuid := pg_catalog.gen_random_uuid();
  v_action_audit_id bigint;
  v_summary_audit_id bigint;
  v_now timestamp with time zone := pg_catalog.clock_timestamp();
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_plan is not null and pg_column_size(p_plan) > 65536 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_REQUEST_TOO_LARGE',
      'status', 413,
      'message', 'Derivative rebuild plan exceeds 64 KiB'
    );
  end if;

  if jsonb_typeof(p_plan) is distinct from 'object'
    or not (p_plan ?& array[
      'schema_version',
      'plan_sha256',
      'operation_id',
      'target_visibility',
      'actions'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_plan) as plan_key(key)
      where plan_key.key <> all (array[
        'schema_version',
        'plan_sha256',
        'operation_id',
        'target_visibility',
        'actions'
      ])
    )
    or p_plan->>'schema_version' is distinct from v_schema_version
    or jsonb_typeof(p_plan->'plan_sha256') is distinct from 'string'
    or (p_plan->>'plan_sha256') !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_plan->'operation_id') is distinct from 'string'
    or nullif(btrim(p_plan->>'operation_id'), '') is null
    or octet_length(p_plan->>'operation_id') > 512
    or p_plan->>'target_visibility' is distinct from 'owner_draft'
    or jsonb_typeof(p_plan->'actions') is distinct from 'array'
    or jsonb_array_length(p_plan->'actions') <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_INVALID_REQUEST',
      'status', 400,
      'message', 'Plan must match dataset-derivative-rebuild-plan.v1 exactly'
    );
  end if;

  v_plan_sha256 := p_plan->>'plan_sha256';
  v_operation_id := btrim(p_plan->>'operation_id');
  v_action := p_plan->'actions'->0;

  if jsonb_typeof(v_action) is distinct from 'object'
    or not (v_action ?& array[
      'action_id',
      'action',
      'table',
      'id',
      'version',
      'expected_state_code',
      'expected_snapshot_sha256',
      'components',
      'reason_code'
    ])
    or exists (
      select 1
      from jsonb_object_keys(v_action) as action_key(key)
      where action_key.key <> all (array[
        'action_id',
        'action',
        'table',
        'id',
        'version',
        'expected_state_code',
        'expected_snapshot_sha256',
        'components',
        'reason_code'
      ])
    )
    or jsonb_typeof(v_action->'action_id') is distinct from 'string'
    or nullif(btrim(v_action->>'action_id'), '') is null
    or octet_length(v_action->>'action_id') > 512
    or v_action->>'action' is distinct from 'rebuild_derivatives'
    or v_action->>'table' is distinct from 'processes'
    or jsonb_typeof(v_action->'id') is distinct from 'string'
    or (v_action->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or jsonb_typeof(v_action->'version') is distinct from 'string'
    or (v_action->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(v_action->'expected_state_code') is distinct from 'number'
    or v_action->>'expected_state_code' <> '0'
    or jsonb_typeof(v_action->'expected_snapshot_sha256') is distinct from 'string'
    or (v_action->>'expected_snapshot_sha256') !~ '^[a-f0-9]{64}$'
    or v_action->'components'
      is distinct from '["extracted_md", "embedding_ft"]'::jsonb
    or jsonb_typeof(v_action->'reason_code') is distinct from 'string'
    or nullif(btrim(v_action->>'reason_code'), '') is null
    or octet_length(v_action->>'reason_code') > 512 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_INVALID_ACTION',
      'status', 400,
      'message', 'Exactly one owner-draft process rebuild action is required'
    );
  end if;

  v_action_id := btrim(v_action->>'action_id');
  v_target_id := (v_action->>'id')::uuid;
  v_target_version := btrim(v_action->>'version');
  v_expected_snapshot_sha256 := v_action->>'expected_snapshot_sha256';
  v_reason_code := btrim(v_action->>'reason_code');
  v_plan_request_sha256 := util.dataset_derivative_rebuild_sha256(p_plan::text);
  v_action_request_sha256 := util.dataset_derivative_rebuild_sha256(v_action::text);

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      v_actor::text || ':' || v_plan_request_sha256,
      0
    )
  );

  select request.*
  into v_existing
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = v_actor
    and request.plan_request_sha256 = v_plan_request_sha256;

  if v_existing.id is not null then
    if v_existing.plan_sha256 is distinct from v_plan_sha256
      or v_existing.operation_id is distinct from v_operation_id
      or v_existing.action_id is distinct from v_action_id
      or v_existing.target_id is distinct from v_target_id
      or v_existing.target_version is distinct from v_target_version
      or v_existing.expected_snapshot_sha256
        is distinct from v_expected_snapshot_sha256
      or v_existing.action_request_sha256
        is distinct from v_action_request_sha256 then
      return jsonb_build_object(
        'ok', false,
        'code', 'DERIVATIVE_PLAN_REPLAY_CONFLICT',
        'status', 409,
        'message', 'Existing admission does not match the exact plan'
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'command', v_command,
      'schema_version', v_schema_version,
      'plan_sha256', v_existing.plan_sha256,
      'operation_id', v_existing.operation_id,
      'target_visibility', v_existing.target_visibility,
      'plan_request_sha256', v_existing.plan_request_sha256,
      'action_count', 1,
      'accepted_count', 1,
      'summary_audit_id', v_existing.summary_audit_id::text,
      'request_id', v_existing.id::text,
      'status', 'queued',
      'action_request_sha256', v_existing.action_request_sha256,
      'database_audit_id', v_existing.action_audit_id::text,
      'idempotent_replay', true
    );
  end if;

  -- SHARE ROW EXCLUSIVE serializes statement snapshots around fence creation.
  -- A process writer either finishes before this frozen snapshot or begins
  -- after the nonterminal request/fence is committed and visible.
  lock table public.processes in share row exclusive mode;

  select process.*
  into v_process
  from public.processes as process
  where process.id = v_target_id
    and btrim(process.version::text) = v_target_version
    and process.user_id = v_actor
    and process.state_code = 0
  for update;

  if v_process.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_SCOPE_MISMATCH',
      'status', 409,
      'message', 'Action does not identify an exact current-owner draft process'
    );
  end if;

  begin
    v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception
    when others then
      return jsonb_build_object(
        'ok', false,
        'code', 'DERIVATIVE_PLAN_PRIMARY_NOT_READY',
        'status', 409,
        'message', 'Owner-draft process primary snapshot is incomplete'
      );
  end;

  if v_snapshot->>'json_sha256'
      is distinct from v_snapshot->>'json_ordered_sha256'
    or v_snapshot->>'snapshot_sha256'
      is distinct from v_expected_snapshot_sha256 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_SNAPSHOT_DRIFT',
      'status', 409,
      'message', 'Action snapshot changed before admission'
    );
  end if;

  select request.id
  into v_active_request_id
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = 'processes'
    and request.target_id = v_target_id
    and request.target_version = v_target_version
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_active_request_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_TARGET_BUSY',
      'status', 409,
      'message', 'Another derivative rebuild already fences this process'
    );
  end if;

  v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
    v_target_id,
    v_target_version
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
    v_actor,
    'processes',
    v_target_id,
    v_target_version,
    jsonb_build_object(
      'record_type', 'action',
      'schema_version', v_schema_version,
      'request_id', v_request_id,
      'plan_sha256', v_plan_sha256,
      'operation_id', v_operation_id,
      'action_id', v_action_id,
      'target_visibility', 'owner_draft',
      'expected_snapshot_sha256', v_expected_snapshot_sha256,
      'plan_request_sha256', v_plan_request_sha256,
      'action_request_sha256', v_action_request_sha256,
      'reason_code', v_reason_code,
      'components', jsonb_build_array('extracted_md', 'embedding_ft'),
      'hash_algorithm', 'postgres-jsonb-text-sha256'
    )
  )
  returning id into v_action_audit_id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    v_command,
    v_actor,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', 'plan_summary',
      'schema_version', v_schema_version,
      'request_id', v_request_id,
      'plan_sha256', v_plan_sha256,
      'operation_id', v_operation_id,
      'target_visibility', 'owner_draft',
      'plan_request_sha256', v_plan_request_sha256,
      'action_request_sha256', v_action_request_sha256,
      'action_count', 1,
      'accepted_count', 1,
      'action_audit_id', v_action_audit_id::text,
      'hash_algorithm', 'postgres-jsonb-text-sha256'
    )
  )
  returning id into v_summary_audit_id;

  insert into util.dataset_derivative_rebuild_requests (
    id,
    actor_user_id,
    plan_sha256,
    operation_id,
    action_id,
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
    quarantined_pending_jobs
  ) values (
    v_request_id,
    v_actor,
    v_plan_sha256,
    v_operation_id,
    v_action_id,
    v_target_id,
    v_target_version,
    v_expected_snapshot_sha256,
    (v_snapshot->>'modified_at')::timestamp with time zone,
    v_snapshot->>'json_sha256',
    v_snapshot->>'json_ordered_sha256',
    v_snapshot->>'extracted_text_sha256',
    v_snapshot->>'extracted_md_sha256',
    v_snapshot->>'embedding_ft_sha256',
    (v_snapshot->>'embedding_ft_at')::timestamp with time zone,
    v_plan_request_sha256,
    v_action_request_sha256,
    v_reason_code,
    'queued',
    'admitted',
    v_now,
    v_now + interval '420 seconds',
    v_action_audit_id,
    v_summary_audit_id,
    coalesce((v_quarantine->>'http_requests')::integer, 0),
    coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
    coalesce((v_quarantine->>'pending_jobs')::integer, 0)
  );

  return jsonb_build_object(
    'ok', true,
    'command', v_command,
    'schema_version', v_schema_version,
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'target_visibility', 'owner_draft',
    'plan_request_sha256', v_plan_request_sha256,
    'action_count', 1,
    'accepted_count', 1,
    'summary_audit_id', v_summary_audit_id::text,
    'request_id', v_request_id::text,
    'status', 'queued',
    'action_request_sha256', v_action_request_sha256,
    'database_audit_id', v_action_audit_id::text,
    'idempotent_replay', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_LOCK_BUSY',
      'status', 409,
      'message', 'Process write fence could not be acquired'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_PLAN_CONCURRENT_CONFLICT',
      'status', 409,
      'message', 'A concurrent admission won the exact plan or target'
    );
end;
$$;

alter function public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)
  owner to postgres;
revoke all on function public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)
  to authenticated;

comment on function public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb) is
  'Exactly-once admission for one immutable current-owner state_code=0 process derivative rebuild. It creates the audit/fence atomically, quarantines visible old work, and returns queued admission only; completion is asynchronous.';

create or replace function util.dataset_derivative_rebuild_markdown_response_matches(
  p_content text,
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
  v_content := p_content::jsonb;
  return v_content->>'success' = 'true'
    and jsonb_typeof(v_content->'results') = 'array'
    and jsonb_array_length(v_content->'results') = 1
    and v_content #>> '{results,0,id}' = p_id::text
    and btrim(v_content #>> '{results,0,version}') = p_version
    and coalesce(v_content #>> '{results,0,table}', 'processes') = 'processes'
    and v_content #>> '{results,0,status}' = 'success';
exception
  when others then
    return false;
end;
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

  -- Stop queued work before starting the release timer.  The second
  -- quarantine at release catches anything that raced with this first pass;
  -- the 420-second interval covers already-dispatched Edge work.
  v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
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
    'processes',
    request.target_id,
    request.target_version,
    jsonb_build_object(
      'record_type', 'terminal',
      'schema_version', 'dataset-derivative-rebuild-status.v1',
      'request_id', request.id,
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

create or replace function util.process_dataset_derivative_rebuilds(
  p_limit integer default 5
) returns integer
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_process public.processes%rowtype;
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

    select process.*
    into v_process
    from public.processes as process
    where process.id = v_request.target_id
      and btrim(process.version::text) = v_request.target_version;

    if v_process.id is null
      or not util.dataset_derivative_rebuild_primary_matches(
        v_request,
        v_process
      ) then
      perform util.fail_dataset_derivative_rebuild_after_drain(
        v_request.id,
        'DERIVATIVE_PRIMARY_DRIFT',
        'Frozen process primary fingerprint is no longer present',
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
          || '/functions/v1/webhook_process_embedding_ft',
        headers => jsonb_build_object(
          'Content-Type', 'application/json',
          'Authorization', 'Bearer ' || v_service_key,
          'apikey', v_service_key,
          'x_region', 'us-east-1'
        ),
        body => jsonb_build_object(
          'type', 'UPDATE',
          'schema', 'public',
          'table', 'processes',
          'record', jsonb_build_object(
            'id', v_process.id,
            'version', btrim(v_process.version::text),
            'json_ordered', v_process.json_ordered::jsonb
          ),
          'old_record', jsonb_build_object(
            'id', v_process.id,
            'version', btrim(v_process.version::text),
            'json_ordered', v_process.json_ordered::jsonb
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
        'processes',
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
            'table', 'processes',
            'contentFunction',
              'processes_derivative_rebuild_embedding_input',
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
          'processes',
          v_request.target_id::text,
          v_request.target_version,
          'processes_derivative_rebuild_embedding_input',
          'embedding_ft',
          'embedding_ft',
          jsonb_build_object(
            'id', v_request.target_id,
            'version', v_request.target_version,
            'schema', 'public',
            'table', 'processes',
            'contentFunction',
              'processes_derivative_rebuild_embedding_input',
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

      select process.*
      into v_process
      from public.processes as process
      where process.id = v_request.target_id
        and btrim(process.version::text) = v_request.target_version;
      v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);

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
  'Private bounded coordinator for fenced process derivative rebuilds. It never waits on network calls in-transaction; pg_net/PGMQ work is correlated through durable ids and staged proposals.';

do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('process-dataset-derivative-rebuilds');
    exception
      when others then
        null;
    end;

    perform cron.schedule(
      'process-dataset-derivative-rebuilds',
      '* * * * *',
      'select util.process_dataset_derivative_rebuilds();'
    );
  end if;
end
$$;

create or replace function public.cmd_dataset_derivative_rebuild_read(
  p_request_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_request util.dataset_derivative_rebuild_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_request_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_REQUEST_INVALID',
      'status', 400,
      'message', 'request id is required'
    );
  end if;

  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.actor_user_id = v_actor;

  if v_request.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_REQUEST_NOT_AVAILABLE',
      'status', 404,
      'message', 'Derivative rebuild request is not available'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_derivative_rebuild_read',
    'schema_version', 'dataset-derivative-rebuild-status.v1',
    'request_id', v_request.id::text,
    'plan_sha256', v_request.plan_sha256,
    'operation_id', v_request.operation_id,
    'action_id', v_request.action_id,
    'table', v_request.target_table,
    'id', v_request.target_id::text,
    'version', v_request.target_version,
    'status', v_request.status,
    'phase', v_request.phase,
    'fence_active',
      v_request.status not in ('completed', 'stale', 'failed'),
    'plan_request_sha256', v_request.plan_request_sha256,
    'action_request_sha256', v_request.action_request_sha256,
    'database_audit_id', v_request.action_audit_id::text,
    'summary_audit_id', v_request.summary_audit_id::text,
    'completed_snapshot_sha256', v_request.completed_snapshot_sha256,
    'completed_at', v_request.completed_at,
    'terminal_at', v_request.terminal_at,
    'drained_at', v_request.drained_at,
    'error', v_request.last_error,
    'expected', jsonb_build_object(
      'snapshot_sha256', v_request.expected_snapshot_sha256,
      'modified_at', v_request.expected_modified_at,
      'json_sha256', v_request.expected_json_sha256,
      'json_ordered_sha256', v_request.expected_json_ordered_sha256,
      'extracted_text_sha256', v_request.expected_extracted_text_sha256,
      'before_extracted_md_sha256',
        v_request.before_extracted_md_sha256,
      'before_embedding_ft_sha256',
        v_request.before_embedding_ft_sha256,
      'before_embedding_ft_at', v_request.before_embedding_ft_at
    ),
    'quarantine', jsonb_build_object(
      'drain_not_before', v_request.drain_not_before,
      'failure_release_not_before',
        v_request.failure_release_not_before,
      'http_requests', v_request.quarantined_http_requests,
      'embedding_jobs', v_request.quarantined_embedding_jobs,
      'pending_jobs', v_request.quarantined_pending_jobs
    ),
    'markdown', jsonb_build_object(
      'request_id', case
        when v_request.markdown_request_id is null then null
        else v_request.markdown_request_id::text
      end,
      'dispatched_at', v_request.markdown_dispatched_at,
      'response_status', v_request.markdown_response_status,
      'response_received_at', v_request.markdown_response_received_at,
      'proposal_id', case
        when v_request.markdown_proposal_id is null then null
        else v_request.markdown_proposal_id::text
      end,
      'accepted_sha256', v_request.accepted_extracted_md_sha256
    ),
    'embedding', jsonb_build_object(
      'pending_job_id', case
        when v_request.embedding_pending_job_id is null then null
        else v_request.embedding_pending_job_id::text
      end,
      'queue_msg_id', case
        when v_request.embedding_queue_msg_id is null then null
        else v_request.embedding_queue_msg_id::text
      end,
      'queued_at', v_request.embedding_queued_at,
      'proposal_id', case
        when v_request.embedding_proposal_id is null then null
        else v_request.embedding_proposal_id::text
      end
    )
  );
end;
$$;

alter function public.cmd_dataset_derivative_rebuild_read(uuid)
  owner to postgres;
revoke all on function public.cmd_dataset_derivative_rebuild_read(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_derivative_rebuild_read(uuid)
  to authenticated;

comment on function public.cmd_dataset_derivative_rebuild_read(uuid) is
  'Owner-only, mutation-free readback for one durable derivative rebuild request. Pending drain/fence state is explicit and cannot be presented as completion.';
