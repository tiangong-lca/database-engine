-- Exact cleanup companion for protected_flow_identity_fixture.sql.
--
-- Every delete is fenced by the disposable actor, request, receipt/scope, or
-- scenario-derived UUID namespace.  There are no TRUNCATEs or global deletes.

begin;

set local search_path = extensions, public, auth;
set local lock_timeout = '5s';
set local statement_timeout = '180s';

create temporary table preview_flow_identity_cleanup_config (
  actor_user_id uuid primary key,
  actor_email text not null,
  foreign_user_id uuid not null unique,
  foreign_email text not null,
  scenario_id text not null,
  request_id uuid not null,
  preview_ref text not null,
  preview_url text not null,
  service_role_key text not null,
  vault_description text not null,
  operation_id text not null
) on commit drop;

insert into preview_flow_identity_cleanup_config values (
  ({{ACTOR_UUID_SQL}})::uuid,
  lower(btrim(({{ACTOR_EMAIL_SQL}})::text)),
  ({{FOREIGN_UUID_SQL}})::uuid,
  lower(btrim(({{FOREIGN_EMAIL_SQL}})::text)),
  btrim(({{SCENARIO_NAMESPACE_SQL}})::text),
  ({{REQUEST_ID_SQL}})::uuid,
  lower(btrim(({{PREVIEW_REF_SQL}})::text)),
  btrim(({{PREVIEW_URL_SQL}})::text),
  ({{SERVICE_ROLE_KEY_SQL}})::text,
  'database-engine#269 flow-identity-preview-e2e scenario='
    || btrim(({{SCENARIO_NAMESPACE_SQL}})::text)
    || ' request=' || ({{REQUEST_ID_SQL}})::uuid::text,
  'preview-flow-identity-' || ({{REQUEST_ID_SQL}})::uuid::text
);

create or replace function pg_temp.preview_flow_identity_cleanup_uuid(
  p_name text
) returns uuid
language sql
stable
strict
as $$
  select extensions.uuid_generate_v5(
    extensions.uuid_ns_url(),
    'database-engine#269/' || config.scenario_id || '/' || p_name
  )
  from pg_temp.preview_flow_identity_cleanup_config as config
$$;

do $cleanup_gate$
declare
  config pg_temp.preview_flow_identity_cleanup_config%rowtype;
  server_context jsonb;
  vault_name_count integer;
  exact_vault_count integer;
  manifest_count integer;
begin
  select * into strict config
  from pg_temp.preview_flow_identity_cleanup_config;

  if config.actor_user_id = config.foreign_user_id
    or config.actor_email = config.foreign_email
    or config.actor_email !~ '^[^[:space:]@]+@[^[:space:]@]+$'
    or config.foreign_email !~ '^[^[:space:]@]+@[^[:space:]@]+$'
    or config.scenario_id !~ '^fie2e-[a-z0-9_-]+-[0-9a-f]{24}$'
    or config.preview_ref !~ '^[a-z0-9][a-z0-9-]{3,127}$'
    or config.preview_ref = 'qgzvkongdjqiiamzbbts'
    or config.preview_url is distinct from
      'https://' || config.preview_ref || '.supabase.co'
    or config.service_role_key is distinct from btrim(config.service_role_key)
    or config.service_role_key ~ '[[:space:]]'
    or octet_length(config.service_role_key) not between 20 and 16384
    or config.vault_description is distinct from
      'database-engine#269 flow-identity-preview-e2e scenario='
        || config.scenario_id || ' request=' || config.request_id::text then
    raise exception 'cleanup requires exact trusted hosted Preview parameters';
  end if;

  if not exists (
    select 1 from auth.users as actor
    where actor.id = config.actor_user_id
      and lower(btrim(actor.email)) = config.actor_email
  ) or not exists (
    select 1 from auth.users as actor
    where actor.id = config.foreign_user_id
      and lower(btrim(actor.email)) = config.foreign_email
  ) then
    raise exception 'cleanup requires both exact disposable auth identities';
  end if;

  select
    count(*)::integer,
    count(*) filter (
      where secret.description is not distinct from config.vault_description
        and (
          (secret.name = 'project_url'
            and secret.decrypted_secret is not distinct from config.preview_url)
          or (secret.name = 'project_secret_key'
            and secret.decrypted_secret is not distinct from
              config.service_role_key)
        )
    )::integer
  into vault_name_count, exact_vault_count
  from vault.decrypted_secrets as secret
  where secret.name in ('project_url', 'project_secret_key');

  if vault_name_count not in (0, 2)
    or (vault_name_count = 2 and exact_vault_count is distinct from 2) then
    raise exception 'cleanup refuses partial, foreign, or mismatched Vault rows';
  end if;

  if vault_name_count = 2 then
    server_context := util.dataset_alias_execution_server_context();
    if server_context->>'environment' is distinct from 'preview'
      or server_context->>'project_ref' is distinct from config.preview_ref then
      raise exception 'cleanup Vault context differs from exact Preview ref';
    end if;
  end if;

  select count(*)::integer into manifest_count
  from public.command_audit_log as audit
  where audit.command = 'preview_e2e_flow_identity_fixture'
    and audit.actor_user_id = config.actor_user_id
    and audit.target_table = 'preview_e2e_flow_identity'
    and audit.target_id = config.request_id
    and audit.target_version = '00.00.001';

  if manifest_count > 1 then
    raise exception 'multiple fixture manifests exist for one request';
  end if;

  if manifest_count = 1 and exists (
    select 1
    from public.command_audit_log as audit
    where audit.command = 'preview_e2e_flow_identity_fixture'
      and audit.actor_user_id = config.actor_user_id
      and audit.target_table = 'preview_e2e_flow_identity'
      and audit.target_id = config.request_id
      and audit.target_version = '00.00.001'
      and (
        audit.payload->>'schema_version'
          is distinct from 'protected-flow-identity-preview-fixture.v1'
        or audit.payload->>'scenario_id' is distinct from config.scenario_id
        or audit.payload->>'request_id' is distinct from config.request_id::text
        or audit.payload->>'preview_ref' is distinct from config.preview_ref
        or audit.payload->>'operation_id' is distinct from config.operation_id
        or audit.payload #>> '{actor,user_id}'
          is distinct from config.actor_user_id::text
        or audit.payload #>> '{actor,email}' is distinct from config.actor_email
        or audit.payload #>> '{foreign_actor,user_id}'
          is distinct from config.foreign_user_id::text
        or audit.payload #>> '{foreign_actor,email}'
          is distinct from config.foreign_email
        or audit.payload #>> '{expected,source_count}' <> '305'
        or audit.payload #>> '{expected,process_count}' <> '2'
        or audit.payload #>> '{expected,rewrite_count}' <> '2'
      )
  ) then
    raise exception 'sealed fixture manifest does not match cleanup parameters';
  end if;
end
$cleanup_gate$;

create temporary table preview_flow_identity_cleanup_processes (
  id uuid primary key,
  version text not null
) on commit drop;

insert into preview_flow_identity_cleanup_processes
select pg_temp.preview_flow_identity_cleanup_uuid('process/' || ordinal),
  '01.00.000'
from generate_series(1, 2) as ordinal;

create temporary table preview_flow_identity_cleanup_flows (
  id uuid primary key,
  version text not null,
  expected_owner uuid not null
) on commit drop;

insert into preview_flow_identity_cleanup_flows
select pg_temp.preview_flow_identity_cleanup_uuid('source-flow'),
  '01.00.000', config.actor_user_id
from pg_temp.preview_flow_identity_cleanup_config as config
union all
select pg_temp.preview_flow_identity_cleanup_uuid('target-flow'),
  '01.00.000', config.foreign_user_id
from pg_temp.preview_flow_identity_cleanup_config as config
union all
select pg_temp.preview_flow_identity_cleanup_uuid('pending-flow'),
  '01.00.000', config.actor_user_id
from pg_temp.preview_flow_identity_cleanup_config as config
union all
select pg_temp.preview_flow_identity_cleanup_uuid('orphan/' || ordinal),
  '01.00.000', config.actor_user_id
from pg_temp.preview_flow_identity_cleanup_config as config
cross join generate_series(1, 303) as ordinal;

create temporary table preview_flow_identity_cleanup_receipts (
  id uuid primary key
) on commit drop;

insert into preview_flow_identity_cleanup_receipts
select receipt.id
from util.dataset_flow_identity_capture_receipts as receipt
cross join pg_temp.preview_flow_identity_cleanup_config as config
where receipt.actor_user_id = config.actor_user_id
  and receipt.request_id = config.request_id
  and receipt.operation_id = config.operation_id
  and receipt.environment = 'preview'
  and receipt.project_ref = config.preview_ref;

create temporary table preview_flow_identity_cleanup_scopes (
  id uuid primary key
) on commit drop;

insert into preview_flow_identity_cleanup_scopes
select scope.id
from util.dataset_flow_identity_scopes as scope
cross join pg_temp.preview_flow_identity_cleanup_config as config
where scope.actor_user_id = config.actor_user_id
  and scope.operation_id = config.operation_id
  and scope.environment = 'preview'
  and scope.project_ref = config.preview_ref
  and scope.receipt_id in (
    select id from pg_temp.preview_flow_identity_cleanup_receipts
  );

create temporary table preview_flow_identity_cleanup_derivatives (
  id uuid primary key,
  batch_id uuid
) on commit drop;

insert into preview_flow_identity_cleanup_derivatives
select distinct request.id, request.batch_id
from util.dataset_derivative_rebuild_requests as request
cross join pg_temp.preview_flow_identity_cleanup_config as config
where request.actor_user_id = config.actor_user_id
  and (
    request.batch_id in (
      select ledger.derivative_batch_id
      from util.dataset_flow_identity_process_ledger as ledger
      where ledger.scope_id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      ) and ledger.derivative_batch_id is not null
    )
    or exists (
      select 1
      from pg_temp.preview_flow_identity_cleanup_processes as target
      where request.target_table = 'processes'
        and request.target_id = target.id
        and btrim(request.target_version) = target.version
    )
  );

create temporary table preview_flow_identity_cleanup_http_ids (
  id bigint primary key
) on commit drop;

insert into preview_flow_identity_cleanup_http_ids
select request.markdown_request_id
from util.dataset_derivative_rebuild_requests as request
where request.id in (
    select id from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  and request.markdown_request_id is not null
union
select queued.id
from net.http_request_queue as queued
where exists (
  select 1
  from pg_temp.preview_flow_identity_cleanup_processes as target
  where util.dataset_derivative_rebuild_http_body_matches(
    queued.body, 'processes', target.id, target.version
  )
);

-- Fast-path cleanup is safe only before any exact derivative request has
-- crossed an external dispatch boundary.  Serialize with the durable
-- coordinator first, lock every captured request, and refuse cleanup if any
-- downstream proposal, permit, transport, queue, or failure evidence exists.
-- A refusal rolls back this whole transaction; durable recovery must take over
-- and callers must not retry this cleanup automatically.
do $derivative_cleanup_fast_path_gate$
declare
  expected_request_count integer;
  locked_request_count integer;
begin
  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtext('util.process_dataset_derivative_rebuilds')
  ) then
    raise exception using
      errcode = '55P03',
      message = 'derivative cleanup coordinator is active; durable recovery required and automatic retry is forbidden';
  end if;

  select count(*)::integer into expected_request_count
  from pg_temp.preview_flow_identity_cleanup_derivatives;

  perform request.id
  from util.dataset_derivative_rebuild_requests as request
  where request.id in (
    select id from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  order by request.id
  for update;
  get diagnostics locked_request_count = row_count;

  if locked_request_count is distinct from expected_request_count then
    raise exception using
      errcode = '40001',
      message = 'exact derivative request set changed before cleanup lock; durable recovery required and automatic retry is forbidden';
  end if;

  if exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.id in (
        select id from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      and not (
        (request.status = 'queued' and request.phase = 'admitted')
        or (
          request.status = 'dispatching'
          and request.phase = 'quarantining'
          and pg_catalog.clock_timestamp() < request.drain_not_before
        )
      )
  ) or exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.id in (
        select id from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      and (
        request.markdown_request_id is not null
        or request.markdown_dispatched_at is not null
        or request.markdown_deadline_at is not null
        or request.markdown_response_status is not null
        or request.markdown_response_received_at is not null
        or request.markdown_proposal_id is not null
        or request.accepted_extracted_md_sha256 is not null
        or request.embedding_pending_job_id is not null
        or request.embedding_queue_msg_id is not null
        or request.embedding_queued_at is not null
        or request.embedding_deadline_at is not null
        or request.embedding_proposal_id is not null
        or request.completed_snapshot_sha256 is not null
        or request.completed_at is not null
        or request.terminal_at is not null
        or request.drained_at is not null
        or request.failure_release_not_before is not null
        or request.last_error is not null
      )
  ) or exists (
    select 1
    from util.dataset_derivative_rebuild_proposals as proposal
    where proposal.request_id in (
      select id from pg_temp.preview_flow_identity_cleanup_derivatives
    )
  ) or exists (
    select 1
    from util.dataset_derivative_rebuild_permits as permit
    where permit.request_id in (
      select id from pg_temp.preview_flow_identity_cleanup_derivatives
    )
  ) or exists (
    select 1
    from public.command_audit_log as audit
    where audit.command = 'cmd_dataset_derivative_rebuild_terminal'
      and audit.payload->>'request_id' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
  ) or exists (
    select 1
    from net.http_request_queue
    where id in (select id from pg_temp.preview_flow_identity_cleanup_http_ids)
  ) or exists (
    select 1
    from net._http_response
    where id in (select id from pg_temp.preview_flow_identity_cleanup_http_ids)
  ) or exists (
    select 1
    from pgmq.q_dataset_extraction_jobs as queued
    where queued.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = 'processes'
          and queued.message->>'id' = target.id::text
          and btrim(queued.message->>'version') = target.version
      )
  ) or exists (
    select 1
    from pgmq.a_dataset_extraction_jobs as archived
    where archived.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where archived.message->>'schema' = 'public'
          and archived.message->>'table' = 'processes'
          and archived.message->>'id' = target.id::text
          and btrim(archived.message->>'version') = target.version
      )
  ) or exists (
    select 1
    from util.dataset_extraction_job_failures as failure
    where failure.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where failure.message->>'schema' = 'public'
          and failure.message->>'table' = 'processes'
          and failure.message->>'id' = target.id::text
          and btrim(failure.message->>'version') = target.version
      )
  ) or exists (
    select 1
    from pgmq.q_embedding_jobs as queued
    where queued.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = 'processes'
          and queued.message->>'id' = target.id::text
          and btrim(queued.message->>'version') = target.version
      )
  ) or exists (
    select 1
    from pgmq.a_embedding_jobs as archived
    where archived.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where archived.message->>'schema' = 'public'
          and archived.message->>'table' = 'processes'
          and archived.message->>'id' = target.id::text
          and btrim(archived.message->>'version') = target.version
      )
  ) or exists (
    select 1
    from util.pending_embedding_jobs as pending
    where pending.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where pending.schema_name = 'public'
          and pending.table_name = 'processes'
          and pending.record_id = target.id::text
          and btrim(pending.record_version) = target.version
      )
  ) or exists (
    select 1
    from util.embedding_job_failures as failure
    where failure.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      )
      or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where failure.message->>'table' = 'processes'
          and failure.message->>'id' = target.id::text
          and btrim(failure.message->>'version') = target.version
      )
  ) then
    raise exception using
      errcode = '55000',
      message = 'exact derivative request crossed the fast-path cleanup boundary; durable recovery required and automatic retry is forbidden';
  end if;
end
$derivative_cleanup_fast_path_gate$;

-- A failed test may leave its exact fault hook behind.  Refuse to drop any
-- same-named object unless the function carries this fixture's marker.
do $remove_fault_hook$
declare
  config pg_temp.preview_flow_identity_cleanup_config%rowtype;
  expected_marker text;
  function_source text;
begin
  select * into strict config
  from pg_temp.preview_flow_identity_cleanup_config;
  expected_marker := 'database-engine#269 hosted Preview fault scenario='
    || config.scenario_id || ' request=' || config.request_id::text;
  if exists (
    select 1 from pg_trigger
    where tgname = 'preview_flow_identity_post_primary_fault_v1'
      and tgrelid = 'public.command_audit_log'::regclass
      and not tgisinternal
  ) then
    if to_regprocedure(
      'private.preview_flow_identity_post_primary_fault_v1()'
    ) is null or exists (
      select 1
      from pg_trigger as fault_trigger
      where fault_trigger.tgname =
          'preview_flow_identity_post_primary_fault_v1'
        and fault_trigger.tgrelid = 'public.command_audit_log'::regclass
        and not fault_trigger.tgisinternal
        and fault_trigger.tgfoid is distinct from to_regprocedure(
          'private.preview_flow_identity_post_primary_fault_v1()'
        )::oid
    ) then
      raise exception
        'cleanup refuses a fault trigger bound to a foreign function';
    end if;

    select procedure.prosrc into function_source
    from pg_proc as procedure
    join pg_namespace as namespace on namespace.oid = procedure.pronamespace
    where namespace.nspname = 'private'
      and procedure.proname = 'preview_flow_identity_post_primary_fault_v1'
      and procedure.pronargs = 0;
    if function_source is null
      or position(expected_marker in function_source) = 0 then
      raise exception 'cleanup refuses a foreign same-named fault hook';
    end if;
    drop trigger preview_flow_identity_post_primary_fault_v1
      on public.command_audit_log;
    drop function private.preview_flow_identity_post_primary_fault_v1();
  elsif to_regprocedure(
    'private.preview_flow_identity_post_primary_fault_v1()'
  ) is not null then
    raise exception 'orphan same-named fault function requires manual review';
  end if;
end
$remove_fault_hook$;

delete from pgmq.q_dataset_extraction_jobs as queued
where queued.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where queued.message->>'schema' = 'public'
      and queued.message->>'table' = 'processes'
      and queued.message->>'id' = target.id::text
      and btrim(queued.message->>'version') = target.version
  );

delete from pgmq.a_dataset_extraction_jobs as archived
where archived.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where archived.message->>'schema' = 'public'
      and archived.message->>'table' = 'processes'
      and archived.message->>'id' = target.id::text
      and btrim(archived.message->>'version') = target.version
  );

delete from util.dataset_extraction_job_failures as failure
where failure.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where failure.message->>'schema' = 'public'
      and failure.message->>'table' = 'processes'
      and failure.message->>'id' = target.id::text
      and btrim(failure.message->>'version') = target.version
  );

delete from pgmq.q_embedding_jobs as queued
where queued.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where queued.message->>'schema' = 'public'
      and queued.message->>'table' = 'processes'
      and queued.message->>'id' = target.id::text
      and btrim(queued.message->>'version') = target.version
  );

delete from pgmq.a_embedding_jobs as archived
where archived.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where archived.message->>'schema' = 'public'
      and archived.message->>'table' = 'processes'
      and archived.message->>'id' = target.id::text
      and btrim(archived.message->>'version') = target.version
  );

delete from util.pending_embedding_jobs as pending
where pending.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where pending.schema_name = 'public'
      and pending.table_name = 'processes'
      and pending.record_id = target.id::text
      and btrim(pending.record_version) = target.version
  );

delete from util.embedding_job_failures as failure
where failure.message->>'requestId' in (
    select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
  )
  or exists (
    select 1
    from pg_temp.preview_flow_identity_cleanup_processes as target
    where failure.message->>'table' = 'processes'
      and failure.message->>'id' = target.id::text
      and btrim(failure.message->>'version') = target.version
  );

delete from net.http_request_queue
where id in (select id from pg_temp.preview_flow_identity_cleanup_http_ids);

delete from net._http_response
where id in (select id from pg_temp.preview_flow_identity_cleanup_http_ids);

delete from util.dataset_derivative_rebuild_requests
where id in (select id from pg_temp.preview_flow_identity_cleanup_derivatives);

delete from util.dataset_flow_identity_mutation_permits
where scope_id in (select id from pg_temp.preview_flow_identity_cleanup_scopes);

delete from util.dataset_flow_identity_process_ledger
where scope_id in (select id from pg_temp.preview_flow_identity_cleanup_scopes);

delete from util.dataset_flow_identity_mappings
where scope_id in (select id from pg_temp.preview_flow_identity_cleanup_scopes);

-- The scope points back to its terminal wrapper with ON DELETE RESTRICT while
-- every wrapper also points to the scope.  Lock the selected relation, prove
-- that any terminal wrapper belongs to that same exact scope, and only then
-- sever the nullable back-reference so the two exact rows can be removed.
do $unlink_final_wrapper_invocations$
begin
  perform scope.id
  from util.dataset_flow_identity_scopes as scope
  where scope.id in (
    select id from pg_temp.preview_flow_identity_cleanup_scopes
  )
  for update;

  perform invocation.id
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id in (
    select scope.final_wrapper_invocation_id
    from util.dataset_flow_identity_scopes as scope
    where scope.id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      )
      and scope.final_wrapper_invocation_id is not null
  )
  for update;

  if exists (
    select 1
    from util.dataset_flow_identity_scopes as scope
    left join util.dataset_flow_identity_wrapper_invocations as invocation
      on invocation.id = scope.final_wrapper_invocation_id
    where scope.id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      )
      and scope.final_wrapper_invocation_id is not null
      and invocation.scope_id is distinct from scope.id
  ) then
    raise exception
      'cleanup refuses a terminal wrapper outside its exact selected scope';
  end if;

  update util.dataset_flow_identity_scopes as scope
  set final_wrapper_invocation_id = null
  where scope.id in (
      select id from pg_temp.preview_flow_identity_cleanup_scopes
    )
    and scope.final_wrapper_invocation_id is not null;
end
$unlink_final_wrapper_invocations$;

delete from util.dataset_flow_identity_wrapper_invocations
where scope_id in (select id from pg_temp.preview_flow_identity_cleanup_scopes);

delete from util.dataset_flow_identity_scopes
where id in (select id from pg_temp.preview_flow_identity_cleanup_scopes);

-- Capture receipts are intentionally immutable in normal operation.  Cleanup
-- disables only the six exact immutability triggers, deletes the exact sealed
-- relation, and restores every trigger before commit.
do $immutable_trigger_gate$
begin
  if (select count(*)
      from pg_trigger as immutable_trigger
      join (values
        ('util.dataset_flow_identity_capture_source_guards'::regclass,
          'dataset_flow_identity_capture_sources_immutable'),
        ('util.dataset_flow_identity_capture_target_guards'::regclass,
          'dataset_flow_identity_capture_targets_immutable'),
        ('util.dataset_flow_identity_capture_support_guards'::regclass,
          'dataset_flow_identity_capture_support_immutable'),
        ('util.dataset_flow_identity_capture_mapping_guards'::regclass,
          'dataset_flow_identity_capture_mappings_immutable'),
        ('util.dataset_flow_identity_capture_process_intents'::regclass,
          'dataset_flow_identity_capture_processes_immutable'),
        ('util.dataset_flow_identity_capture_receipts'::regclass,
          'dataset_flow_identity_capture_receipts_immutable')
      ) as expected(relation_id, trigger_name)
        on immutable_trigger.tgrelid = expected.relation_id
       and immutable_trigger.tgname = expected.trigger_name
      where not immutable_trigger.tgisinternal
        and immutable_trigger.tgenabled = 'O') <> 6 then
    raise exception
      'cleanup requires all exact capture immutability triggers enabled';
  end if;
end
$immutable_trigger_gate$;

alter table util.dataset_flow_identity_capture_source_guards
  disable trigger dataset_flow_identity_capture_sources_immutable;
alter table util.dataset_flow_identity_capture_target_guards
  disable trigger dataset_flow_identity_capture_targets_immutable;
alter table util.dataset_flow_identity_capture_support_guards
  disable trigger dataset_flow_identity_capture_support_immutable;
alter table util.dataset_flow_identity_capture_mapping_guards
  disable trigger dataset_flow_identity_capture_mappings_immutable;
alter table util.dataset_flow_identity_capture_process_intents
  disable trigger dataset_flow_identity_capture_processes_immutable;
alter table util.dataset_flow_identity_capture_receipts
  disable trigger dataset_flow_identity_capture_receipts_immutable;

delete from util.dataset_flow_identity_capture_source_guards
where receipt_id in (
  select id from pg_temp.preview_flow_identity_cleanup_receipts
);
delete from util.dataset_flow_identity_capture_target_guards
where receipt_id in (
  select id from pg_temp.preview_flow_identity_cleanup_receipts
);
delete from util.dataset_flow_identity_capture_support_guards
where receipt_id in (
  select id from pg_temp.preview_flow_identity_cleanup_receipts
);
delete from util.dataset_flow_identity_capture_mapping_guards
where receipt_id in (
  select id from pg_temp.preview_flow_identity_cleanup_receipts
);
delete from util.dataset_flow_identity_capture_process_intents
where receipt_id in (
  select id from pg_temp.preview_flow_identity_cleanup_receipts
);
delete from util.dataset_flow_identity_capture_receipts
where id in (select id from pg_temp.preview_flow_identity_cleanup_receipts);

alter table util.dataset_flow_identity_capture_source_guards
  enable trigger dataset_flow_identity_capture_sources_immutable;
alter table util.dataset_flow_identity_capture_target_guards
  enable trigger dataset_flow_identity_capture_targets_immutable;
alter table util.dataset_flow_identity_capture_support_guards
  enable trigger dataset_flow_identity_capture_support_immutable;
alter table util.dataset_flow_identity_capture_mapping_guards
  enable trigger dataset_flow_identity_capture_mappings_immutable;
alter table util.dataset_flow_identity_capture_process_intents
  enable trigger dataset_flow_identity_capture_processes_immutable;
alter table util.dataset_flow_identity_capture_receipts
  enable trigger dataset_flow_identity_capture_receipts_immutable;

delete from public.command_audit_log as audit
using pg_temp.preview_flow_identity_cleanup_config as config
where audit.actor_user_id = config.actor_user_id
  and (
    (
      audit.command = 'preview_e2e_flow_identity_fixture'
      and audit.target_table = 'preview_e2e_flow_identity'
      and audit.target_id = config.request_id
      and audit.target_version = '00.00.001'
      and audit.payload->>'scenario_id' is not distinct from config.scenario_id
    )
    or (
      audit.command in (
        'cmd_dataset_flow_identity_scope_preflight_guarded',
        'cmd_dataset_flow_identity_process_rewrite_guarded',
        'cmd_dataset_flow_identity_scope_finalize_guarded',
        'cmd_dataset_derivative_rebuild_plan_guarded',
        'cmd_dataset_derivative_rebuild_terminal'
      )
      and (
        audit.payload->>'operation_id' is not distinct from config.operation_id
        or audit.payload->>'scope_id' in (
          select id::text from pg_temp.preview_flow_identity_cleanup_scopes
        )
        or audit.payload->>'request_id' in (
          select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
        )
        or audit.payload->>'derivative_batch_id' in (
          select batch_id::text
          from pg_temp.preview_flow_identity_cleanup_derivatives
          where batch_id is not null
        )
        or exists (
          select 1
          from pg_temp.preview_flow_identity_cleanup_processes as target
          where audit.target_table = 'processes'
            and audit.target_id = target.id
            and btrim(audit.target_version) = target.version
        )
      )
    )
  );

-- Suppress dataset side effects while deleting only namespace-bound rows.
set local session_replication_role = replica;

delete from public.processes as process
using pg_temp.preview_flow_identity_cleanup_processes as target,
      pg_temp.preview_flow_identity_cleanup_config as config
where process.id = target.id
  and btrim(process.version) = target.version
  and process.user_id = config.actor_user_id;

delete from public.flows as flow
using pg_temp.preview_flow_identity_cleanup_flows as target
where flow.id = target.id
  and btrim(flow.version) = target.version
  and flow.user_id = target.expected_owner;

delete from public.flowproperties as flowproperty
using pg_temp.preview_flow_identity_cleanup_config as config
where flowproperty.id = pg_temp.preview_flow_identity_cleanup_uuid(
    'flowproperty'
  )
  and btrim(flowproperty.version) = '01.00.000'
  and flowproperty.user_id = config.foreign_user_id;

delete from public.unitgroups as unitgroup
using pg_temp.preview_flow_identity_cleanup_config as config
where unitgroup.id = pg_temp.preview_flow_identity_cleanup_uuid('unitgroup')
  and btrim(unitgroup.version) = '01.00.000'
  and unitgroup.user_id = config.foreign_user_id;

set local session_replication_role = origin;

delete from vault.secrets as encrypted_secret
using vault.decrypted_secrets as secret,
      pg_temp.preview_flow_identity_cleanup_config as config
where encrypted_secret.id = secret.id
  and secret.name in ('project_url', 'project_secret_key')
  and secret.description is not distinct from config.vault_description
  and (
    (secret.name = 'project_url'
      and secret.decrypted_secret is not distinct from config.preview_url)
    or (secret.name = 'project_secret_key'
      and secret.decrypted_secret is not distinct from config.service_role_key)
  );

do $cleanup_assertions$
declare
  config pg_temp.preview_flow_identity_cleanup_config%rowtype;
begin
  select * into strict config
  from pg_temp.preview_flow_identity_cleanup_config;

  if exists (select 1 from util.dataset_flow_identity_mutation_permits
      where scope_id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      ))
    or exists (select 1 from util.dataset_flow_identity_process_ledger
      where scope_id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      ))
    or exists (select 1 from util.dataset_flow_identity_mappings
      where scope_id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      ))
    or exists (select 1 from util.dataset_flow_identity_wrapper_invocations
      where scope_id in (
        select id from pg_temp.preview_flow_identity_cleanup_scopes
      ))
    or exists (select 1 from util.dataset_flow_identity_scopes
      where id in (select id from pg_temp.preview_flow_identity_cleanup_scopes))
    or exists (select 1 from util.dataset_flow_identity_capture_source_guards
      where receipt_id in (
        select id from pg_temp.preview_flow_identity_cleanup_receipts
      ))
    or exists (select 1 from util.dataset_flow_identity_capture_target_guards
      where receipt_id in (
        select id from pg_temp.preview_flow_identity_cleanup_receipts
      ))
    or exists (select 1 from util.dataset_flow_identity_capture_support_guards
      where receipt_id in (
        select id from pg_temp.preview_flow_identity_cleanup_receipts
      ))
    or exists (select 1 from util.dataset_flow_identity_capture_mapping_guards
      where receipt_id in (
        select id from pg_temp.preview_flow_identity_cleanup_receipts
      ))
    or exists (select 1 from util.dataset_flow_identity_capture_process_intents
      where receipt_id in (
        select id from pg_temp.preview_flow_identity_cleanup_receipts
      ))
    or exists (select 1 from util.dataset_flow_identity_capture_receipts
      where id in (select id from pg_temp.preview_flow_identity_cleanup_receipts))
    or exists (select 1 from util.dataset_derivative_rebuild_requests
      where id in (
        select id from pg_temp.preview_flow_identity_cleanup_derivatives
      ))
    or exists (select 1 from net.http_request_queue
      where id in (select id from pg_temp.preview_flow_identity_cleanup_http_ids))
    or exists (select 1 from net._http_response
      where id in (select id from pg_temp.preview_flow_identity_cleanup_http_ids))
    or exists (select 1 from pgmq.q_dataset_extraction_jobs as queued
      where queued.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = 'processes'
          and queued.message->>'id' = target.id::text
          and btrim(queued.message->>'version') = target.version
      ))
    or exists (select 1 from pgmq.q_embedding_jobs as queued
      where queued.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = 'processes'
          and queued.message->>'id' = target.id::text
          and btrim(queued.message->>'version') = target.version
      ))
    or exists (select 1 from pgmq.a_dataset_extraction_jobs as archived
      where archived.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where archived.message->>'schema' = 'public'
          and archived.message->>'table' = 'processes'
          and archived.message->>'id' = target.id::text
          and btrim(archived.message->>'version') = target.version
      ))
    or exists (select 1 from util.dataset_extraction_job_failures as failure
      where failure.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where failure.message->>'schema' = 'public'
          and failure.message->>'table' = 'processes'
          and failure.message->>'id' = target.id::text
          and btrim(failure.message->>'version') = target.version
      ))
    or exists (select 1 from pgmq.a_embedding_jobs as archived
      where archived.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where archived.message->>'schema' = 'public'
          and archived.message->>'table' = 'processes'
          and archived.message->>'id' = target.id::text
          and btrim(archived.message->>'version') = target.version
      ))
    or exists (select 1 from util.pending_embedding_jobs as pending
      where pending.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where pending.schema_name = 'public'
          and pending.table_name = 'processes'
          and pending.record_id = target.id::text
          and btrim(pending.record_version) = target.version
      ))
    or exists (select 1 from util.embedding_job_failures as failure
      where failure.message->>'requestId' in (
        select id::text from pg_temp.preview_flow_identity_cleanup_derivatives
      ) or exists (
        select 1
        from pg_temp.preview_flow_identity_cleanup_processes as target
        where failure.message->>'table' = 'processes'
          and failure.message->>'id' = target.id::text
          and btrim(failure.message->>'version') = target.version
      ))
    or exists (select 1 from public.command_audit_log as audit
      where audit.actor_user_id = config.actor_user_id and (
        (audit.command = 'preview_e2e_flow_identity_fixture'
          and audit.target_id = config.request_id)
        or audit.payload->>'operation_id' is not distinct from
          config.operation_id
        or audit.payload->>'scope_id' in (
          select id::text from pg_temp.preview_flow_identity_cleanup_scopes
        )
        or exists (
          select 1
          from pg_temp.preview_flow_identity_cleanup_processes as target
          where audit.target_table = 'processes'
            and audit.target_id = target.id
            and btrim(audit.target_version) = target.version
        )
      ))
    or exists (select 1 from public.processes as process
      join pg_temp.preview_flow_identity_cleanup_processes as target
        on target.id = process.id
          and target.version = btrim(process.version))
    or exists (select 1 from public.flows as flow
      join pg_temp.preview_flow_identity_cleanup_flows as target
        on target.id = flow.id and target.version = btrim(flow.version))
    or exists (select 1 from public.flowproperties
      where id = pg_temp.preview_flow_identity_cleanup_uuid('flowproperty'))
    or exists (select 1 from public.unitgroups
      where id = pg_temp.preview_flow_identity_cleanup_uuid('unitgroup'))
    or exists (select 1 from vault.secrets
      where name in ('project_url', 'project_secret_key'))
    or exists (select 1 from pg_trigger
      where tgname = 'preview_flow_identity_post_primary_fault_v1'
        and tgrelid = 'public.command_audit_log'::regclass
        and not tgisinternal)
    or to_regprocedure(
      'private.preview_flow_identity_post_primary_fault_v1()'
    ) is not null then
    raise exception 'flow-identity Preview cleanup left exact-scope residue';
  end if;
end
$cleanup_assertions$;

select tap
from (values
  ('TAP version 13'),
  ('1..9'),
  ('ok 1 - exact fault hook is absent'),
  ('ok 2 - Step 3 scope, ledgers, invocation, and permits are removed'),
  ('ok 3 - immutable capture receipt relation is removed'),
  ('ok 4 - derivative child requests are removed'),
  ('ok 5 - exact HTTP, PGMQ, and pending queue rows are removed'),
  ('ok 6 - fixture manifest and scoped audits are removed'),
  ('ok 7 - 306 flows and two processes are removed'),
  ('ok 8 - public support fixture rows are removed'),
  ('ok 9 - exact branch-local Vault execution secrets are removed')
) as cleanup_tap(tap);

commit;
