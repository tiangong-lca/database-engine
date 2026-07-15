-- Exact cleanup companion for protected_alias_fixture.sql.
--
-- The Node runner replaces the same validated whole-token SQL literals used
-- for setup.  Every delete is fenced by actor, scenario-derived keys, or the
-- exact request ID.  There are intentionally no TRUNCATEs or global queue
-- deletes.

begin;

set local search_path = extensions, public, auth;
set local lock_timeout = '5s';
set local statement_timeout = '120s';

create temporary table preview_alias_cleanup_config (
  actor_user_id uuid primary key,
  actor_email text not null,
  scenario_id text not null,
  scenario_kind text not null,
  request_id uuid not null,
  preview_ref text not null,
  preview_url text not null,
  service_role_key text not null,
  vault_description text not null,
  operation_id text not null
) on commit drop;

insert into preview_alias_cleanup_config values (
  ({{ACTOR_UUID_SQL}})::uuid,
  lower(btrim(({{ACTOR_EMAIL_SQL}})::text)),
  btrim(({{SCENARIO_NAMESPACE_SQL}})::text),
  btrim(({{SCENARIO_KIND_SQL}})::text),
  ({{REQUEST_ID_SQL}})::uuid,
  lower(btrim(({{PREVIEW_REF_SQL}})::text)),
  btrim(({{PREVIEW_URL_SQL}})::text),
  ({{SERVICE_ROLE_KEY_SQL}})::text,
  'database-engine#262 protected-alias-preview-e2e scenario='
    || btrim(({{SCENARIO_NAMESPACE_SQL}})::text)
    || ' request=' || ({{REQUEST_ID_SQL}})::uuid::text,
  'preview-e2e-protected-alias-' || ({{REQUEST_ID_SQL}})::uuid::text
);

create or replace function pg_temp.preview_alias_cleanup_uuid(p_name text)
returns uuid
language sql
stable
strict
as $$
  select extensions.uuid_generate_v5(
    extensions.uuid_ns_url(),
    'database-engine#262/' || config.scenario_id || '/' || p_name
  )
  from pg_temp.preview_alias_cleanup_config as config
$$;

create or replace function pg_temp.preview_alias_cleanup_entity_id(
  p_dimension text,
  p_kind text,
  p_index integer default 0
) returns uuid
language sql
stable
as $$
  select pg_temp.preview_alias_cleanup_uuid(
    p_dimension || '/' || p_kind || '/' || p_index::text
  )
$$;

do $cleanup_gate$
declare
  config pg_temp.preview_alias_cleanup_config%rowtype;
  server_context jsonb;
  artifact_count integer;
  vault_name_count integer;
  exact_vault_count integer;
begin
  select * into strict config from pg_temp.preview_alias_cleanup_config;

  if config.actor_email !~ '^[^[:space:]@]+@[^[:space:]@]+$'
    or config.scenario_id !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$'
    or config.scenario_kind !~ '^[a-z0-9][a-z0-9_-]{1,63}$'
    or config.preview_ref !~ '^[a-z0-9][a-z0-9-]{3,127}$'
    or config.preview_ref = 'qgzvkongdjqiiamzbbts'
    or config.preview_url is distinct from
      'https://' || config.preview_ref || '.supabase.co'
    or config.service_role_key is distinct from btrim(config.service_role_key)
    or config.service_role_key ~ '[[:space:]]'
    or octet_length(config.service_role_key) not between 20 and 16384
    or config.vault_description is distinct from
      'database-engine#262 protected-alias-preview-e2e scenario='
        || config.scenario_id || ' request=' || config.request_id::text then
    raise exception 'cleanup requires the exact trusted hosted Preview context';
  end if;

  if not exists (
    select 1
    from auth.users as actor
    where actor.id = config.actor_user_id
      and lower(btrim(actor.email)) = config.actor_email
  ) then
    raise exception
      'cleanup requires the exact disposable Preview actor UUID/email identity';
  end if;

  select
    count(*)::integer,
    count(*) filter (
      where secret.description is not distinct from config.vault_description
        and (
          (secret.name = 'project_url'
            and secret.decrypted_secret is not distinct from config.preview_url)
          or (secret.name = 'project_secret_key'
            and secret.decrypted_secret is not distinct from config.service_role_key)
        )
    )::integer
  into vault_name_count, exact_vault_count
  from vault.decrypted_secrets as secret
  where secret.name in ('project_url', 'project_secret_key');

  if vault_name_count not in (0, 2)
    or (vault_name_count = 2 and exact_vault_count is distinct from 2) then
    raise exception
      'cleanup refuses partial, foreign, or value-mismatched Vault secrets';
  end if;

  if vault_name_count = 2 then
    server_context := util.dataset_alias_execution_server_context();
    if server_context->>'environment' is distinct from 'preview'
      or server_context->>'project_ref' is distinct from config.preview_ref then
      raise exception 'cleanup Vault context does not match the exact Preview ref';
    end if;
  end if;

  select count(*)::integer into artifact_count
  from public.command_audit_log as audit
  where audit.command = 'preview_e2e_protected_alias_fixture'
    and audit.actor_user_id = config.actor_user_id
    and audit.target_table = 'preview_e2e_protected_alias'
    and audit.target_id = config.request_id
    and audit.target_version = '00.00.001';

  if artifact_count > 1 then
    raise exception 'multiple sealed fixture artifacts exist for one request';
  end if;

  if artifact_count = 1 and exists (
    select 1
    from public.command_audit_log as audit
    where audit.command = 'preview_e2e_protected_alias_fixture'
      and audit.actor_user_id = config.actor_user_id
      and audit.target_table = 'preview_e2e_protected_alias'
      and audit.target_id = config.request_id
      and (
        audit.payload->>'schema_version' is distinct from 'protected-alias-preview-fixture.v1'
        or audit.payload->>'scenario_id' is distinct from config.scenario_id
        or audit.payload->>'scenario_kind' is distinct from config.scenario_kind
        or audit.payload->>'request_id' is distinct from config.request_id::text
        or audit.payload #>> '{preflight_request,project_ref}' is distinct from config.preview_ref
        or jsonb_typeof(audit.payload->'target_keys') is distinct from 'array'
        or jsonb_array_length(audit.payload->'target_keys') is distinct from 50
        or exists (
          select 1
          from jsonb_array_elements(audit.payload->'target_keys') as target(value)
          where jsonb_typeof(target.value) is distinct from 'object'
            or not (target.value ?& array['table', 'id', 'version'])
            or exists (
              select 1 from jsonb_object_keys(target.value) as target_key(key)
              where target_key.key <> all (array['table', 'id', 'version'])
            )
            or target.value->>'table' not in ('flows', 'processes')
            or (target.value->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            or (target.value->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        )
      )
  ) then
    raise exception 'sealed fixture artifact does not match cleanup parameters';
  end if;
end;
$cleanup_gate$;

create temporary table preview_alias_cleanup_derived_targets (
  table_name text not null,
  id uuid not null,
  version text not null,
  primary key (table_name, id, version)
) on commit drop;

insert into preview_alias_cleanup_derived_targets
select
  'flows',
  pg_temp.preview_alias_cleanup_entity_id(dimensions.dimension, 'flow', ordinal),
  '01.00.000'
from (values ('time'::text, 10), ('length_time'::text, 13)) as dimensions(dimension, row_count)
cross join lateral generate_series(1, dimensions.row_count) as ordinal
union all
select
  'processes',
  pg_temp.preview_alias_cleanup_entity_id(dimensions.dimension, 'process', ordinal),
  '01.00.000'
from (values ('time'::text, 14), ('length_time'::text, 13)) as dimensions(dimension, row_count)
cross join lateral generate_series(1, dimensions.row_count) as ordinal;

-- Queue/audit cleanup is allowed to use target identities only when they came
-- from the exact sealed manifest.  On an idempotent/missing-manifest cleanup
-- this table stays empty; only actor/request ledgers and actor-owned business
-- rows remain eligible for deletion.
create temporary table preview_alias_cleanup_targets (
  table_name text not null,
  id uuid not null,
  version text not null,
  primary key (table_name, id, version)
) on commit drop;

insert into preview_alias_cleanup_targets
select
  target.value->>'table',
  (target.value->>'id')::uuid,
  target.value->>'version'
from public.command_audit_log as audit
cross join pg_temp.preview_alias_cleanup_config as config
cross join lateral jsonb_array_elements(audit.payload->'target_keys') as target(value)
where audit.command = 'preview_e2e_protected_alias_fixture'
  and audit.actor_user_id = config.actor_user_id
  and audit.target_table = 'preview_e2e_protected_alias'
  and audit.target_id = config.request_id
  and audit.target_version = '00.00.001'
  and audit.payload->>'schema_version' is not distinct from 'protected-alias-preview-fixture.v1'
  and audit.payload->>'scenario_id' is not distinct from config.scenario_id
  and audit.payload->>'scenario_kind' is not distinct from config.scenario_kind
  and audit.payload->>'request_id' is not distinct from config.request_id::text;

do $sealed_target_gate$
begin
  if exists (select 1 from pg_temp.preview_alias_cleanup_targets)
    and (
      exists (
        select * from pg_temp.preview_alias_cleanup_targets
        except
        select * from pg_temp.preview_alias_cleanup_derived_targets
      )
      or exists (
        select * from pg_temp.preview_alias_cleanup_derived_targets
        except
        select * from pg_temp.preview_alias_cleanup_targets
      )
    ) then
    raise exception 'sealed fixture target_keys differ from scenario-derived business keys';
  end if;
end;
$sealed_target_gate$;

create temporary table preview_alias_cleanup_derivative_requests (
  id uuid primary key
) on commit drop;

insert into preview_alias_cleanup_derivative_requests
select request.id
from util.dataset_derivative_rebuild_requests as request
cross join pg_temp.preview_alias_cleanup_config as config
where request.actor_user_id = config.actor_user_id
  and (
    request.batch_id = config.request_id
    or exists (
      select 1 from pg_temp.preview_alias_cleanup_targets as target
      where target.table_name = request.target_table
        and target.id = request.target_id
        and target.version = request.target_version
    )
  );

create temporary table preview_alias_cleanup_http_ids (
  id bigint primary key
) on commit drop;

insert into preview_alias_cleanup_http_ids
select request.net_request_id
from util.dataset_alias_execution_requests as request
cross join pg_temp.preview_alias_cleanup_config as config
where request.id = config.request_id
  and request.actor_user_id = config.actor_user_id
  and request.net_request_id is not null
union
select request.markdown_request_id
from util.dataset_derivative_rebuild_requests as request
join pg_temp.preview_alias_cleanup_derivative_requests as cleanup_request
  on cleanup_request.id = request.id
where request.markdown_request_id is not null
union
select queued.id
from net.http_request_queue as queued
where exists (
  select 1 from pg_temp.preview_alias_cleanup_targets as target
  where util.dataset_derivative_rebuild_http_body_matches(
    queued.body,
    target.table_name,
    target.id,
    target.version
  )
);

delete from pgmq.q_dataset_extraction_jobs as queued
where queued.message->>'requestId' in (
    select id::text from pg_temp.preview_alias_cleanup_derivative_requests
  )
  or exists (
    select 1 from pg_temp.preview_alias_cleanup_targets as target
    where queued.message->>'schema' = 'public'
      and queued.message->>'table' = target.table_name
      and queued.message->>'id' = target.id::text
      and btrim(queued.message->>'version') = target.version
  );

delete from pgmq.q_embedding_jobs as queued
where queued.message->>'requestId' in (
    select id::text from pg_temp.preview_alias_cleanup_derivative_requests
  )
  or exists (
    select 1 from pg_temp.preview_alias_cleanup_targets as target
    where queued.message->>'schema' = 'public'
      and queued.message->>'table' = target.table_name
      and queued.message->>'id' = target.id::text
      and btrim(queued.message->>'version') = target.version
  );

delete from util.pending_embedding_jobs as pending
where pending.message->>'requestId' in (
    select id::text from pg_temp.preview_alias_cleanup_derivative_requests
  )
  or exists (
    select 1 from pg_temp.preview_alias_cleanup_targets as target
    where pending.schema_name = 'public'
      and pending.table_name = target.table_name
      and pending.record_id = target.id::text
      and btrim(pending.record_version) = target.version
  );

delete from util.embedding_job_failures as failure
where failure.message->>'requestId' in (
    select id::text from pg_temp.preview_alias_cleanup_derivative_requests
  )
  or exists (
    select 1 from pg_temp.preview_alias_cleanup_targets as target
    where failure.message->>'table' = target.table_name
      and failure.message->>'id' = target.id::text
      and btrim(failure.message->>'version') = target.version
  );

delete from net.http_request_queue as queued
where queued.id in (select id from pg_temp.preview_alias_cleanup_http_ids);

delete from net._http_response as response
where response.id in (select id from pg_temp.preview_alias_cleanup_http_ids);

delete from util.dataset_alias_execution_requests as request
using pg_temp.preview_alias_cleanup_config as config
where request.id = config.request_id
  and request.actor_user_id = config.actor_user_id;

delete from util.dataset_derivative_rebuild_requests as request
where request.id in (
  select id from pg_temp.preview_alias_cleanup_derivative_requests
);

delete from util.dataset_alias_execution_gate_receipts as receipt
using pg_temp.preview_alias_cleanup_config as config
where receipt.preflight_id = config.request_id
  and receipt.actor_user_id = config.actor_user_id;

delete from util.dataset_alias_execution_preflights as preflight
using pg_temp.preview_alias_cleanup_config as config
where preflight.id = config.request_id
  and preflight.actor_user_id = config.actor_user_id;

delete from public.command_audit_log as audit
using pg_temp.preview_alias_cleanup_config as config
where audit.actor_user_id = config.actor_user_id
  and (
    (
      audit.command = 'preview_e2e_protected_alias_fixture'
      and audit.target_table = 'preview_e2e_protected_alias'
      and audit.target_id = config.request_id
      and audit.target_version = '00.00.001'
      and audit.payload->>'scenario_id' is not distinct from config.scenario_id
    )
    or (
      audit.command in (
        'cmd_dataset_alias_batch_guarded',
        'cmd_dataset_alias_plan_guarded',
        'cmd_dataset_derivative_rebuild_plan_guarded',
        'cmd_dataset_derivative_rebuild_terminal'
      )
      and (
        audit.payload->>'operation_id' is not distinct from config.operation_id
        or audit.payload->>'request_id' is not distinct from config.request_id::text
        or audit.payload->>'request_id' in (
          select id::text from pg_temp.preview_alias_cleanup_derivative_requests
        )
        or exists (
          select 1 from pg_temp.preview_alias_cleanup_targets as target
          where audit.target_table = target.table_name
            and audit.target_id = target.id
            and btrim(audit.target_version) = target.version
        )
      )
    )
  );

-- Delete only scenario-derived business rows, with trigger side effects
-- suppressed so cleanup cannot enqueue fresh work.
set local session_replication_role = replica;

delete from public.processes as process
using pg_temp.preview_alias_cleanup_derived_targets as target,
      pg_temp.preview_alias_cleanup_config as config
where target.table_name = 'processes'
  and process.id = target.id
  and btrim(process.version) = target.version
  and process.user_id = config.actor_user_id;

delete from public.flows as flow
using pg_temp.preview_alias_cleanup_derived_targets as target,
      pg_temp.preview_alias_cleanup_config as config
where target.table_name = 'flows'
  and flow.id = target.id
  and btrim(flow.version) = target.version
  and flow.user_id = config.actor_user_id;

delete from public.flowproperties as flowproperty
using pg_temp.preview_alias_cleanup_config as config
where flowproperty.user_id = config.actor_user_id
  and (flowproperty.id, btrim(flowproperty.version)) in (
    (pg_temp.preview_alias_cleanup_entity_id('time', 'source_flowproperty'), '00.00.000'),
    (pg_temp.preview_alias_cleanup_entity_id('time', 'target_flowproperty'), '01.00.000'),
    (pg_temp.preview_alias_cleanup_entity_id('length_time', 'source_flowproperty'), '00.00.000'),
    (pg_temp.preview_alias_cleanup_entity_id('length_time', 'target_flowproperty'), '01.00.000')
  );

delete from public.unitgroups as unitgroup
using pg_temp.preview_alias_cleanup_config as config
where unitgroup.user_id = config.actor_user_id
  and (unitgroup.id, btrim(unitgroup.version)) in (
    (pg_temp.preview_alias_cleanup_entity_id('time', 'source_unitgroup'), '00.00.000'),
    (pg_temp.preview_alias_cleanup_entity_id('time', 'target_unitgroup'), '01.00.000'),
    (pg_temp.preview_alias_cleanup_entity_id('length_time', 'source_unitgroup'), '00.00.000'),
    (pg_temp.preview_alias_cleanup_entity_id('length_time', 'target_unitgroup'), '01.00.000')
  );

set local session_replication_role = origin;

-- Remove only the two secrets created by this exact scenario.  A partial or
-- foreign same-name row was rejected above and can never be overwritten or
-- deleted by this cleanup.
delete from vault.secrets as encrypted_secret
using vault.decrypted_secrets as secret,
      pg_temp.preview_alias_cleanup_config as config
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
  config pg_temp.preview_alias_cleanup_config%rowtype;
begin
  select * into strict config from pg_temp.preview_alias_cleanup_config;

  if exists (select 1 from util.dataset_alias_execution_requests where id = config.request_id)
    or exists (select 1 from util.dataset_alias_execution_preflights where id = config.request_id)
    or exists (select 1 from util.dataset_alias_execution_gate_receipts where preflight_id = config.request_id)
    or exists (select 1 from util.dataset_derivative_rebuild_requests where id in (
      select id from pg_temp.preview_alias_cleanup_derivative_requests
    ))
    or exists (select 1 from util.dataset_derivative_rebuild_proposals where request_id in (
      select id from pg_temp.preview_alias_cleanup_derivative_requests
    ))
    or exists (select 1 from util.dataset_derivative_rebuild_permits where request_id in (
      select id from pg_temp.preview_alias_cleanup_derivative_requests
    ))
    or exists (select 1 from net.http_request_queue where id in (
      select id from pg_temp.preview_alias_cleanup_http_ids
    ))
    or exists (select 1 from net._http_response where id in (
      select id from pg_temp.preview_alias_cleanup_http_ids
    ))
    or exists (select 1 from public.command_audit_log
      where actor_user_id = config.actor_user_id
        and command = 'preview_e2e_protected_alias_fixture'
        and target_id = config.request_id)
    or exists (select 1 from public.command_audit_log as audit
      where audit.actor_user_id = config.actor_user_id
        and audit.command in (
          'cmd_dataset_alias_batch_guarded',
          'cmd_dataset_alias_plan_guarded',
          'cmd_dataset_derivative_rebuild_plan_guarded',
          'cmd_dataset_derivative_rebuild_terminal'
        )
        and (
          audit.payload->>'operation_id' is not distinct from config.operation_id
          or audit.payload->>'request_id' is not distinct from config.request_id::text
          or audit.payload->>'request_id' in (
            select id::text from pg_temp.preview_alias_cleanup_derivative_requests
          )
          or exists (
            select 1 from pg_temp.preview_alias_cleanup_targets as target
            where audit.target_table = target.table_name
              and audit.target_id = target.id
              and btrim(audit.target_version) = target.version
          )
        ))
    or exists (select 1 from pgmq.q_dataset_extraction_jobs as queued
      where queued.message->>'requestId' in (
          select id::text from pg_temp.preview_alias_cleanup_derivative_requests
        )
        or exists (
          select 1 from pg_temp.preview_alias_cleanup_targets as target
          where queued.message->>'schema' = 'public'
            and queued.message->>'table' = target.table_name
            and queued.message->>'id' = target.id::text
            and btrim(queued.message->>'version') = target.version
        ))
    or exists (select 1 from pgmq.q_embedding_jobs as queued
      where queued.message->>'requestId' in (
          select id::text from pg_temp.preview_alias_cleanup_derivative_requests
        )
        or exists (
          select 1 from pg_temp.preview_alias_cleanup_targets as target
          where queued.message->>'schema' = 'public'
            and queued.message->>'table' = target.table_name
            and queued.message->>'id' = target.id::text
            and btrim(queued.message->>'version') = target.version
        ))
    or exists (select 1 from util.pending_embedding_jobs as pending
      where pending.message->>'requestId' in (
          select id::text from pg_temp.preview_alias_cleanup_derivative_requests
        )
        or exists (
          select 1 from pg_temp.preview_alias_cleanup_targets as target
          where pending.schema_name = 'public'
            and pending.table_name = target.table_name
            and pending.record_id = target.id::text
            and btrim(pending.record_version) = target.version
        ))
    or exists (select 1 from util.embedding_job_failures as failure
      where failure.message->>'requestId' in (
          select id::text from pg_temp.preview_alias_cleanup_derivative_requests
        )
        or exists (
          select 1 from pg_temp.preview_alias_cleanup_targets as target
          where failure.message->>'table' = target.table_name
            and failure.message->>'id' = target.id::text
            and btrim(failure.message->>'version') = target.version
        ))
    or exists (select 1 from public.flows as flow
      join pg_temp.preview_alias_cleanup_derived_targets target
        on target.table_name = 'flows' and target.id = flow.id
          and target.version = btrim(flow.version))
    or exists (select 1 from public.processes as process
      join pg_temp.preview_alias_cleanup_derived_targets target
        on target.table_name = 'processes' and target.id = process.id
          and target.version = btrim(process.version))
    or exists (select 1 from public.flowproperties where id in (
      pg_temp.preview_alias_cleanup_entity_id('time', 'source_flowproperty'),
      pg_temp.preview_alias_cleanup_entity_id('time', 'target_flowproperty'),
      pg_temp.preview_alias_cleanup_entity_id('length_time', 'source_flowproperty'),
      pg_temp.preview_alias_cleanup_entity_id('length_time', 'target_flowproperty')
    ))
    or exists (select 1 from public.unitgroups where id in (
      pg_temp.preview_alias_cleanup_entity_id('time', 'source_unitgroup'),
      pg_temp.preview_alias_cleanup_entity_id('time', 'target_unitgroup'),
      pg_temp.preview_alias_cleanup_entity_id('length_time', 'source_unitgroup'),
      pg_temp.preview_alias_cleanup_entity_id('length_time', 'target_unitgroup')
    ))
    or exists (
      select 1
      from vault.secrets as secret
      where secret.name in ('project_url', 'project_secret_key')
    ) then
    raise exception 'protected alias Preview cleanup left exact-scope residue';
  end if;
end;
$cleanup_assertions$;

-- Standard TAP output without leaving pgtap installed on the disposable
-- branch.  The fail-closed residue audit above backs every line.
select tap
from (values
  ('TAP version 13'),
  ('1..7'),
  ('ok 1 - protected execution ledger is removed'),
  ('ok 2 - preflight and gate ledger is removed'),
  ('ok 3 - derivative requests and cascading proposals/permits are removed'),
  ('ok 4 - exact HTTP and PGMQ queue rows are removed'),
  ('ok 5 - sealed fixture artifact and scoped audits are removed'),
  ('ok 6 - all scenario-derived business rows are removed'),
  ('ok 7 - exact branch-local Vault execution secrets are removed')
) as cleanup_tap(tap);

commit;
