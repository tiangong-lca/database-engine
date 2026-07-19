-- Read-only, exact-scope residue proof for the guarded Step 3 Hosted Preview E2E.
--
-- A dedicated renderer must replace every whole {{..._SQL}} token with one
-- validated SQL literal.  JSON tokens are canonical JSON string arrays encoded
-- as one SQL string literal (for example, '[]' or '["uuid"]'); they are never
-- raw SQL fragments.  Required tokens:
--
--   ACTOR_UUID_SQL                 owner disposable Auth user UUID
--   FOREIGN_UUID_SQL               foreign disposable Auth user UUID
--   SCENARIO_NAMESPACE_SQL         exact fie2e-hosted-<24 lowercase hex>
--   REQUEST_ID_SQL                 capture-attestation request UUID
--   PREVIEW_REF_SQL                exact non-production Supabase project ref
--   OPERATION_ID_SQL               preview-flow-identity-<REQUEST_ID_SQL>
--   CAPTURE_RECEIPT_IDS_JSON_SQL   zero or one observed capture-receipt UUID
--   SCOPE_IDS_JSON_SQL             zero or one observed Step 3 scope UUID
--   DERIVATIVE_REQUEST_IDS_JSON_SQL zero to two observed derivative UUIDs
--   DERIVATIVE_BATCH_IDS_JSON_SQL  zero to two observed derivative batch UUIDs
--   HTTP_REQUEST_IDS_JSON_SQL      zero to two observed pg_net bigint IDs
--   FIXTURE_BACKEND_PIDS_JSON_SQL  zero to 64 observed positive backend PIDs
--
-- The ID arrays must be captured before cleanup.  They let this proof address
-- child rows which no longer have a discoverable parent after a partial cleanup.
-- No URL, Auth token, database URL, anon key, service-role key, or Vault value is
-- accepted by this artifact.  The expected fixture application_name is derived
-- as fi269-<SCENARIO_NAMESPACE_SQL>; the full runner must set and assert it on
-- every fixture connection.  An outer recovery wrapper may also bind observed
-- backend PIDs when a child process exits ambiguously.

\set ON_ERROR_STOP on

begin read only;

set local search_path = extensions, public, auth;
set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $validate_bound_readback$
declare
  v_actor uuid := ({{ACTOR_UUID_SQL}})::uuid;
  v_foreign uuid := ({{FOREIGN_UUID_SQL}})::uuid;
  v_scenario text := btrim(({{SCENARIO_NAMESPACE_SQL}})::text);
  v_request uuid := ({{REQUEST_ID_SQL}})::uuid;
  v_preview_ref text := lower(btrim(({{PREVIEW_REF_SQL}})::text));
  v_operation text := btrim(({{OPERATION_ID_SQL}})::text);
  v_name text;
  v_values jsonb;
  v_limit integer;
begin
  if current_database() <> 'postgres'
    or current_setting('transaction_read_only') <> 'on' then
    raise exception 'residue readback must run read-only against postgres';
  end if;

  if v_actor = v_foreign
    or v_scenario !~ '^fie2e-hosted-[0-9a-f]{24}$'
    or v_preview_ref !~ '^[a-z0-9][a-z0-9-]{3,127}$'
    or v_preview_ref = 'qgzvkongdjqiiamzbbts'
    or v_operation is distinct from
      'preview-flow-identity-' || v_request::text then
    raise exception 'invalid exact Hosted Preview residue selectors';
  end if;

  for v_name, v_values, v_limit in
    select selector.name, selector.values_json, selector.maximum_count
    from (values
      ('capture_receipt_ids',
        ({{CAPTURE_RECEIPT_IDS_JSON_SQL}})::jsonb, 1),
      ('scope_ids', ({{SCOPE_IDS_JSON_SQL}})::jsonb, 1),
      ('derivative_request_ids',
        ({{DERIVATIVE_REQUEST_IDS_JSON_SQL}})::jsonb, 2),
      ('derivative_batch_ids',
        ({{DERIVATIVE_BATCH_IDS_JSON_SQL}})::jsonb, 2)
    ) as selector(name, values_json, maximum_count)
  loop
    if jsonb_typeof(v_values) is distinct from 'array'
      or jsonb_array_length(v_values) > v_limit
      or exists (
        select 1
        from jsonb_array_elements_text(v_values) as entry(value)
        where entry.value !~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      )
      or (select count(*) from jsonb_array_elements_text(v_values))
        is distinct from
        (select count(distinct entry.value)
         from jsonb_array_elements_text(v_values) as entry(value)) then
      raise exception 'invalid bounded UUID selector set: %', v_name;
    end if;
  end loop;

  v_values := ({{HTTP_REQUEST_IDS_JSON_SQL}})::jsonb;
  if jsonb_typeof(v_values) is distinct from 'array'
    or jsonb_array_length(v_values) > 2
    or exists (
      select 1 from jsonb_array_elements_text(v_values) as entry(value)
      where entry.value !~ '^[1-9][0-9]{0,18}$'
    )
    or exists (
      select 1 from jsonb_array_elements_text(v_values) as entry(value)
      where entry.value::numeric > 9223372036854775807
    )
    or (select count(*) from jsonb_array_elements_text(v_values))
      is distinct from
      (select count(distinct entry.value)
       from jsonb_array_elements_text(v_values) as entry(value)) then
    raise exception 'invalid bounded pg_net request-id selector set';
  end if;

  v_values := ({{FIXTURE_BACKEND_PIDS_JSON_SQL}})::jsonb;
  if jsonb_typeof(v_values) is distinct from 'array'
    or jsonb_array_length(v_values) > 64
    or exists (
      select 1 from jsonb_array_elements_text(v_values) as entry(value)
      where entry.value !~ '^[1-9][0-9]{0,9}$'
    )
    or exists (
      select 1 from jsonb_array_elements_text(v_values) as entry(value)
      where entry.value::numeric > 2147483647
    )
    or (select count(*) from jsonb_array_elements_text(v_values))
      is distinct from
      (select count(distinct entry.value)
       from jsonb_array_elements_text(v_values) as entry(value)) then
    raise exception 'invalid bounded fixture backend-pid selector set';
  end if;
end
$validate_bound_readback$;

with
params as (
  select
    ({{ACTOR_UUID_SQL}})::uuid as actor_user_id,
    ({{FOREIGN_UUID_SQL}})::uuid as foreign_user_id,
    btrim(({{SCENARIO_NAMESPACE_SQL}})::text) as scenario_id,
    ({{REQUEST_ID_SQL}})::uuid as capture_request_id,
    lower(btrim(({{PREVIEW_REF_SQL}})::text)) as preview_ref,
    btrim(({{OPERATION_ID_SQL}})::text) as operation_id,
    'fi269-' || btrim(({{SCENARIO_NAMESPACE_SQL}})::text)
      as fixture_application_name
),
bound_receipt_ids as (
  select entry.value::uuid as id
  from jsonb_array_elements_text(
    ({{CAPTURE_RECEIPT_IDS_JSON_SQL}})::jsonb
  ) as entry(value)
),
bound_scope_ids as (
  select entry.value::uuid as id
  from jsonb_array_elements_text(({{SCOPE_IDS_JSON_SQL}})::jsonb)
    as entry(value)
),
bound_derivative_request_ids as (
  select entry.value::uuid as id
  from jsonb_array_elements_text(
    ({{DERIVATIVE_REQUEST_IDS_JSON_SQL}})::jsonb
  ) as entry(value)
),
bound_derivative_batch_ids as (
  select entry.value::uuid as id
  from jsonb_array_elements_text(
    ({{DERIVATIVE_BATCH_IDS_JSON_SQL}})::jsonb
  ) as entry(value)
),
bound_http_request_ids as (
  select entry.value::bigint as id
  from jsonb_array_elements_text(
    ({{HTTP_REQUEST_IDS_JSON_SQL}})::jsonb
  ) as entry(value)
),
bound_backend_pids as (
  select entry.value::integer as pid
  from jsonb_array_elements_text(
    ({{FIXTURE_BACKEND_PIDS_JSON_SQL}})::jsonb
  ) as entry(value)
),
derived_ids as (
  select entity.name,
    extensions.uuid_generate_v5(
      extensions.uuid_ns_url(),
      'database-engine#269/' || params.scenario_id || '/' || entity.name
    ) as id
  from params
  cross join lateral (values
    ('unitgroup'),
    ('flowproperty'),
    ('source-flow'),
    ('target-flow'),
    ('pending-flow'),
    ('process/1'),
    ('process/2')
  ) as entity(name)
  union all
  select 'orphan/' || ordinal,
    extensions.uuid_generate_v5(
      extensions.uuid_ns_url(),
      'database-engine#269/' || params.scenario_id
        || '/orphan/' || ordinal
    )
  from params
  cross join generate_series(1, 303) as ordinal
),
process_targets as (
  select id, '01.00.000'::text as version
  from derived_ids
  where name in ('process/1', 'process/2')
),
flow_targets as (
  select id, '01.00.000'::text as version
  from derived_ids
  where name in ('source-flow', 'target-flow', 'pending-flow')
    or name like 'orphan/%'
),
receipt_ids as (
  select id from bound_receipt_ids
  union
  select receipt.id
  from util.dataset_flow_identity_capture_receipts as receipt
  cross join params
  where receipt.actor_user_id = params.actor_user_id
    and receipt.request_id = params.capture_request_id
    and receipt.operation_id = params.operation_id
    and receipt.environment = 'preview'
    and receipt.project_ref = params.preview_ref
),
scope_ids as (
  select id from bound_scope_ids
  union
  select scope.id
  from util.dataset_flow_identity_scopes as scope
  cross join params
  where scope.actor_user_id = params.actor_user_id
    and scope.environment = 'preview'
    and scope.project_ref = params.preview_ref
    and scope.operation_id = params.operation_id
    and scope.receipt_id in (select id from receipt_ids)
),
candidate_derivative_requests as (
  select request.*
  from util.dataset_derivative_rebuild_requests as request
  cross join params
  where request.id in (select id from bound_derivative_request_ids)
    or request.batch_id in (select id from bound_derivative_batch_ids)
    or exists (
      select 1 from process_targets as target
      where request.target_table = 'processes'
        and request.target_id = target.id
        and btrim(request.target_version) = target.version
    )
),
derivative_request_ids as (
  select id from bound_derivative_request_ids
  union
  select id from candidate_derivative_requests
),
derivative_batch_ids as (
  select id from bound_derivative_batch_ids
  union
  select batch_id
  from candidate_derivative_requests
  where batch_id is not null
),
http_request_ids as (
  select id from bound_http_request_ids
  union
  select markdown_request_id
  from candidate_derivative_requests
  where markdown_request_id is not null
),
counts(ordinal, name, residue_count, description) as (
  values
    (1, 'auth_users', (
      select count(*) from auth.users as actor cross join params
      where actor.id in (params.actor_user_id, params.foreign_user_id)
    ), 'disposable Auth users'),
    (2, 'auth_identities', (
      select count(*) from auth.identities as auth_identity cross join params
      where auth_identity.user_id in (
        params.actor_user_id, params.foreign_user_id
      )
    ), 'disposable Auth identities'),
    (3, 'auth_sessions', (
      select count(*) from auth.sessions as auth_session cross join params
      where auth_session.user_id in (
        params.actor_user_id, params.foreign_user_id
      )
    ), 'disposable Auth sessions'),
    (4, 'auth_refresh_tokens', (
      select count(*) from auth.refresh_tokens as token cross join params
      where token.user_id::text in (
        params.actor_user_id::text, params.foreign_user_id::text
      )
    ), 'disposable Auth refresh tokens'),
    (5, 'capture_receipts', (
      select count(*) from util.dataset_flow_identity_capture_receipts
      where id in (select id from receipt_ids)
    ), 'capture receipts'),
    (6, 'capture_source_guards', (
      select count(*) from util.dataset_flow_identity_capture_source_guards
      where receipt_id in (select id from receipt_ids)
    ), 'capture source guards'),
    (7, 'capture_target_guards', (
      select count(*) from util.dataset_flow_identity_capture_target_guards
      where receipt_id in (select id from receipt_ids)
    ), 'capture target guards'),
    (8, 'capture_support_guards', (
      select count(*) from util.dataset_flow_identity_capture_support_guards
      where receipt_id in (select id from receipt_ids)
    ), 'capture support guards'),
    (9, 'capture_mapping_guards', (
      select count(*) from util.dataset_flow_identity_capture_mapping_guards
      where receipt_id in (select id from receipt_ids)
    ), 'capture mapping guards'),
    (10, 'capture_process_intents', (
      select count(*) from util.dataset_flow_identity_capture_process_intents
      where receipt_id in (select id from receipt_ids)
    ), 'capture process intents'),
    (11, 'flow_identity_scopes', (
      select count(*) from util.dataset_flow_identity_scopes
      where id in (select id from scope_ids)
    ), 'flow-identity scopes'),
    (12, 'flow_identity_mappings', (
      select count(*) from util.dataset_flow_identity_mappings
      where scope_id in (select id from scope_ids)
    ), 'flow-identity mappings'),
    (13, 'flow_identity_process_ledger', (
      select count(*) from util.dataset_flow_identity_process_ledger
      where scope_id in (select id from scope_ids)
    ), 'flow-identity process ledger rows'),
    (14, 'flow_identity_mutation_permits', (
      select count(*) from util.dataset_flow_identity_mutation_permits
      where scope_id in (select id from scope_ids)
    ), 'flow-identity mutation permits'),
    (15, 'flow_identity_wrapper_invocations', (
      select count(*)
      from util.dataset_flow_identity_wrapper_invocations as invocation
      cross join params
      where invocation.scope_id in (select id from scope_ids)
        or invocation.actor_user_id = params.actor_user_id
    ), 'flow-identity wrapper invocations'),
    (16, 'derivative_requests', (
      select count(*) from util.dataset_derivative_rebuild_requests
      where id in (select id from derivative_request_ids)
    ), 'derivative rebuild requests'),
    (17, 'derivative_proposals', (
      select count(*) from util.dataset_derivative_rebuild_proposals
      where request_id in (select id from derivative_request_ids)
    ), 'derivative rebuild proposals'),
    (18, 'derivative_permits', (
      select count(*) from util.dataset_derivative_rebuild_permits
      where request_id in (select id from derivative_request_ids)
    ), 'derivative rebuild permits'),
    (19, 'dataset_extraction_queue', (
      select count(*) from pgmq.q_dataset_extraction_jobs as queued
      where queued.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = 'processes'
          and queued.message->>'id' = target.id::text
          and btrim(queued.message->>'version') = target.version
      )
    ), 'dataset extraction PGMQ queue rows'),
    (20, 'dataset_extraction_archive', (
      select count(*) from pgmq.a_dataset_extraction_jobs as archived
      where archived.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where archived.message->>'schema' = 'public'
          and archived.message->>'table' = 'processes'
          and archived.message->>'id' = target.id::text
          and btrim(archived.message->>'version') = target.version
      )
    ), 'dataset extraction PGMQ archive rows'),
    (21, 'dataset_extraction_failures', (
      select count(*) from util.dataset_extraction_job_failures as failure
      where failure.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where failure.message->>'schema' = 'public'
          and failure.message->>'table' = 'processes'
          and failure.message->>'id' = target.id::text
          and btrim(failure.message->>'version') = target.version
      )
    ), 'dataset extraction failure rows'),
    (22, 'embedding_queue', (
      select count(*) from pgmq.q_embedding_jobs as queued
      where queued.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = 'processes'
          and queued.message->>'id' = target.id::text
          and btrim(queued.message->>'version') = target.version
      )
    ), 'embedding PGMQ queue rows'),
    (23, 'embedding_archive', (
      select count(*) from pgmq.a_embedding_jobs as archived
      where archived.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where archived.message->>'schema' = 'public'
          and archived.message->>'table' = 'processes'
          and archived.message->>'id' = target.id::text
          and btrim(archived.message->>'version') = target.version
      )
    ), 'embedding PGMQ archive rows'),
    (24, 'pending_embedding_jobs', (
      select count(*) from util.pending_embedding_jobs as pending
      where pending.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where pending.schema_name = 'public'
          and pending.table_name = 'processes'
          and pending.record_id = target.id::text
          and btrim(pending.record_version) = target.version
      )
    ), 'pending embedding rows'),
    (25, 'embedding_failures', (
      select count(*) from util.embedding_job_failures as failure
      where failure.message->>'requestId' in (
        select id::text from derivative_request_ids
      ) or exists (
        select 1 from process_targets as target
        where failure.message->>'table' = 'processes'
          and failure.message->>'id' = target.id::text
          and btrim(failure.message->>'version') = target.version
      )
    ), 'embedding failure rows'),
    (26, 'pg_net_request_queue', (
      select count(*) from net.http_request_queue as queued
      where queued.id in (select id from http_request_ids)
        or exists (
          select 1 from process_targets as target
          where util.dataset_derivative_rebuild_http_body_matches(
            queued.body, 'processes', target.id, target.version
          )
        )
    ), 'pg_net request queue rows'),
    (27, 'pg_net_responses', (
      select count(*) from net._http_response
      where id in (select id from http_request_ids)
    ), 'pg_net response rows'),
    (28, 'fixture_manifest_audits', (
      select count(*)
      from public.command_audit_log as audit
      cross join params
      where audit.command = 'preview_e2e_flow_identity_fixture'
        and audit.target_table = 'preview_e2e_flow_identity'
        and audit.target_id = params.capture_request_id
        and audit.target_version = '00.00.001'
        and audit.payload->>'scenario_id' is not distinct from
          params.scenario_id
    ), 'fixture manifest audit rows'),
    (29, 'scenario_command_audits', (
      select count(*)
      from public.command_audit_log as audit
      cross join params
      where not (
        audit.command is not distinct from
          'preview_e2e_flow_identity_fixture'
        and audit.target_table is not distinct from
          'preview_e2e_flow_identity'
        and audit.target_id is not distinct from params.capture_request_id
        and audit.target_version is not distinct from '00.00.001'
        and audit.payload->>'scenario_id' is not distinct from
          params.scenario_id
      ) and (
        audit.actor_user_id in (
          params.actor_user_id, params.foreign_user_id
        )
        or audit.target_id in (select id from process_targets)
        or audit.payload->>'operation_id' is not distinct from
          params.operation_id
        or audit.payload->>'scope_id' in (
          select id::text from scope_ids
        )
        or audit.payload->>'request_id' in (
          select id::text from derivative_request_ids
        )
        or audit.payload->>'derivative_batch_id' in (
          select id::text from derivative_batch_ids
        )
      )
    ), 'scenario command audit rows'),
    (30, 'processes', (
      select count(*) from public.processes
      where id in (select id from process_targets)
    ), 'fixture process rows'),
    (31, 'flows', (
      select count(*) from public.flows
      where id in (select id from flow_targets)
    ), 'fixture flow rows'),
    (32, 'flowproperties', (
      select count(*) from public.flowproperties
      where id = (select id from derived_ids where name = 'flowproperty')
    ), 'fixture flow-property rows'),
    (33, 'unitgroups', (
      select count(*) from public.unitgroups
      where id = (select id from derived_ids where name = 'unitgroup')
    ), 'fixture unit-group rows'),
    (34, 'vault_project_url', (
      select count(*)
      from vault.secrets as secret
      where secret.name = 'project_url'
    ), 'global exact-name project_url Vault rows'),
    (35, 'vault_project_secret_key', (
      select count(*)
      from vault.secrets as secret
      where secret.name = 'project_secret_key'
    ), 'global exact-name project_secret_key Vault rows'),
    (36, 'fault_trigger', (
      select count(*) from pg_trigger
      where tgname = 'preview_flow_identity_post_primary_fault_v1'
        and tgrelid = 'public.command_audit_log'::regclass
        and not tgisinternal
    ), 'fixture fault triggers'),
    (37, 'fault_function', (
      select count(*)
      from pg_proc as procedure
      join pg_namespace as namespace on namespace.oid = procedure.pronamespace
      where namespace.nspname = 'private'
        and procedure.proname = 'preview_flow_identity_post_primary_fault_v1'
        and procedure.pronargs = 0
    ), 'fixture fault functions'),
    (38, 'active_fixture_sessions', (
      select count(*)
      from pg_stat_activity as activity
      cross join params
      where activity.pid <> pg_backend_pid()
        and activity.backend_type = 'client backend'
        and activity.state is distinct from 'idle'
        and (
          activity.application_name = params.fixture_application_name
          or activity.pid in (select pid from bound_backend_pids)
        )
    ), 'active fixture database sessions'),
    (39, 'retained_fixture_sessions', (
      select count(*)
      from pg_stat_activity as activity
      cross join params
      where activity.pid <> pg_backend_pid()
        and activity.backend_type = 'client backend'
        and (
          activity.application_name = params.fixture_application_name
          or activity.pid in (select pid from bound_backend_pids)
        )
    ), 'active or idle fixture database sessions')
),
tap_rows(sequence, tap) as (
  select 0, 'TAP version 13'
  union all
  select 1, '1..' || count(*)::text from counts
  union all
  select ordinal + 1,
    case when residue_count = 0 then 'ok ' else 'not ok ' end
      || ordinal::text || ' - ' || description
      || ' [count=' || residue_count::text || ']'
  from counts
  union all
  select 1000, '# residue_counts=' || jsonb_object_agg(
    name, residue_count order by ordinal
  )::text
  from counts
)
select tap
from tap_rows
order by sequence;

rollback;
