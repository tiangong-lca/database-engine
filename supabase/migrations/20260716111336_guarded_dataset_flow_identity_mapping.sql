-- Guarded Step 3 elementary-flow identity rewrites.
--
-- The public surface deliberately separates a durable, read-only scope seal
-- from one-process transactions.  A process transaction can change only the
-- five TIDAS flow-reference identity fields and admits the matching protected
-- derivative rebuild before it commits.  Public/source flow rows are never
-- update targets of this capability.

create table util.dataset_flow_identity_capture_receipts (
  id uuid primary key default gen_random_uuid(),
  actor_user_id uuid not null,
  actor_email text not null,
  request_id uuid not null,
  environment text not null,
  project_ref text not null,
  target_visibility text not null,
  operation_id text not null,
  compatibility_policy jsonb not null,
  policy_approval_text_sha256 text not null,
  artifact_evidence jsonb not null,
  protected_closure jsonb not null,
  protected_closure_sha256 text not null,
  source_universe jsonb not null,
  source_universe_sha256 text not null,
  support_snapshot_set_sha256 text not null,
  source_guard_set_sha256 text not null,
  target_guard_set_sha256 text not null,
  mapping_guard_set_sha256 text not null,
  process_intent_set_sha256 text not null,
  mapping_set_sha256 text not null,
  process_manifest_sha256 text not null,
  capture_request_sha256 text not null,
  receipt_proof_sha256 text not null,
  whole_scope_proof_sha256 text not null,
  source_count integer not null,
  target_count integer not null,
  support_count integer not null,
  mapping_count integer not null,
  process_count integer not null,
  rewrite_count integer not null,
  captured_at timestamp with time zone not null,
  expires_at timestamp with time zone not null,
  created_at timestamp with time zone not null default clock_timestamp(),
  constraint dataset_flow_identity_capture_visibility_chk
    check (target_visibility = 'owner_draft'),
  constraint dataset_flow_identity_capture_environment_chk
    check (environment in ('local', 'preview', 'production')),
  constraint dataset_flow_identity_capture_counts_chk check (
    source_count = 305 and target_count > 0 and support_count >= 2
    and mapping_count > 0 and process_count > 0 and rewrite_count > 0
  ),
  constraint dataset_flow_identity_capture_lifetime_chk
    check (expires_at > captured_at and expires_at <= captured_at + interval '7 days'),
  constraint dataset_flow_identity_capture_hashes_chk check (
    policy_approval_text_sha256 ~ '^[a-f0-9]{64}$'
    and protected_closure_sha256 ~ '^[a-f0-9]{64}$'
    and source_universe_sha256 ~ '^[a-f0-9]{64}$'
    and support_snapshot_set_sha256 ~ '^[a-f0-9]{64}$'
    and source_guard_set_sha256 ~ '^[a-f0-9]{64}$'
    and target_guard_set_sha256 ~ '^[a-f0-9]{64}$'
    and mapping_guard_set_sha256 ~ '^[a-f0-9]{64}$'
    and process_intent_set_sha256 ~ '^[a-f0-9]{64}$'
    and mapping_set_sha256 ~ '^[a-f0-9]{64}$'
    and process_manifest_sha256 ~ '^[a-f0-9]{64}$'
    and capture_request_sha256 ~ '^[a-f0-9]{64}$'
    and receipt_proof_sha256 ~ '^[a-f0-9]{64}$'
    and whole_scope_proof_sha256 ~ '^[a-f0-9]{64}$'
  ),
  unique (actor_user_id, request_id),
  unique (actor_user_id, receipt_proof_sha256)
);

create table util.dataset_flow_identity_capture_source_guards (
  receipt_id uuid not null references
    util.dataset_flow_identity_capture_receipts(id) on delete restrict,
  ordinal integer not null,
  disposition text not null,
  source_id uuid not null,
  source_version text not null,
  guard jsonb not null,
  evidence_sha256 text not null,
  primary key (receipt_id, ordinal),
  unique (receipt_id, source_id, source_version),
  constraint dataset_flow_identity_capture_source_ordinal_chk check (ordinal > 0),
  constraint dataset_flow_identity_capture_source_disposition_chk
    check (disposition in ('mapped', 'pending', 'blocker', 'orphan')),
  constraint dataset_flow_identity_capture_source_evidence_chk
    check (evidence_sha256 ~ '^[a-f0-9]{64}$')
);

create table util.dataset_flow_identity_capture_target_guards (
  receipt_id uuid not null references
    util.dataset_flow_identity_capture_receipts(id) on delete restrict,
  ordinal integer not null,
  target_id uuid not null,
  target_version text not null,
  guard jsonb not null,
  primary key (receipt_id, ordinal),
  unique (receipt_id, target_id, target_version),
  constraint dataset_flow_identity_capture_target_ordinal_chk check (ordinal > 0)
);

create table util.dataset_flow_identity_capture_support_guards (
  receipt_id uuid not null references
    util.dataset_flow_identity_capture_receipts(id) on delete restrict,
  ordinal integer not null,
  support_table text not null,
  support_id uuid not null,
  support_version text not null,
  guard jsonb not null,
  primary key (receipt_id, ordinal),
  unique (receipt_id, support_table, support_id, support_version),
  constraint dataset_flow_identity_capture_support_ordinal_chk check (ordinal > 0),
  constraint dataset_flow_identity_capture_support_table_chk
    check (support_table in ('flowproperties', 'unitgroups'))
);

create table util.dataset_flow_identity_capture_mapping_guards (
  receipt_id uuid not null references
    util.dataset_flow_identity_capture_receipts(id) on delete restrict,
  ordinal integer not null,
  mapping_id text not null,
  source_id uuid not null,
  source_version text not null,
  target_id uuid not null,
  target_version text not null,
  mapping jsonb not null,
  primary key (receipt_id, ordinal),
  unique (receipt_id, mapping_id),
  unique (receipt_id, source_id, source_version),
  constraint dataset_flow_identity_capture_mapping_ordinal_chk check (ordinal > 0),
  constraint dataset_flow_identity_capture_mapping_hash_chk
    check (mapping_id ~ '^[a-f0-9]{64}$')
);

create table util.dataset_flow_identity_capture_process_intents (
  receipt_id uuid not null references
    util.dataset_flow_identity_capture_receipts(id) on delete restrict,
  ordinal integer not null,
  process_id uuid not null,
  process_version text not null,
  intent_proof_sha256 text not null,
  manifest jsonb not null,
  primary key (receipt_id, ordinal),
  unique (receipt_id, process_id, process_version),
  constraint dataset_flow_identity_capture_process_ordinal_chk check (ordinal > 0),
  constraint dataset_flow_identity_capture_process_proof_chk
    check (intent_proof_sha256 ~ '^[a-f0-9]{64}$')
);

create table util.dataset_flow_identity_scopes (
  id uuid primary key default gen_random_uuid(),
  receipt_id uuid not null default nullif(
    current_setting('app.dataset_flow_identity_receipt_id', true), ''
  )::uuid references
    util.dataset_flow_identity_capture_receipts(id) on delete restrict,
  receipt_proof_sha256 text not null default nullif(
    current_setting('app.dataset_flow_identity_receipt_proof_sha256', true), ''
  ),
  actor_user_id uuid not null,
  actor_email text not null,
  request_id uuid not null,
  environment text not null,
  project_ref text not null,
  target_visibility text not null,
  user_state_claim text not null default
    'authenticated_actor_state_100_plus_own_state_0',
  operation_id text not null,
  plan_sha256 text not null,
  freeze_sha256 text not null,
  approval_identity_sha256 text not null,
  approval_text_sha256 text not null,
  policy_approval_text_sha256 text not null default nullif(
    current_setting('app.dataset_flow_identity_policy_approval_sha256', true), ''
  ),
  execution_approval_request_sha256 text not null default nullif(
    current_setting('app.dataset_flow_identity_execution_request_sha256', true), ''
  ),
  toolchain_evidence_sha256 text not null,
  compatibility_policy jsonb not null,
  support_snapshot_set_sha256 text not null,
  support_snapshots jsonb not null,
  source_universe_sha256 text not null,
  source_universe jsonb not null,
  source_universe_count integer not null,
  mapping_set_sha256 text not null,
  process_manifest_sha256 text not null,
  protected_closure_sha256 text not null,
  protected_closure jsonb not null,
  preflight_request_sha256 text not null default nullif(
    current_setting('app.dataset_flow_identity_preflight_request_sha256', true), ''
  ),
  scope_request_sha256 text not null,
  scope_proof_sha256 text not null,
  status text not null default 'sealed',
  mapping_count integer not null,
  process_count integer not null,
  rewrite_count integer not null,
  final_request_sha256 text,
  cancel_request_sha256 text,
  terminal_proof_sha256 text,
  final_wrapper_invocation_id uuid,
  final_permit_generation_before integer,
  last_error jsonb,
  sealed_at timestamp with time zone not null default clock_timestamp(),
  primary_completed_at timestamp with time zone,
  completed_at timestamp with time zone,
  updated_at timestamp with time zone not null default clock_timestamp(),
  constraint dataset_flow_identity_scope_visibility_chk
    check (target_visibility = 'owner_draft'),
  constraint dataset_flow_identity_scope_user_state_claim_chk check (
    user_state_claim = 'authenticated_actor_state_100_plus_own_state_0'
  ),
  constraint dataset_flow_identity_scope_environment_chk
    check (environment in ('local', 'preview', 'production')),
  constraint dataset_flow_identity_scope_status_chk
    check (status in (
      'sealed', 'running', 'primary_complete', 'derivatives_pending',
      'completed', 'failed', 'cancelled'
    )),
  constraint dataset_flow_identity_scope_counts_chk
    check (mapping_count > 0 and process_count > 0 and rewrite_count > 0),
  constraint dataset_flow_identity_scope_hashes_chk check (
    plan_sha256 ~ '^[a-f0-9]{64}$'
    and freeze_sha256 ~ '^[a-f0-9]{64}$'
    and approval_identity_sha256 ~ '^[a-f0-9]{64}$'
    and approval_text_sha256 ~ '^[a-f0-9]{64}$'
    and policy_approval_text_sha256 ~ '^[a-f0-9]{64}$'
    and execution_approval_request_sha256 ~ '^[a-f0-9]{64}$'
    and receipt_proof_sha256 ~ '^[a-f0-9]{64}$'
    and toolchain_evidence_sha256 ~ '^[a-f0-9]{64}$'
    and support_snapshot_set_sha256 ~ '^[a-f0-9]{64}$'
    and source_universe_sha256 ~ '^[a-f0-9]{64}$'
    and mapping_set_sha256 ~ '^[a-f0-9]{64}$'
    and process_manifest_sha256 ~ '^[a-f0-9]{64}$'
    and protected_closure_sha256 ~ '^[a-f0-9]{64}$'
    and preflight_request_sha256 ~ '^[a-f0-9]{64}$'
    and scope_request_sha256 ~ '^[a-f0-9]{64}$'
    and scope_proof_sha256 ~ '^[a-f0-9]{64}$'
    and (final_request_sha256 is null
      or final_request_sha256 ~ '^[a-f0-9]{64}$')
    and (cancel_request_sha256 is null
      or cancel_request_sha256 ~ '^[a-f0-9]{64}$')
    and (terminal_proof_sha256 is null
      or terminal_proof_sha256 ~ '^[a-f0-9]{64}$')
  ),
  constraint dataset_flow_identity_scope_approval_domains_chk check (
    policy_approval_text_sha256 <> execution_approval_request_sha256
    and policy_approval_text_sha256 <> approval_text_sha256
    and policy_approval_text_sha256 <> approval_identity_sha256
    and execution_approval_request_sha256 <> approval_text_sha256
    and execution_approval_request_sha256 <> approval_identity_sha256
    and approval_text_sha256 <> approval_identity_sha256
  ),
  constraint dataset_flow_identity_scope_universe_chk
    check (source_universe_count = 305),
  constraint dataset_flow_identity_scope_final_permit_generation_chk
    check (final_permit_generation_before is null
      or final_permit_generation_before >= 0),
  unique (actor_user_id, request_id),
  unique (actor_user_id, operation_id),
  unique (actor_user_id, plan_sha256)
);

create table util.dataset_flow_identity_mappings (
  scope_id uuid not null
    references util.dataset_flow_identity_scopes(id) on delete restrict,
  ordinal integer not null,
  mapping_id text not null,
  source_id uuid not null,
  source_version text not null,
  target_id uuid not null,
  target_version text not null,
  mapping jsonb not null,
  primary key (scope_id, ordinal),
  unique (scope_id, mapping_id),
  unique (scope_id, source_id, source_version),
  constraint dataset_flow_identity_mapping_ordinal_chk check (ordinal > 0),
  constraint dataset_flow_identity_mapping_hash_chk
    check (mapping_id ~ '^[a-f0-9]{64}$'),
  constraint dataset_flow_identity_mapping_versions_chk check (
    source_version ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    and target_version ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
  )
);

create table util.dataset_flow_identity_process_ledger (
  scope_id uuid not null
    references util.dataset_flow_identity_scopes(id) on delete restrict,
  ordinal integer not null,
  process_id uuid not null,
  process_version text not null,
  manifest jsonb not null,
  process_template_sha256 text not null,
  process_intent_proof_sha256 text not null,
  process_request_sha256 text,
  rewrite_count integer not null,
  status text not null default 'pending',
  active boolean not null default true,
  mutation_nonce uuid not null default gen_random_uuid(),
  audit_id bigint references public.command_audit_log(id),
  before_payload_sha256 text not null,
  after_payload_sha256 text,
  after_exchange_set_sha256 text,
  derivative_batch_id uuid,
  derivative_admission jsonb,
  wrapper_invocation_id uuid,
  permit_generation_before integer,
  completed_at timestamp with time zone,
  last_error jsonb,
  primary key (scope_id, ordinal),
  unique (scope_id, process_id, process_version),
  constraint dataset_flow_identity_process_ordinal_chk check (ordinal > 0),
  constraint dataset_flow_identity_process_version_chk
    check (process_version ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'),
  constraint dataset_flow_identity_process_hash_chk check (
    process_template_sha256 ~ '^[a-f0-9]{64}$'
    and process_intent_proof_sha256 ~ '^[a-f0-9]{64}$'
    and (process_request_sha256 is null
      or process_request_sha256 ~ '^[a-f0-9]{64}$')
    and before_payload_sha256 ~ '^[a-f0-9]{64}$'
    and (after_payload_sha256 is null
      or after_payload_sha256 ~ '^[a-f0-9]{64}$')
    and (after_exchange_set_sha256 is null
      or after_exchange_set_sha256 ~ '^[a-f0-9]{64}$')
  ),
  constraint dataset_flow_identity_process_rewrite_count_chk
    check (rewrite_count > 0),
  constraint dataset_flow_identity_process_status_chk
    check (status in ('pending', 'completed', 'failed')),
  constraint dataset_flow_identity_process_permit_generation_chk
    check (permit_generation_before is null or permit_generation_before >= 0)
);

-- A permit is minted only inside the private rewrite core immediately before
-- its one primary UPDATE.  The row is scoped to the current transaction and
-- consumed by the active-scope fence trigger.  Authenticated callers cannot
-- forge it with custom GUCs or reuse it for a second mutation.
create table util.dataset_flow_identity_mutation_permits (
  transaction_id bigint not null,
  scope_id uuid not null,
  ordinal integer not null,
  process_id uuid not null,
  process_version text not null,
  mutation_nonce uuid not null,
  before_payload_sha256 text not null,
  after_payload_sha256 text not null,
  created_at timestamp with time zone not null default clock_timestamp(),
  primary key (transaction_id, scope_id, ordinal),
  foreign key (scope_id, ordinal)
    references util.dataset_flow_identity_process_ledger(scope_id, ordinal)
    on delete restrict,
  constraint dataset_flow_identity_mutation_permit_hashes_chk check (
    before_payload_sha256 ~ '^[a-f0-9]{64}$'
    and after_payload_sha256 ~ '^[a-f0-9]{64}$'
  )
);

-- A human approval may start exactly one CLI execution wrapper.  The raw
-- bearer token is returned only by the fresh preflight/recovery call and is
-- never persisted; only its SHA-256 is stored.  Every successful process or
-- finalize transaction rotates the token atomically with its business result.
-- A replayed admission returns no token, so another process/outDir/machine
-- cannot turn the same approval into a second write-capable wrapper.
create table util.dataset_flow_identity_wrapper_invocations (
  id uuid primary key default gen_random_uuid(),
  scope_id uuid not null
    references util.dataset_flow_identity_scopes(id) on delete restrict,
  actor_user_id uuid not null,
  approval_kind text not null,
  approval_request_sha256 text not null,
  approval_text_sha256 text not null,
  approval_identity_sha256 text not null,
  admission_request_sha256 text not null,
  baseline_whole_scope_proof_sha256 text not null,
  generation integer not null default 0,
  token_sha256 text not null,
  maximum_process_posts integer not null,
  successful_process_posts integer not null default 0,
  maximum_finalize_posts integer not null default 1,
  successful_finalize_posts integer not null default 0,
  status text not null default 'active',
  admitted_at timestamp with time zone not null default clock_timestamp(),
  updated_at timestamp with time zone not null default clock_timestamp(),
  closed_at timestamp with time zone,
  constraint dataset_flow_identity_invocation_kind_chk
    check (approval_kind in ('initial', 'recovery')),
  constraint dataset_flow_identity_invocation_status_chk
    check (status in ('active', 'superseded', 'completed', 'cancelled')),
  constraint dataset_flow_identity_invocation_counts_chk check (
    generation >= 0
    and maximum_process_posts >= 0
    and successful_process_posts >= 0
    and successful_process_posts <= maximum_process_posts
    and maximum_finalize_posts = 1
    and successful_finalize_posts >= 0
    and successful_finalize_posts <= maximum_finalize_posts
  ),
  constraint dataset_flow_identity_invocation_hashes_chk check (
    approval_request_sha256 ~ '^[a-f0-9]{64}$'
    and approval_text_sha256 ~ '^[a-f0-9]{64}$'
    and approval_identity_sha256 ~ '^[a-f0-9]{64}$'
    and admission_request_sha256 ~ '^[a-f0-9]{64}$'
    and baseline_whole_scope_proof_sha256 ~ '^[a-f0-9]{64}$'
    and token_sha256 ~ '^[a-f0-9]{64}$'
  ),
  constraint dataset_flow_identity_invocation_approval_domains_chk check (
    approval_request_sha256 <> approval_text_sha256
    and approval_request_sha256 <> approval_identity_sha256
    and approval_text_sha256 <> approval_identity_sha256
  ),
  unique (actor_user_id, approval_request_sha256),
  unique (actor_user_id, approval_text_sha256),
  unique (actor_user_id, approval_identity_sha256)
);

alter table util.dataset_flow_identity_process_ledger
  add constraint dataset_flow_identity_process_invocation_fk
  foreign key (wrapper_invocation_id)
  references util.dataset_flow_identity_wrapper_invocations(id)
  on delete restrict;

alter table util.dataset_flow_identity_scopes
  add constraint dataset_flow_identity_scope_final_invocation_fk
  foreign key (final_wrapper_invocation_id)
  references util.dataset_flow_identity_wrapper_invocations(id)
  on delete restrict;

create unique index dataset_flow_identity_process_active_uidx
  on util.dataset_flow_identity_process_ledger (process_id, process_version)
  where active;

create unique index dataset_flow_identity_scope_actor_active_uidx
  on util.dataset_flow_identity_scopes (actor_user_id)
  where status in (
    'sealed', 'running', 'primary_complete', 'derivatives_pending'
  );

create unique index dataset_flow_identity_invocation_scope_active_uidx
  on util.dataset_flow_identity_wrapper_invocations (scope_id)
  where status = 'active';

create unique index dataset_flow_identity_process_invocation_generation_uidx
  on util.dataset_flow_identity_process_ledger (
    wrapper_invocation_id, permit_generation_before
  )
  where wrapper_invocation_id is not null;

create index dataset_flow_identity_process_scope_read_idx
  on util.dataset_flow_identity_process_ledger (scope_id, ordinal)
  include (
    process_id, process_version, status, rewrite_count, audit_id,
    derivative_batch_id, completed_at
  );

create index dataset_flow_identity_process_next_pending_idx
  on util.dataset_flow_identity_process_ledger (scope_id, status, ordinal);

create unique index command_audit_log_flow_identity_process_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'scope_id'),
    (payload ->> 'process_request_sha256')
  )
  where command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
    and target_table = 'processes';

create unique index command_audit_log_flow_identity_recovery_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'scope_id'),
    (payload ->> 'recovery_approval_identity_sha256')
  )
  where command = 'cmd_dataset_flow_identity_scope_recover_guarded'
    and target_table is null;

create index dataset_derivative_rebuild_flow_identity_compensation_idx
  on util.dataset_derivative_rebuild_requests (
    actor_user_id, target_table, target_id, target_version,
    admitted_at desc, id desc
  )
  include (
    batch_id, plan_sha256, operation_id, action_id, reason_code,
    expected_json_ordered_sha256, status, action_audit_id, summary_audit_id
  )
  where batch_id is null;

revoke all on util.dataset_flow_identity_scopes
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_capture_receipts
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_capture_source_guards
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_capture_target_guards
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_capture_support_guards
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_capture_mapping_guards
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_capture_process_intents
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_mutation_permits
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_mappings
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_process_ledger
  from public, anon, authenticated, service_role;
revoke all on util.dataset_flow_identity_wrapper_invocations
  from public, anon, authenticated, service_role;

comment on table util.dataset_flow_identity_scopes is
  'Private durable Step 3 scope seals. approval_identity_sha256 and approval_text_sha256 are execution-approval hashes; policy_approval_text_sha256 is independently bound from the capture receipt. Scope rows bind exact mapping, process, and protected pending/blocker closure evidence.';
comment on table util.dataset_flow_identity_process_ledger is
  'Private ordered one-process Step 3 replay ledger. Active rows exclude overlapping live scopes; completed rows bind one primary audit and one protected derivative batch.';
comment on table util.dataset_flow_identity_wrapper_invocations is
  'Private one-wrapper approval-consumption ledger. Fresh initial/recovery admission returns one memory-only bearer token; replay returns no token, successful writes rotate it, and superseded/terminal tokens can never write.';

create or replace function private.dataset_flow_identity_exact_keys(
  p_value jsonb,
  p_keys text[]
) returns boolean
language sql
immutable
strict
set search_path = ''
as $$
  select jsonb_typeof(p_value) = 'object'
    and p_value ?& p_keys
    and not exists (
      select 1
      from jsonb_object_keys(p_value) as actual(key)
      where actual.key <> all (p_keys)
    )
$$;

alter function private.dataset_flow_identity_exact_keys(jsonb, text[])
  owner to postgres;
revoke all on function private.dataset_flow_identity_exact_keys(jsonb, text[])
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_safe_json_v2(
  p_value jsonb
) returns jsonb
language plpgsql
stable
strict
parallel safe
set search_path = ''
as $$
declare
  v_kind text := jsonb_typeof(p_value);
  v_number numeric;
  v_result jsonb;
  v_children_valid boolean;
begin
  case v_kind
    when 'null' then return 'null'::jsonb;
    when 'string' then return p_value;
    when 'boolean' then return p_value;
    when 'number' then
      begin
        v_number := (p_value #>> '{}')::numeric;
      exception when others then
        return null;
      end;
      if v_number <> trunc(v_number)
        or v_number < -9007199254740991::numeric
        or v_number > 9007199254740991::numeric then
        return null;
      end if;
      return to_jsonb(v_number::bigint);
    when 'array' then
      with children as materialized (
        select item.ordinality,
          private.dataset_flow_identity_safe_json_v2(item.value) as value
        from jsonb_array_elements(p_value)
          with ordinality as item(value, ordinality)
      )
      select coalesce(bool_and(children.value is not null), true),
        coalesce(jsonb_agg(children.value order by children.ordinality),
          '[]'::jsonb)
      into v_children_valid, v_result
      from children;
      if not v_children_valid then return null; end if;
      return v_result;
    when 'object' then
      with children as materialized (
        select item.key,
          private.dataset_alias_js_object_key_sort_key_v1(item.key)
            as sort_key,
          private.dataset_flow_identity_safe_json_v2(item.value) as value
        from jsonb_each(p_value) as item(key, value)
      )
      select coalesce(bool_and(children.value is not null), true),
        coalesce(jsonb_object_agg(
          children.key, children.value order by children.sort_key
        ), '{}'::jsonb)
      into v_children_valid, v_result
      from children;
      if not v_children_valid then return null; end if;
      return v_result;
    else
      return null;
  end case;
end;
$$;

alter function private.dataset_flow_identity_safe_json_v2(jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_safe_json_v2(jsonb)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_restricted_sha256_v2(
  p_value jsonb
) returns text
language plpgsql
stable
strict
set search_path = ''
as $$
declare
  v_normalized jsonb;
begin
  v_normalized := private.dataset_flow_identity_safe_json_v2(p_value);
  if v_normalized is null then return null; end if;
  return encode(
    extensions.digest(
      convert_to(private.dataset_alias_canonical_jsonb_v1(v_normalized), 'UTF8'),
      'sha256'
    ),
    'hex'
  );
end;
$$;

alter function util.dataset_flow_identity_restricted_sha256_v2(jsonb)
  owner to postgres;
revoke all on function util.dataset_flow_identity_restricted_sha256_v2(jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_short_description_v2(
  p_value jsonb
) returns boolean
language sql
immutable
strict
parallel safe
set search_path = ''
as $$
  select case jsonb_typeof(p_value)
    when 'string' then true
    when 'object' then
      private.dataset_flow_identity_exact_keys(
        p_value, array['@xml:lang', '#text']
      )
      and jsonb_typeof(p_value->'@xml:lang') = 'string'
      and jsonb_typeof(p_value->'#text') = 'string'
    when 'array' then
      jsonb_array_length(p_value) >= 1
      and not exists (
        select 1
        from jsonb_array_elements(p_value) as item(value)
        where jsonb_typeof(item.value) <> 'object'
          or not private.dataset_flow_identity_exact_keys(
            item.value, array['@xml:lang', '#text']
          )
          or jsonb_typeof(item.value->'@xml:lang') <> 'string'
          or jsonb_typeof(item.value->'#text') <> 'string'
      )
    else false
  end
$$;

alter function private.dataset_flow_identity_short_description_v2(jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_short_description_v2(jsonb)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_sha256(
  p_value jsonb
) returns text
language sql
stable
strict
set search_path = ''
as $$
  select encode(
    extensions.digest(
      convert_to(private.dataset_alias_canonical_jsonb_v1(p_value), 'UTF8'),
      'sha256'
    ),
    'hex'
  )
$$;

alter function util.dataset_flow_identity_sha256(jsonb) owner to postgres;
revoke all on function util.dataset_flow_identity_sha256(jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_permit_token_sha256_v1(
  p_token text
) returns text
language sql
immutable
strict
set search_path = ''
as $$
  select pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(p_token, 'UTF8'), 'sha256'),
    'hex'
  )
$$;

alter function private.dataset_flow_identity_permit_token_sha256_v1(text)
  owner to postgres;
revoke all on function private.dataset_flow_identity_permit_token_sha256_v1(text)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_validate_wrapper_permit_v1(
  p_actor uuid,
  p_scope_id uuid,
  p_authorization jsonb,
  p_post_kind text
) returns uuid
language plpgsql
stable
set search_path = ''
as $$
declare
  v_invocation_id uuid;
  v_generation numeric;
begin
  if p_actor is null or p_scope_id is null
    or p_post_kind not in ('process', 'finalize')
    or not coalesce(private.dataset_flow_identity_exact_keys(
      p_authorization,
      array['schema_version', 'invocation_id', 'generation', 'token']
    ), false)
    or p_authorization->>'schema_version'
      <> 'dataset-flow-identity-execution-permit.v1'
    or p_authorization->>'invocation_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or jsonb_typeof(p_authorization->'generation') <> 'number'
    or p_authorization->>'token' !~ '^[a-f0-9]{64}$' then
    return null;
  end if;
  v_generation := (p_authorization->>'generation')::numeric;
  if v_generation < 0 or v_generation > 2147483647
    or v_generation <> trunc(v_generation) then
    return null;
  end if;
  select invocation.id into v_invocation_id
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = (p_authorization->>'invocation_id')::uuid
    and invocation.scope_id = p_scope_id
    and invocation.actor_user_id = p_actor
    and invocation.status = 'active'
    and invocation.generation = v_generation::integer
    and invocation.token_sha256 =
      private.dataset_flow_identity_permit_token_sha256_v1(
        p_authorization->>'token'
      )
    and case p_post_kind
      when 'process' then invocation.successful_process_posts
        < invocation.maximum_process_posts
      when 'finalize' then invocation.successful_finalize_posts
        < invocation.maximum_finalize_posts
      else false
    end;
  return v_invocation_id;
exception when invalid_text_representation or numeric_value_out_of_range then
  return null;
end;
$$;

alter function private.dataset_flow_identity_validate_wrapper_permit_v1(
  uuid, uuid, jsonb, text
) owner to postgres;
revoke all on function private.dataset_flow_identity_validate_wrapper_permit_v1(
  uuid, uuid, jsonb, text
) from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_rotate_wrapper_permit_v1(
  p_invocation_id uuid,
  p_post_kind text,
  p_terminal boolean default false
) returns jsonb
language plpgsql
volatile
set search_path = ''
as $$
declare
  v_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_token text;
begin
  select invocation.* into v_invocation
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = p_invocation_id and invocation.status = 'active'
  for update;
  if v_invocation.id is null
    or p_post_kind not in ('process', 'finalize') then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_WRAPPER_PERMIT_ROTATE_INVALID';
  end if;
  v_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
  update util.dataset_flow_identity_wrapper_invocations
  set generation = generation + 1,
    token_sha256 =
      private.dataset_flow_identity_permit_token_sha256_v1(v_token),
    successful_process_posts = successful_process_posts
      + case when p_post_kind = 'process' then 1 else 0 end,
    successful_finalize_posts = successful_finalize_posts
      + case when p_post_kind = 'finalize' then 1 else 0 end,
    status = case when p_terminal then 'completed' else status end,
    updated_at = clock_timestamp(),
    closed_at = case when p_terminal then clock_timestamp() else closed_at end
  where id = v_invocation.id
  returning * into v_invocation;
  if p_terminal then
    return null;
  end if;
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-execution-permit.v1',
    'invocation_id', v_invocation.id,
    'generation', v_invocation.generation,
    'token', v_token
  );
end;
$$;

alter function private.dataset_flow_identity_rotate_wrapper_permit_v1(
  uuid, text, boolean
) owner to postgres;
revoke all on function private.dataset_flow_identity_rotate_wrapper_permit_v1(
  uuid, text, boolean
) from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_invalidate_wrapper_permit_v1(
  p_invocation_id uuid
) returns void
language plpgsql
volatile
set search_path = ''
as $$
declare
  v_token text := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
begin
  update util.dataset_flow_identity_wrapper_invocations
  set generation = generation + 1,
    token_sha256 =
      private.dataset_flow_identity_permit_token_sha256_v1(v_token),
    status = 'superseded',
    updated_at = clock_timestamp(),
    closed_at = clock_timestamp()
  where id = p_invocation_id and status = 'active';
  if not found then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_WRAPPER_PERMIT_INVALIDATE_FAILED';
  end if;
end;
$$;

alter function private.dataset_flow_identity_invalidate_wrapper_permit_v1(uuid)
  owner to postgres;
revoke all on function private.dataset_flow_identity_invalidate_wrapper_permit_v1(uuid)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_exchanges(
  p_payload jsonb
) returns jsonb
language plpgsql
stable
strict
set search_path = ''
as $$
declare
  v_exchange jsonb := p_payload #> '{processDataSet,exchanges,exchange}';
begin
  if jsonb_typeof(v_exchange) = 'array' then
    return v_exchange;
  elsif jsonb_typeof(v_exchange) = 'object' then
    return jsonb_build_array(v_exchange);
  end if;
  return null;
end;
$$;

alter function private.dataset_flow_identity_exchanges(jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_exchanges(jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_replace_exchanges(
  p_payload jsonb,
  p_exchanges jsonb
) returns jsonb
language plpgsql
immutable
strict
set search_path = ''
as $$
declare
  v_original jsonb := p_payload #> '{processDataSet,exchanges,exchange}';
begin
  if jsonb_typeof(v_original) = 'array' then
    return jsonb_set(
      p_payload,
      '{processDataSet,exchanges,exchange}',
      p_exchanges,
      false
    );
  elsif jsonb_typeof(v_original) = 'object'
    and jsonb_array_length(p_exchanges) = 1 then
    return jsonb_set(
      p_payload,
      '{processDataSet,exchanges,exchange}',
      p_exchanges->0,
      false
    );
  end if;
  return null;
end;
$$;

alter function private.dataset_flow_identity_replace_exchanges(jsonb, jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_replace_exchanges(jsonb, jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_reference(
  p_exchange jsonb
) returns jsonb
language sql
immutable
strict
set search_path = ''
as $$
  select jsonb_build_object(
    '@refObjectId', p_exchange #> '{referenceToFlowDataSet,@refObjectId}',
    '@type', p_exchange #> '{referenceToFlowDataSet,@type}',
    '@uri', p_exchange #> '{referenceToFlowDataSet,@uri}',
    '@version', p_exchange #> '{referenceToFlowDataSet,@version}',
    'common:shortDescription',
      p_exchange #> '{referenceToFlowDataSet,common:shortDescription}'
  )
$$;

alter function private.dataset_flow_identity_reference(jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_reference(jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_text_values(
  p_value jsonb
) returns text[]
language sql
immutable
set search_path = ''
as $$
  select coalesce(array_agg(distinct candidate.value order by candidate.value),
    array[]::text[])
  from (
    select case jsonb_typeof(p_value)
      when 'string' then p_value #>> '{}'
      when 'object' then p_value->>'#text'
      else null
    end as value
    union all
    select case jsonb_typeof(item.value)
      when 'string' then item.value #>> '{}'
      when 'object' then item.value->>'#text'
      else null
    end
    from jsonb_array_elements(
      case when jsonb_typeof(p_value) = 'array'
        then p_value else '[]'::jsonb end
    ) as item(value)
  ) as candidate
  where nullif(btrim(candidate.value), '') is not null
$$;

alter function private.dataset_flow_identity_text_values(jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_text_values(jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_row_sha256(
  p_id uuid,
  p_version text,
  p_user_id uuid,
  p_state_code integer,
  p_modified_at timestamp with time zone,
  p_payload_sha256 text
) returns text
language sql
stable
strict
set search_path = ''
as $$
  select util.dataset_flow_identity_sha256(jsonb_build_object(
    'id', p_id,
    'version', p_version,
    'user_id', p_user_id,
    'state_code', p_state_code,
    'modified_at', p_modified_at,
    'payload_sha256', p_payload_sha256
  ))
$$;

alter function private.dataset_flow_identity_row_sha256(
  uuid, text, uuid, integer, timestamp with time zone, text
) owner to postgres;
revoke all on function private.dataset_flow_identity_row_sha256(
  uuid, text, uuid, integer, timestamp with time zone, text
) from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_validate_support_snapshot(
  p_actor uuid,
  p_snapshot jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_keys constant text[] := array[
    'ordinal', 'table', 'id', 'version', 'user_id', 'state_code',
    'modified_at', 'payload_sha256', 'row_sha256'
  ];
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_payload jsonb;
  v_payload_sha256 text;
  v_row_sha256 text;
  v_live_user_id uuid;
  v_live_state_code integer;
  v_live_modified_at timestamp with time zone;
  v_embedded_id text;
  v_embedded_version text;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(p_snapshot, v_keys)
    or p_snapshot->>'ordinal' !~ '^[1-9][0-9]*$'
    or p_snapshot->>'table' not in ('flowproperties', 'unitgroups')
    or p_snapshot->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_snapshot->>'user_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_snapshot->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_snapshot->>'state_code' not in ('0', '100')
    or p_snapshot->>'payload_sha256' !~ '^[a-f0-9]{64}$'
    or p_snapshot->>'row_sha256' !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_SCHEMA_MISMATCH'
    );
  end if;

  begin
    perform (p_snapshot->>'modified_at')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_VALUE_MISMATCH'
    );
  end;

  if p_snapshot->>'table' = 'flowproperties' then
    select support.*
    into v_flowproperty
    from public.flowproperties as support
    where support.id = (p_snapshot->>'id')::uuid
      and btrim(support.version::text) = p_snapshot->>'version';
    if v_flowproperty.id is null
      or v_flowproperty.json is null or v_flowproperty.json_ordered is null
      or v_flowproperty.json::jsonb
        is distinct from v_flowproperty.json_ordered::jsonb then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_JSON_PARITY_MISMATCH'
      );
    end if;
    v_payload := v_flowproperty.json_ordered::jsonb;
    v_live_user_id := v_flowproperty.user_id;
    v_live_state_code := v_flowproperty.state_code;
    v_live_modified_at := v_flowproperty.modified_at;
    v_embedded_id := v_payload #>>
      '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:UUID}';
    v_embedded_version := v_payload #>>
      '{flowPropertyDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}';
  else
    select support.*
    into v_unitgroup
    from public.unitgroups as support
    where support.id = (p_snapshot->>'id')::uuid
      and btrim(support.version::text) = p_snapshot->>'version';
    if v_unitgroup.id is null
      or v_unitgroup.json is null or v_unitgroup.json_ordered is null
      or v_unitgroup.json::jsonb
        is distinct from v_unitgroup.json_ordered::jsonb then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_JSON_PARITY_MISMATCH'
      );
    end if;
    v_payload := v_unitgroup.json_ordered::jsonb;
    v_live_user_id := v_unitgroup.user_id;
    v_live_state_code := v_unitgroup.state_code;
    v_live_modified_at := v_unitgroup.modified_at;
    v_embedded_id := v_payload #>>
      '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:UUID}';
    v_embedded_version := v_payload #>>
      '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}';
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(v_payload);
  v_row_sha256 := private.dataset_flow_identity_row_sha256(
    (p_snapshot->>'id')::uuid,
    p_snapshot->>'version',
    v_live_user_id,
    v_live_state_code,
    v_live_modified_at,
    v_payload_sha256
  );
  if v_live_user_id::text is distinct from p_snapshot->>'user_id'
    or v_live_state_code::text is distinct from p_snapshot->>'state_code'
    or v_live_modified_at is distinct from
      (p_snapshot->>'modified_at')::timestamp with time zone
    or (v_live_state_code = 0 and v_live_user_id is distinct from p_actor)
    or v_live_state_code not in (0, 100)
    or v_embedded_id is distinct from p_snapshot->>'id'
    or v_embedded_version is distinct from p_snapshot->>'version'
    or v_payload_sha256 is distinct from p_snapshot->>'payload_sha256'
    or v_row_sha256 is distinct from p_snapshot->>'row_sha256' then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_LIVE_MISMATCH'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'table', p_snapshot->>'table',
    'id', p_snapshot->>'id',
    'version', p_snapshot->>'version',
    'payload_sha256', v_payload_sha256,
    'row_sha256', v_row_sha256
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_validate_support_snapshot(uuid, jsonb)
  owner to postgres;
revoke all on function util.dataset_flow_identity_validate_support_snapshot(
  uuid, jsonb
) from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_validate_support_set(
  p_actor uuid,
  p_snapshots jsonb,
  p_expected_sha256 text
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_snapshot jsonb;
  v_validation jsonb;
  v_count integer;
begin
  if p_actor is null
    or jsonb_typeof(p_snapshots) <> 'array'
    or jsonb_array_length(p_snapshots) not between 2 and 100
    or p_expected_sha256 !~ '^[a-f0-9]{64}$'
    or util.dataset_flow_identity_sha256(p_snapshots)
      is distinct from p_expected_sha256 then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_SCHEMA_MISMATCH'
    );
  end if;
  v_count := jsonb_array_length(p_snapshots);
  if (
    select count(distinct (
      (item.value->>'table') || ':' || (item.value->>'id') || '@'
        || (item.value->>'version')
    )) = v_count
      and min((item.value->>'ordinal')::integer) = 1
      and max((item.value->>'ordinal')::integer) = v_count
      and bool_and(
        (item.value->>'ordinal')::integer = item.ordinality::integer
      )
    from jsonb_array_elements(p_snapshots)
      with ordinality as item(value, ordinality)
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_IDENTITY_MISMATCH'
    );
  end if;
  for v_snapshot in
    select item.value
    from jsonb_array_elements(p_snapshots)
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_validation := util.dataset_flow_identity_validate_support_snapshot(
      p_actor, v_snapshot
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_LIVE_MISMATCH',
        'details', v_validation
      );
    end if;
  end loop;
  return jsonb_build_object(
    'ok', true, 'support_count', v_count,
    'support_snapshot_set_sha256', p_expected_sha256
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_validate_support_set(
  uuid, jsonb, text
) owner to postgres;
revoke all on function util.dataset_flow_identity_validate_support_set(
  uuid, jsonb, text
) from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_validate_flow_guard(
  p_actor uuid,
  p_guard jsonb,
  p_target boolean,
  p_support_snapshots jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_source_keys constant text[] := array[
    'id', 'version', 'user_id', 'state_code', 'modified_at',
    'payload_sha256', 'row_sha256', 'flow_type', 'flow_property_id',
    'flow_property_version', 'unit_group_id', 'unit_group_version',
    'category_path_sha256', 'source_trace_sha256'
  ];
  v_target_keys constant text[] := array[
    'id', 'version', 'user_id', 'state_code', 'modified_at',
    'payload_sha256', 'row_sha256', 'flow_type', 'flow_property_id',
    'flow_property_version', 'unit_group_id', 'unit_group_version',
    'category_path_sha256', 'reference'
  ];
  v_reference_keys constant text[] := array[
    '@refObjectId', '@type', '@uri', '@version',
    'common:shortDescription'
  ];
  v_flow public.flows%rowtype;
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_payload_sha256 text;
  v_row_sha256 text;
  v_category_sha256 text;
  v_flow_properties jsonb;
  v_reference_flow_property_internal_id text;
  v_reference_unit_internal_id text;
  v_claimed_fp_id uuid;
  v_claimed_fp_version text;
  v_claimed_ug_id uuid;
  v_claimed_ug_version text;
  v_fp_snapshot jsonb;
  v_ug_snapshot jsonb;
  v_support_validation jsonb;
begin
  if p_actor is null
    or jsonb_typeof(p_support_snapshots) <> 'array'
    or not private.dataset_flow_identity_exact_keys(
      p_guard,
      case when p_target then v_target_keys else v_source_keys end
    )
    or p_guard->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'user_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_guard->>'flow_property_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'unit_group_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_guard->>'flow_property_version'
      !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_guard->>'unit_group_version'
      !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_guard->>'payload_sha256' !~ '^[a-f0-9]{64}$'
    or p_guard->>'row_sha256' !~ '^[a-f0-9]{64}$'
    or p_guard->>'category_path_sha256' !~ '^[a-f0-9]{64}$'
    or (not p_target
      and p_guard->>'source_trace_sha256' !~ '^[a-f0-9]{64}$')
    or p_guard->>'flow_type' <> 'Elementary flow'
    or jsonb_typeof(p_guard->'state_code') <> 'number'
    or (p_guard->>'state_code')::integer
      <> (case when p_target then 100 else 0 end)
    or (p_target and not private.dataset_flow_identity_exact_keys(
      p_guard->'reference', v_reference_keys
    )) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_SCHEMA_MISMATCH'
    );
  end if;

  begin
    perform (p_guard->>'modified_at')::timestamp with time zone;
    v_claimed_fp_id := (p_guard->>'flow_property_id')::uuid;
    v_claimed_fp_version := p_guard->>'flow_property_version';
    v_claimed_ug_id := (p_guard->>'unit_group_id')::uuid;
    v_claimed_ug_version := p_guard->>'unit_group_version';
  exception when others then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_VALUE_MISMATCH'
    );
  end;

  select flow.*
  into v_flow
  from public.flows as flow
  where flow.id = (p_guard->>'id')::uuid
    and btrim(flow.version::text) = p_guard->>'version';

  if v_flow.id is null
    or v_flow.json_ordered is null
    or v_flow.json is null
    or v_flow.json::jsonb is distinct from v_flow.json_ordered::jsonb
    or v_flow.user_id::text is distinct from p_guard->>'user_id'
    or v_flow.state_code is distinct from (p_guard->>'state_code')::integer
    or v_flow.modified_at is distinct from
      (p_guard->>'modified_at')::timestamp with time zone
    or (not p_target and v_flow.user_id is distinct from p_actor)
    or (p_target and (v_flow.user_id is null or v_flow.user_id = p_actor))
    or v_flow.json #>>
      '{flowDataSet,flowInformation,dataSetInformation,common:UUID}'
      is distinct from p_guard->>'id'
    or v_flow.json #>>
      '{flowDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from p_guard->>'version'
    or v_flow.json #>>
      '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      is distinct from 'Elementary flow' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_LIVE_MISMATCH'
    );
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(
    v_flow.json_ordered::jsonb
  );
  v_row_sha256 := private.dataset_flow_identity_row_sha256(
    v_flow.id,
    btrim(v_flow.version::text),
    v_flow.user_id,
    v_flow.state_code,
    v_flow.modified_at,
    v_payload_sha256
  );
  v_category_sha256 := util.dataset_flow_identity_sha256(coalesce(
    v_flow.json #>
      '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}',
    'null'::jsonb
  ));

  if v_payload_sha256 is distinct from p_guard->>'payload_sha256'
    or v_row_sha256 is distinct from p_guard->>'row_sha256'
    or v_category_sha256 is distinct from p_guard->>'category_path_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_GUARD_HASH_DRIFT'
    );
  end if;

  v_flow_properties := v_flow.json #>
    '{flowDataSet,flowProperties,flowProperty}';
  if jsonb_typeof(v_flow_properties) = 'object' then
    v_flow_properties := jsonb_build_array(v_flow_properties);
  end if;
  v_reference_flow_property_internal_id := v_flow.json #>>
    '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  if nullif(v_reference_flow_property_internal_id, '') is null
    or jsonb_typeof(v_flow_properties) <> 'array'
    or not exists (
      select 1
      from jsonb_array_elements(v_flow_properties) as fp(value)
      where fp.value->>'@dataSetInternalID'
          = v_reference_flow_property_internal_id
        and fp.value #>>
        '{referenceToFlowPropertyDataSet,@refObjectId}'
          = v_claimed_fp_id::text
        and fp.value #>>
          '{referenceToFlowPropertyDataSet,@version}'
          = v_claimed_fp_version
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_REFERENCE_FLOW_PROPERTY_MISMATCH'
    );
  end if;

  select flowproperty.*
  into v_flowproperty
  from public.flowproperties as flowproperty
  where flowproperty.id = v_claimed_fp_id
    and btrim(flowproperty.version::text) = v_claimed_fp_version
    and (
      flowproperty.state_code = 100
      or (flowproperty.user_id = p_actor and flowproperty.state_code = 0)
    );

  if v_flowproperty.id is null
    or v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@refObjectId}'
      is distinct from v_claimed_ug_id::text
    or v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@version}'
      is distinct from v_claimed_ug_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_REFERENCE_UNIT_GROUP_MISMATCH'
    );
  end if;

  select snapshot.value
  into v_fp_snapshot
  from jsonb_array_elements(p_support_snapshots) as snapshot(value)
  where snapshot.value->>'table' = 'flowproperties'
    and snapshot.value->>'id' = v_claimed_fp_id::text
    and snapshot.value->>'version' = v_claimed_fp_version;
  v_support_validation := util.dataset_flow_identity_validate_support_snapshot(
    p_actor, v_fp_snapshot
  );
  if v_fp_snapshot is null
    or coalesce((v_support_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_FLOW_PROPERTY_SNAPSHOT_MISMATCH',
      'details', v_support_validation
    );
  end if;

  select unitgroup.*
  into v_unitgroup
  from public.unitgroups as unitgroup
  where unitgroup.id = v_claimed_ug_id
    and btrim(unitgroup.version::text) = v_claimed_ug_version
    and (
      unitgroup.state_code = 100
      or (unitgroup.user_id = p_actor and unitgroup.state_code = 0)
    );
  v_reference_unit_internal_id := v_unitgroup.json #>>
    '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}';

  if v_unitgroup.id is null
    or v_unitgroup.json #>>
      '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:UUID}'
      is distinct from v_claimed_ug_id::text
    or v_unitgroup.json #>>
      '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from v_claimed_ug_version
    or nullif(v_reference_unit_internal_id, '') is null
    or not exists (
      select 1
      from jsonb_array_elements(
        case jsonb_typeof(v_unitgroup.json #>
          '{unitGroupDataSet,units,unit}')
          when 'array' then v_unitgroup.json #>
            '{unitGroupDataSet,units,unit}'
          when 'object' then jsonb_build_array(v_unitgroup.json #>
            '{unitGroupDataSet,units,unit}')
          else '[]'::jsonb
        end
      ) as unit_item(value)
      where unit_item.value->>'@dataSetInternalID'
          = v_reference_unit_internal_id
        and (unit_item.value->>'meanValue')::numeric = 1
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_REFERENCE_UNIT_MISMATCH'
    );
  end if;

  select snapshot.value
  into v_ug_snapshot
  from jsonb_array_elements(p_support_snapshots) as snapshot(value)
  where snapshot.value->>'table' = 'unitgroups'
    and snapshot.value->>'id' = v_claimed_ug_id::text
    and snapshot.value->>'version' = v_claimed_ug_version;
  v_support_validation := util.dataset_flow_identity_validate_support_snapshot(
    p_actor, v_ug_snapshot
  );
  if v_ug_snapshot is null
    or coalesce((v_support_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_UNIT_GROUP_SNAPSHOT_MISMATCH',
      'details', v_support_validation
    );
  end if;

  if p_target and (
    p_guard #>> '{reference,@refObjectId}' is distinct from v_flow.id::text
    or p_guard #>> '{reference,@version}'
      is distinct from btrim(v_flow.version::text)
    or p_guard #>> '{reference,@type}' is distinct from 'flow data set'
    or p_guard #>> '{reference,@uri}' is distinct from
      '../flows/' || v_flow.id::text || '_'
        || btrim(v_flow.version::text) || '.xml'
    or jsonb_typeof(p_guard #> '{reference,common:shortDescription}')
      not in ('string', 'object', 'array')
    or not (
      private.dataset_flow_identity_text_values(
        p_guard #> '{reference,common:shortDescription}'
      ) && private.dataset_flow_identity_text_values(
        v_flow.json #>
          '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
      )
    )
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_TARGET_REFERENCE_MISMATCH'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'payload_sha256', v_payload_sha256,
    'row_sha256', v_row_sha256,
    'category_path_sha256', v_category_sha256
  );
exception when others then
  return jsonb_build_object(
    'ok', false,
    'code', 'FLOW_IDENTITY_FLOW_GUARD_INVALID',
    'sqlstate', sqlstate,
    'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_validate_flow_guard(
  uuid, jsonb, boolean, jsonb
) owner to postgres;
revoke all on function util.dataset_flow_identity_validate_flow_guard(
  uuid, jsonb, boolean, jsonb
) from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_validate_process_guard(
  p_actor uuid,
  p_manifest jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_keys constant text[] := array[
    'ordinal', 'id', 'version', 'user_id', 'state_code', 'modified_at',
    'model_id', 'rule_verification',
    'before_row_sha256', 'before_payload_sha256',
    'before_exchange_set_sha256', 'before_exchange_count',
    'desired_payload_sha256', 'desired_exchange_set_sha256',
    'rewrite_count', 'process_template_sha256', 'rewrite_set_sha256',
    'rewrites', 'collision_ledger', 'collision_ledger_sha256',
    'derivative_baseline_snapshot_sha256',
    'process_schema', 'pending_blocker_closure_sha256'
  ];
  v_process public.processes%rowtype;
  v_payload jsonb;
  v_exchanges jsonb;
  v_payload_sha256 text;
  v_exchange_sha256 text;
  v_row_sha256 text;
  v_snapshot jsonb;
  v_template_sha256 text;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(p_manifest, v_keys)
    or jsonb_typeof(p_manifest->'ordinal') <> 'number'
    or (p_manifest->>'ordinal')::integer <= 0
    or p_manifest->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_manifest->>'user_id' is distinct from p_actor::text
    or p_manifest->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(p_manifest->'state_code') <> 'number'
    or (p_manifest->>'state_code')::integer <> 0
    or jsonb_typeof(p_manifest->'before_exchange_count') <> 'number'
    or jsonb_typeof(p_manifest->'rewrite_count') <> 'number'
    or (p_manifest->>'rewrite_count')::integer <= 0
    or jsonb_typeof(p_manifest->'rewrites') <> 'array'
    or jsonb_array_length(p_manifest->'rewrites')
      <> (p_manifest->>'rewrite_count')::integer
    or jsonb_typeof(p_manifest->'collision_ledger') <> 'object'
    or util.dataset_flow_identity_restricted_sha256_v2(p_manifest->'rewrites')
      is distinct from p_manifest->>'rewrite_set_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_manifest->'collision_ledger'
    )
      is distinct from p_manifest->>'collision_ledger_sha256'
    or not private.dataset_flow_identity_exact_keys(
      p_manifest->'process_schema', array['status', 'evidence_sha256']
    )
    or p_manifest #>> '{process_schema,status}' <> 'pass'
    or exists (
      select 1
      from unnest(array[
        'before_row_sha256', 'before_payload_sha256',
        'before_exchange_set_sha256', 'desired_payload_sha256',
        'desired_exchange_set_sha256', 'process_template_sha256',
        'rewrite_set_sha256', 'collision_ledger_sha256',
        'derivative_baseline_snapshot_sha256',
        'pending_blocker_closure_sha256'
      ]) as hash_field(name)
      where p_manifest->>hash_field.name !~ '^[a-f0-9]{64}$'
    )
    or p_manifest #>> '{process_schema,evidence_sha256}'
      !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_SCHEMA_MISMATCH'
    );
  end if;

  if jsonb_typeof(p_manifest->'model_id') not in ('string', 'null')
    or (
      jsonb_typeof(p_manifest->'model_id') = 'string'
      and p_manifest->>'model_id'
        !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
    or jsonb_typeof(p_manifest->'rule_verification')
      not in ('boolean', 'null') then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_METADATA_SCHEMA_MISMATCH'
    );
  end if;

  begin
    perform (p_manifest->>'modified_at')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_VALUE_MISMATCH'
    );
  end;

  v_template_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    p_manifest - 'process_template_sha256'
  );
  if v_template_sha256 is distinct from
    p_manifest->>'process_template_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_TEMPLATE_HASH_MISMATCH'
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = (p_manifest->>'id')::uuid
    and btrim(process.version::text) = p_manifest->>'version'
    and process.user_id = p_actor
    and process.state_code = 0;

  if v_process.id is null
    or v_process.json is null
    or v_process.json_ordered is null
    or v_process.json::jsonb is distinct from v_process.json_ordered::jsonb
    or v_process.modified_at is distinct from
      (p_manifest->>'modified_at')::timestamp with time zone
    or coalesce(to_jsonb(v_process.model_id), 'null'::jsonb)
      is distinct from p_manifest->'model_id'
    or coalesce(to_jsonb(v_process.rule_verification), 'null'::jsonb)
      is distinct from p_manifest->'rule_verification' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_SCOPE_MISMATCH'
    );
  end if;

  v_payload := v_process.json_ordered::jsonb;
  v_exchanges := private.dataset_flow_identity_exchanges(v_payload);
  if v_exchanges is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_EXCHANGES_INVALID'
    );
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(v_payload);
  v_exchange_sha256 := util.dataset_flow_identity_sha256(v_exchanges);
  v_row_sha256 := util.dataset_flow_identity_sha256(jsonb_build_object(
    'id', v_process.id,
    'version', btrim(v_process.version::text),
    'user_id', v_process.user_id,
    'state_code', v_process.state_code,
    'modified_at', v_process.modified_at,
    'model_id', v_process.model_id,
    'rule_verification', v_process.rule_verification,
    'payload_sha256', v_payload_sha256
  ));
  begin
    v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception when others then
    v_snapshot := null;
  end;

  if v_payload_sha256 is distinct from
      p_manifest->>'before_payload_sha256'
    or v_exchange_sha256 is distinct from
      p_manifest->>'before_exchange_set_sha256'
    or v_row_sha256 is distinct from p_manifest->>'before_row_sha256'
    or jsonb_array_length(v_exchanges) is distinct from
      (p_manifest->>'before_exchange_count')::integer
    or v_snapshot is null
    or v_snapshot->>'snapshot_sha256' is distinct from
      p_manifest->>'derivative_baseline_snapshot_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_BASELINE_DRIFT'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'payload_sha256', v_payload_sha256,
    'exchange_set_sha256', v_exchange_sha256,
    'row_sha256', v_row_sha256,
    'derivative_snapshot_sha256', v_snapshot->>'snapshot_sha256'
  );
exception when others then
  return jsonb_build_object(
    'ok', false,
    'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_INVALID',
    'sqlstate', sqlstate,
    'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_validate_process_guard(uuid, jsonb)
  owner to postgres;
revoke all on function util.dataset_flow_identity_validate_process_guard(
  uuid, jsonb
) from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_protected_closure(
  p_actor uuid,
  p_closure jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_keys constant text[] := array[
    'schema_version', 'pending', 'blockers', 'orphans',
    'pending_set_sha256', 'blocker_set_sha256', 'orphan_set_sha256',
    'total_expected_reference_count'
  ];
  v_entries jsonb;
  v_observed jsonb;
  v_invalid integer;
  v_expected_total bigint;
  v_observed_total bigint;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(p_closure, v_keys)
    or p_closure->>'schema_version'
      <> 'dataset-flow-identity-protected-closure.v1'
    or jsonb_typeof(p_closure->'pending') <> 'array'
    or jsonb_typeof(p_closure->'blockers') <> 'array'
    or jsonb_typeof(p_closure->'orphans') <> 'array'
    or p_closure->>'pending_set_sha256' !~ '^[a-f0-9]{64}$'
    or p_closure->>'blocker_set_sha256' !~ '^[a-f0-9]{64}$'
    or p_closure->>'orphan_set_sha256' !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_closure->'total_expected_reference_count')
      <> 'number'
    or util.dataset_flow_identity_sha256(p_closure->'pending')
      is distinct from p_closure->>'pending_set_sha256'
    or util.dataset_flow_identity_sha256(p_closure->'blockers')
      is distinct from p_closure->>'blocker_set_sha256'
    or util.dataset_flow_identity_sha256(p_closure->'orphans')
      is distinct from p_closure->>'orphan_set_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROTECTED_CLOSURE_SCHEMA_MISMATCH'
    );
  end if;

  select coalesce(jsonb_agg(entry order by partition_ordinal, ordinal), '[]'::jsonb)
  into v_entries
  from (
    select
      1 as partition_ordinal,
      item.ordinality::integer as ordinal,
      item.value || jsonb_build_object('partition', 'pending') as entry
    from jsonb_array_elements(p_closure->'pending')
      with ordinality as item(value, ordinality)
    union all
    select
      2,
      item.ordinality::integer,
      item.value || jsonb_build_object('partition', 'blockers')
    from jsonb_array_elements(p_closure->'blockers')
      with ordinality as item(value, ordinality)
    union all
    select
      3,
      item.ordinality::integer,
      item.value || jsonb_build_object(
        'partition', 'orphans',
        'expected_reference_count', 0,
        'occurrences', '[]'::jsonb,
        'occurrence_set_sha256',
          util.dataset_flow_identity_sha256('[]'::jsonb)
      )
    from jsonb_array_elements(p_closure->'orphans')
      with ordinality as item(value, ordinality)
  ) as combined;

  select count(*)::integer
  into v_invalid
  from jsonb_array_elements(v_entries) as entry(value)
  where not private.dataset_flow_identity_exact_keys(entry.value, array[
      'source_id', 'source_version', 'expected_reference_count',
      'occurrences', 'occurrence_set_sha256', 'evidence_sha256', 'partition'
    ])
    or entry.value->>'source_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or entry.value->>'source_version'
      !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(entry.value->'expected_reference_count') <> 'number'
    or (entry.value->>'expected_reference_count')::integer < 0
    or jsonb_typeof(entry.value->'occurrences') <> 'array'
    or jsonb_array_length(entry.value->'occurrences')
      <> (entry.value->>'expected_reference_count')::integer
    or entry.value->>'occurrence_set_sha256' !~ '^[a-f0-9]{64}$'
    or util.dataset_flow_identity_sha256(entry.value->'occurrences')
      is distinct from entry.value->>'occurrence_set_sha256'
    or entry.value->>'evidence_sha256' !~ '^[a-f0-9]{64}$'
    or entry.value->>'partition' not in ('pending', 'blockers', 'orphans')
    or exists (
      select 1
      from jsonb_array_elements(entry.value->'occurrences')
        with ordinality as occurrence(value, ordinality)
      where not private.dataset_flow_identity_exact_keys(
          occurrence.value,
          array[
            'process_id', 'process_version', 'exchange_index', 'internal_id',
            'direction', 'reference_sha256'
          ]
        )
        or occurrence.value->>'process_id'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or occurrence.value->>'process_version'
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or jsonb_typeof(occurrence.value->'exchange_index') <> 'number'
        or (occurrence.value->>'exchange_index')::integer < 0
        or nullif(occurrence.value->>'internal_id', '') is null
        or occurrence.value->>'direction' not in ('Input', 'Output')
        or occurrence.value->>'reference_sha256' !~ '^[a-f0-9]{64}$'
    );

  if v_invalid > 0 or (
    select count(*)
    from (
      select distinct
        entry.value->>'source_id' as id,
        entry.value->>'source_version' as version
      from jsonb_array_elements(v_entries) as entry(value)
    ) as unique_entry
  ) <> jsonb_array_length(v_entries) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROTECTED_CLOSURE_ENTRY_MISMATCH'
    );
  end if;

  with wanted as (
    select
      entry.ordinality::integer as ordinal,
      (entry.value->>'source_id')::uuid as source_id,
      entry.value->>'source_version' as source_version,
      (entry.value->>'expected_reference_count')::integer
        as expected_reference_count,
      entry.value->'occurrences' as expected_occurrences,
      entry.value->>'occurrence_set_sha256' as occurrence_set_sha256,
      entry.value->>'evidence_sha256' as evidence_sha256,
      entry.value->>'partition' as partition
    from jsonb_array_elements(v_entries)
      with ordinality as entry(value, ordinality)
  ), live_occurrence as (
    select
      wanted.source_id,
      wanted.source_version,
      process.id as process_id,
      btrim(process.version::text) as process_version,
      (exchange.ordinality - 1)::integer as exchange_index,
      exchange.value->>'@dataSetInternalID' as internal_id,
      exchange.value->>'exchangeDirection' as direction,
      util.dataset_flow_identity_sha256(
        private.dataset_flow_identity_reference(exchange.value)
      ) as reference_sha256
    from public.processes as process
    cross join lateral jsonb_array_elements(
      private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
    ) with ordinality as exchange(value, ordinality)
    join wanted
      on wanted.source_id::text = exchange.value #>>
          '{referenceToFlowDataSet,@refObjectId}'
      and wanted.source_version = exchange.value #>>
          '{referenceToFlowDataSet,@version}'
    where process.user_id = p_actor
      and process.state_code = 0
  ), live as (
    select
      wanted.source_id,
      wanted.source_version,
      coalesce(jsonb_agg(jsonb_build_object(
        'process_id', live_occurrence.process_id,
        'process_version', live_occurrence.process_version,
        'exchange_index', live_occurrence.exchange_index,
        'internal_id', live_occurrence.internal_id,
        'direction', live_occurrence.direction,
        'reference_sha256', live_occurrence.reference_sha256
      ) order by
        live_occurrence.process_id,
        live_occurrence.process_version,
        live_occurrence.exchange_index
      ) filter (where live_occurrence.process_id is not null), '[]'::jsonb)
        as observed_occurrences
    from wanted
    left join live_occurrence using (source_id, source_version)
    group by wanted.source_id, wanted.source_version
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', wanted.ordinal,
      'partition', wanted.partition,
      'source_id', wanted.source_id,
      'source_version', wanted.source_version,
      'expected_reference_count', wanted.expected_reference_count,
      'observed_reference_count',
        jsonb_array_length(live.observed_occurrences),
      'evidence_sha256', wanted.evidence_sha256,
      'expected_occurrence_set_sha256', wanted.occurrence_set_sha256,
      'observed_occurrence_set_sha256',
        util.dataset_flow_identity_sha256(live.observed_occurrences),
      'matches', wanted.expected_occurrences = live.observed_occurrences
    ) order by wanted.ordinal), '[]'::jsonb),
    coalesce(sum(wanted.expected_reference_count), 0)::bigint,
    coalesce(sum(jsonb_array_length(live.observed_occurrences)), 0)::bigint
  into v_observed, v_expected_total, v_observed_total
  from wanted
  left join live using (source_id, source_version);

  return jsonb_build_object(
    'ok', v_expected_total = v_observed_total
      and not exists (
        select 1
        from jsonb_array_elements(v_observed) as observed(value)
        where coalesce((observed.value->>'matches')::boolean, false) is false
      )
      and v_expected_total =
        (p_closure->>'total_expected_reference_count')::bigint,
    'schema_version', 'dataset-flow-identity-protected-closure-proof.v1',
    'expected_total_reference_count', v_expected_total,
    'observed_total_reference_count', v_observed_total,
    'entries', v_observed,
    'observed_sha256', util.dataset_flow_identity_sha256(v_observed)
  );
exception when others then
  return jsonb_build_object(
    'ok', false,
    'code', 'FLOW_IDENTITY_PROTECTED_CLOSURE_INVALID',
    'sqlstate', sqlstate,
    'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_protected_closure(uuid, jsonb)
  owner to postgres;
revoke all on function util.dataset_flow_identity_protected_closure(uuid, jsonb)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_source_universe(
  p_actor uuid,
  p_expected_universe jsonb,
  p_expected_sha256 text
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_live jsonb;
  v_invalid_count integer;
  v_live_sha256 text;
begin
  if p_actor is null
    or jsonb_typeof(p_expected_universe) <> 'array'
    or jsonb_array_length(p_expected_universe) <> 305
    or p_expected_sha256 !~ '^[a-f0-9]{64}$'
    or util.dataset_flow_identity_sha256(p_expected_universe)
      is distinct from p_expected_sha256
    or exists (
      select 1
      from jsonb_array_elements(p_expected_universe)
        with ordinality as item(value, ordinality)
      where not private.dataset_flow_identity_exact_keys(
          item.value,
          array['id', 'version', 'user_id', 'state_code', 'flow_type']
        )
        or item.value->>'id'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or item.value->>'version'
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or item.value->>'user_id' <> p_actor::text
        or item.value->>'state_code' <> '0'
        or item.value->>'flow_type' <> 'Elementary flow'
    ) then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SOURCE_UNIVERSE_SCHEMA_MISMATCH'
    );
  end if;

  select
    coalesce(jsonb_agg(jsonb_build_object(
      'id', flow.id,
      'version', btrim(flow.version::text),
      'user_id', flow.user_id,
      'state_code', flow.state_code,
      'flow_type', flow.json_ordered #>>
        '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
    ) order by flow.id, btrim(flow.version::text)), '[]'::jsonb),
    count(*) filter (
      where flow.json is null
        or flow.json_ordered is null
        or flow.json::jsonb is distinct from flow.json_ordered::jsonb
        or flow.json_ordered #>>
          '{flowDataSet,flowInformation,dataSetInformation,common:UUID}'
          is distinct from flow.id::text
        or flow.json_ordered #>>
          '{flowDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
          is distinct from btrim(flow.version::text)
    )::integer
  into v_live, v_invalid_count
  from public.flows as flow
  where flow.user_id = p_actor
    and flow.state_code = 0
    and flow.json_ordered #>>
      '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      = 'Elementary flow';

  v_live_sha256 := util.dataset_flow_identity_sha256(v_live);
  return jsonb_build_object(
    'ok', v_invalid_count = 0
      and jsonb_array_length(v_live) = 305
      and v_live is not distinct from p_expected_universe
      and v_live_sha256 is not distinct from p_expected_sha256,
    'schema_version', 'dataset-flow-identity-source-universe-proof.v1',
    'expected_count', 305,
    'observed_count', jsonb_array_length(v_live),
    'expected_sha256', p_expected_sha256,
    'observed_sha256', v_live_sha256,
    'invalid_live_row_count', v_invalid_count
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_SOURCE_UNIVERSE_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_source_universe(uuid, jsonb, text)
  owner to postgres;
revoke all on function util.dataset_flow_identity_source_universe(
  uuid, jsonb, text
) from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_validate_mapping(
  p_actor uuid,
  p_mapping jsonb,
  p_policy jsonb,
  p_support_snapshots jsonb,
  p_expected_ordinal integer
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_mapping_keys constant text[] := array[
    'ordinal', 'mapping_id', 'source', 'target', 'compatibility'
  ];
  v_compatibility_keys constant text[] := array[
    'policy_sha256', 'mode', 'confidence', 'flow_property_compatible',
    'unit_group_compatible', 'direction_compatible',
    'compartment_compatible', 'conversion_factor', 'evidence_sha256',
    'flow_schema', 'process_schema_required'
  ];
  v_flow_schema_keys constant text[] := array[
    'status', 'warning_set_sha256'
  ];
  v_mapping_id text;
  v_source_result jsonb;
  v_target_result jsonb;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(
      p_mapping, v_mapping_keys
    )
    or jsonb_typeof(p_mapping->'ordinal') <> 'number'
    or (p_mapping->>'ordinal')::integer <> p_expected_ordinal
    or p_mapping->>'mapping_id' !~ '^[a-f0-9]{64}$'
    or not private.dataset_flow_identity_exact_keys(
      p_mapping->'compatibility', v_compatibility_keys
    )
    or not private.dataset_flow_identity_exact_keys(
      p_mapping #> '{compatibility,flow_schema}', v_flow_schema_keys
    )
    or p_mapping #>> '{compatibility,policy_sha256}'
      is distinct from p_policy->>'policy_sha256'
    or p_mapping #>> '{compatibility,mode}' <> 'identity'
    or p_mapping #>> '{compatibility,confidence}' <> 'approved'
    or p_mapping #>> '{compatibility,conversion_factor}' <> '1'
    or p_mapping #>> '{compatibility,evidence_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_mapping #>> '{compatibility,flow_schema,status}'
      not in ('pass', 'legacy_warning')
    or p_mapping #>> '{compatibility,flow_schema,warning_set_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_mapping #>> '{compatibility,process_schema_required}' <> 'pass'
    or exists (
      select 1
      from unnest(array[
        'flow_property_compatible', 'unit_group_compatible',
        'direction_compatible', 'compartment_compatible'
      ]) as boolean_field(name)
      where jsonb_typeof(
          p_mapping->'compatibility'->boolean_field.name
        ) <> 'boolean'
        or coalesce((
          p_mapping->'compatibility'->>boolean_field.name
        )::boolean, false) is false
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_SCHEMA_MISMATCH'
    );
  end if;

  v_mapping_id := util.dataset_flow_identity_restricted_sha256_v2(
    p_mapping - 'ordinal' - 'mapping_id'
  );
  if v_mapping_id is distinct from p_mapping->>'mapping_id' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_HASH_MISMATCH'
    );
  end if;

  v_source_result := util.dataset_flow_identity_validate_flow_guard(
    p_actor, p_mapping->'source', false, p_support_snapshots
  );
  if coalesce((v_source_result->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_SOURCE_REJECTED',
      'details', v_source_result
    );
  end if;

  v_target_result := util.dataset_flow_identity_validate_flow_guard(
    p_actor, p_mapping->'target', true, p_support_snapshots
  );
  if coalesce((v_target_result->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_TARGET_REJECTED',
      'details', v_target_result
    );
  end if;

  if p_mapping #>> '{source,id}' = p_mapping #>> '{target,id}'
    and p_mapping #>> '{source,version}'
      = p_mapping #>> '{target,version}' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_NOOP'
    );
  end if;

  if p_mapping #>> '{source,flow_property_id}'
      is distinct from p_mapping #>> '{target,flow_property_id}'
    or p_mapping #>> '{source,flow_property_version}'
      is distinct from p_mapping #>> '{target,flow_property_version}'
    or p_mapping #>> '{source,unit_group_id}'
      is distinct from p_mapping #>> '{target,unit_group_id}'
    or p_mapping #>> '{source,unit_group_version}'
      is distinct from p_mapping #>> '{target,unit_group_version}' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_NOT_IDENTITY_ONLY'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'mapping_id', v_mapping_id,
    'source', v_source_result,
    'target', v_target_result
  );
end;
$$;

alter function util.dataset_flow_identity_validate_mapping(
  uuid, jsonb, jsonb, jsonb, integer
) owner to postgres;
revoke all on function util.dataset_flow_identity_validate_mapping(
  uuid, jsonb, jsonb, jsonb, integer
) from public, anon, authenticated, service_role;

-- Defined before preflight because every sealed process must be reconstructed
-- and collision-checked before any scope metadata or primary write exists.
create or replace function private.dataset_flow_identity_collision_ledger(
  p_exchanges jsonb,
  p_rewrites jsonb
) returns jsonb
language sql
stable
strict
set search_path = ''
as $$
  with touched as (
    select distinct
      rewrite.value #>> '{target_reference,@refObjectId}' as target_id,
      rewrite.value #>> '{target_reference,@version}' as target_version
    from jsonb_array_elements(p_rewrites) as rewrite(value)
  ), matching as (
    select
      touched.target_id,
      touched.target_version,
      (exchange.ordinality - 1)::integer as exchange_index,
      exchange.value->>'@dataSetInternalID' as internal_id,
      rewrite.value->>'mapping_id' as mapping_id
    from touched
    join lateral jsonb_array_elements(p_exchanges)
      with ordinality as exchange(value, ordinality)
      on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
          = touched.target_id
        and exchange.value #>> '{referenceToFlowDataSet,@version}'
          = touched.target_version
    left join lateral (
      select candidate.value
      from jsonb_array_elements(p_rewrites) as candidate(value)
      where (candidate.value->>'exchange_index')::integer
        = exchange.ordinality - 1
      limit 1
    ) as rewrite on true
  ), grouped as (
    select
      matching.target_id,
      matching.target_version,
      count(*)::integer as multiplicity,
      jsonb_agg(matching.exchange_index order by matching.exchange_index)
        as exchange_indexes,
      jsonb_agg(matching.internal_id order by matching.exchange_index)
        as internal_ids,
      jsonb_agg(to_jsonb(matching.mapping_id)
        order by matching.exchange_index) as mapping_ids
    from matching
    group by matching.target_id, matching.target_version
    having count(*) > 1
  )
  select jsonb_build_object(
    'schema_version', 'dataset-flow-identity-collision-ledger.v1',
    'entries', coalesce(jsonb_agg(jsonb_build_object(
      'target_id', grouped.target_id,
      'target_version', grouped.target_version,
      'exchange_indexes', grouped.exchange_indexes,
      'internal_ids', grouped.internal_ids,
      'mapping_ids', grouped.mapping_ids,
      'preserve_rows', true
    ) order by grouped.target_id, grouped.target_version), '[]'::jsonb)
  )
  from grouped
$$;

alter function private.dataset_flow_identity_collision_ledger(jsonb, jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_collision_ledger(jsonb, jsonb)
  from public, anon, authenticated, service_role;

-- Materialized once per request so process validation can resolve mapping
-- ordinal/id/source identities without repeated array scans.
create or replace function private.dataset_flow_identity_mapping_index_v2(
  p_mappings jsonb
) returns jsonb
language plpgsql
stable
strict
set search_path = ''
as $$
declare
  v_count integer;
  v_distinct_ordinals integer;
  v_distinct_ids integer;
  v_distinct_sources integer;
  v_by_ordinal jsonb;
  v_by_id jsonb;
  v_by_source jsonb;
begin
  if jsonb_typeof(p_mappings) <> 'array'
    or jsonb_array_length(p_mappings) not between 1 and 305 then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_MAPPING_INDEX_ARRAY_INVALID';
  end if;
  select
    count(*)::integer,
    count(distinct item.value->>'ordinal')::integer,
    count(distinct item.value->>'mapping_id')::integer,
    count(distinct (item.value #>> '{source,id}') || '@'
      || (item.value #>> '{source,version}'))::integer,
    jsonb_object_agg(item.value->>'ordinal', item.value),
    jsonb_object_agg(item.value->>'mapping_id', item.value),
    jsonb_object_agg(
      (item.value #>> '{source,id}') || '@'
        || (item.value #>> '{source,version}'), item.value
    )
  into v_count, v_distinct_ordinals, v_distinct_ids, v_distinct_sources,
    v_by_ordinal, v_by_id, v_by_source
  from jsonb_array_elements(p_mappings) as item(value)
  where jsonb_typeof(item.value->'ordinal') = 'number'
    and (item.value->>'ordinal')::numeric between 1 and 305
    and item.value->>'mapping_id' ~ '^[a-f0-9]{64}$';
  if v_count <> jsonb_array_length(p_mappings)
    or v_distinct_ordinals <> v_count
    or v_distinct_ids <> v_count
    or v_distinct_sources <> v_count then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_MAPPING_INDEX_IDENTITY_INVALID';
  end if;
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-mapping-index.v2',
    'mapping_count', v_count,
    'mapping_guard_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(p_mappings),
    'mappings', p_mappings,
    'by_ordinal', v_by_ordinal,
    'by_id', v_by_id,
    'by_source', v_by_source
  );
end;
$$;

alter function private.dataset_flow_identity_mapping_index_v2(jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_mapping_index_v2(jsonb)
  from public, anon, authenticated, service_role;

create or replace function util.dataset_flow_identity_dry_validate_process(
  p_actor uuid,
  p_manifest jsonb,
  p_mappings jsonb,
  p_policy jsonb,
  p_support_snapshots jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_rewrite_keys constant text[] := array[
    'ordinal', 'exchange_index', 'internal_id', 'direction', 'mapping_id',
    'source_reference', 'target_reference', 'before_reference_sha256',
    'after_reference_sha256'
  ];
  v_reference_keys constant text[] := array[
    '@refObjectId', '@type', '@uri', '@version',
    'common:shortDescription'
  ];
  v_process_validation jsonb;
  v_mapping_validation jsonb;
  v_process public.processes%rowtype;
  v_mapping jsonb;
  v_rewrite jsonb;
  v_before_payload jsonb;
  v_before_exchanges jsonb;
  v_after_exchanges jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_before_reference jsonb;
  v_after_reference jsonb;
  v_collision jsonb;
  v_desired_payload jsonb;
  v_after_payload_sha256 text;
  v_after_exchange_sha256 text;
  v_mapping_array jsonb;
  v_mapping_by_id jsonb;
  v_mapping_by_source jsonb;
  v_indexed boolean := false;
begin
  if jsonb_typeof(p_mappings) = 'array' then
    v_mapping_array := p_mappings;
  elsif jsonb_typeof(p_mappings) = 'object'
    and private.dataset_flow_identity_exact_keys(p_mappings, array[
      'schema_version', 'mapping_count', 'mapping_guard_set_sha256',
      'mappings', 'by_ordinal', 'by_id', 'by_source'
    ])
    and p_mappings->>'schema_version'
      = 'dataset-flow-identity-mapping-index.v2'
    and jsonb_typeof(p_mappings->'mapping_count') = 'number'
    and (p_mappings->>'mapping_count')::integer
      = jsonb_array_length(p_mappings->'mappings')
    and p_mappings->>'mapping_guard_set_sha256' ~ '^[a-f0-9]{64}$'
    and jsonb_typeof(p_mappings->'mappings') = 'array'
    and jsonb_typeof(p_mappings->'by_ordinal') = 'object'
    and jsonb_typeof(p_mappings->'by_id') = 'object'
    and jsonb_typeof(p_mappings->'by_source') = 'object' then
    v_mapping_array := p_mappings->'mappings';
    v_mapping_by_id := p_mappings->'by_id';
    v_mapping_by_source := p_mappings->'by_source';
    v_indexed := true;
  end if;
  if p_actor is null
    or v_mapping_array is null
    or jsonb_typeof(p_support_snapshots) <> 'array' then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_PROCESS_INVALID_REQUEST'
    );
  end if;
  v_process_validation := util.dataset_flow_identity_validate_process_guard(
    p_actor, p_manifest
  );
  if coalesce((v_process_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_PROCESS_BASELINE_REJECTED',
      'details', v_process_validation
    );
  end if;
  if (
    select count(*) = (p_manifest->>'rewrite_count')::integer
      and min((rewrite.value->>'ordinal')::integer) = 1
      and max((rewrite.value->>'ordinal')::integer)
        = (p_manifest->>'rewrite_count')::integer
      and count(distinct (rewrite.value->>'ordinal')::integer)
        = (p_manifest->>'rewrite_count')::integer
      and count(distinct (rewrite.value->>'exchange_index')::integer)
        = (p_manifest->>'rewrite_count')::integer
      and bool_and(
        (rewrite.value->>'ordinal')::integer = rewrite.ordinality::integer
      )
    from jsonb_array_elements(p_manifest->'rewrites')
      with ordinality as rewrite(value, ordinality)
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_REWRITE_ORDINAL_MISMATCH'
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = (p_manifest->>'id')::uuid
    and btrim(process.version::text) = p_manifest->>'version'
    and process.user_id = p_actor and process.state_code = 0;
  v_before_payload := v_process.json_ordered::jsonb;
  v_before_exchanges := private.dataset_flow_identity_exchanges(v_before_payload);
  v_after_exchanges := v_before_exchanges;

  for v_rewrite in
    select rewrite.value
    from jsonb_array_elements(p_manifest->'rewrites')
      with ordinality as rewrite(value, ordinality)
    order by rewrite.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(v_rewrite, v_rewrite_keys)
      or v_rewrite->>'ordinal' !~ '^[1-9][0-9]*$'
      or v_rewrite->>'exchange_index' !~ '^(0|[1-9][0-9]*)$'
      or (v_rewrite->>'exchange_index')::integer
        >= jsonb_array_length(v_before_exchanges)
      or nullif(v_rewrite->>'internal_id', '') is null
      or v_rewrite->>'direction' not in ('Input', 'Output')
      or v_rewrite->>'mapping_id' !~ '^[a-f0-9]{64}$'
      or v_rewrite->>'before_reference_sha256' !~ '^[a-f0-9]{64}$'
      or v_rewrite->>'after_reference_sha256' !~ '^[a-f0-9]{64}$'
      or not private.dataset_flow_identity_exact_keys(
        v_rewrite->'source_reference', v_reference_keys
      )
      or not private.dataset_flow_identity_exact_keys(
        v_rewrite->'target_reference', v_reference_keys
      ) then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_REWRITE_SCHEMA_MISMATCH'
      );
    end if;

    if v_indexed then
      v_mapping := v_mapping_by_id->(v_rewrite->>'mapping_id');
    else
      select mapping.value
      into v_mapping
      from jsonb_array_elements(v_mapping_array) as mapping(value)
      where mapping.value->>'mapping_id' = v_rewrite->>'mapping_id';
    end if;
    if v_mapping is null then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_MAPPING_NOT_SEALED'
      );
    end if;
    if not v_indexed then
      v_mapping_validation := util.dataset_flow_identity_validate_mapping(
        p_actor, v_mapping, p_policy, p_support_snapshots,
        (v_mapping->>'ordinal')::integer
      );
      if coalesce((v_mapping_validation->>'ok')::boolean, false) is false then
        return jsonb_build_object(
          'ok', false, 'code', 'FLOW_IDENTITY_DRY_MAPPING_REJECTED',
          'details', v_mapping_validation
        );
      end if;
    end if;

    v_before_exchange := v_after_exchanges ->
      (v_rewrite->>'exchange_index')::integer;
    v_before_reference := private.dataset_flow_identity_reference(
      v_before_exchange
    );
    if v_before_exchange->>'@dataSetInternalID'
        is distinct from v_rewrite->>'internal_id'
      or v_before_exchange->>'exchangeDirection'
        is distinct from v_rewrite->>'direction'
      or v_before_reference is distinct from v_rewrite->'source_reference'
      or util.dataset_flow_identity_sha256(v_before_reference)
        is distinct from v_rewrite->>'before_reference_sha256'
      or v_before_reference->>'@refObjectId'
        is distinct from v_mapping #>> '{source,id}'
      or v_before_reference->>'@version'
        is distinct from v_mapping #>> '{source,version}'
      or v_rewrite->'target_reference'
        is distinct from v_mapping #> '{target,reference}'
      or util.dataset_flow_identity_sha256(v_rewrite->'target_reference')
        is distinct from v_rewrite->>'after_reference_sha256' then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_EXCHANGE_LOCATOR_DRIFT'
      );
    end if;

    v_after_exchange := jsonb_set(
      v_before_exchange,
      '{referenceToFlowDataSet}',
      (v_before_exchange->'referenceToFlowDataSet')
        || v_rewrite->'target_reference',
      false
    );
    v_after_reference := private.dataset_flow_identity_reference(v_after_exchange);
    if v_after_reference is distinct from v_rewrite->'target_reference'
      or v_after_exchange - 'referenceToFlowDataSet'
        is distinct from v_before_exchange - 'referenceToFlowDataSet'
      or (v_after_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription'
        is distinct from
        (v_before_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription' then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_FIVE_FIELD_BOUNDARY_FAILED'
      );
    end if;
    v_after_exchanges := jsonb_set(
      v_after_exchanges,
      array[(v_rewrite->>'exchange_index')::integer::text],
      v_after_exchange,
      false
    );
  end loop;

  v_collision := private.dataset_flow_identity_collision_ledger(
    v_after_exchanges, p_manifest->'rewrites'
  );
  v_desired_payload := private.dataset_flow_identity_replace_exchanges(
    v_before_payload, v_after_exchanges
  );
  v_after_payload_sha256 := util.dataset_flow_identity_sha256(v_desired_payload);
  v_after_exchange_sha256 := util.dataset_flow_identity_sha256(v_after_exchanges);
  if v_collision is distinct from p_manifest->'collision_ledger'
    or v_after_payload_sha256
      is distinct from p_manifest->>'desired_payload_sha256'
    or v_after_exchange_sha256
      is distinct from p_manifest->>'desired_exchange_set_sha256'
    or jsonb_array_length(v_after_exchanges)
      <> jsonb_array_length(v_before_exchanges) then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_DESIRED_CLOSURE_MISMATCH'
    );
  end if;
  if v_indexed then
    if exists (
      select 1
      from jsonb_array_elements(v_after_exchanges) as exchange(value)
      where v_mapping_by_source ? (
        (exchange.value #>> '{referenceToFlowDataSet,@refObjectId}') || '@'
          || (exchange.value #>> '{referenceToFlowDataSet,@version}')
      )
    ) then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_DRY_DESIRED_CLOSURE_MISMATCH'
      );
    end if;
  elsif exists (
    select 1
    from jsonb_array_elements(v_mapping_array) as mapping(value)
    join lateral jsonb_array_elements(v_after_exchanges) as exchange(value)
      on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
        = mapping.value #>> '{source,id}'
      and exchange.value #>> '{referenceToFlowDataSet,@version}'
        = mapping.value #>> '{source,version}'
  ) then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_DRY_DESIRED_CLOSURE_MISMATCH'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'before_payload_sha256', p_manifest->>'before_payload_sha256',
    'after_payload_sha256', v_after_payload_sha256,
    'after_exchange_set_sha256', v_after_exchange_sha256,
    'desired_payload', v_desired_payload,
    'desired_exchanges', v_after_exchanges,
    'collision_ledger', v_collision
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_DRY_PROCESS_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$$;

alter function util.dataset_flow_identity_dry_validate_process(
  uuid, jsonb, jsonb, jsonb, jsonb
) owner to postgres;
revoke all on function util.dataset_flow_identity_dry_validate_process(
  uuid, jsonb, jsonb, jsonb, jsonb
) from public, anon, authenticated, service_role;

create unique index command_audit_log_flow_identity_scope_uidx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'scope_id')
  )
  where command = 'cmd_dataset_flow_identity_scope_preflight_guarded'
    and target_table is null;

create or replace function public.cmd_dataset_flow_identity_scope_preflight_guarded(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '120s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_preflight_guarded';
  v_schema_version constant text :=
    'dataset-flow-identity-scope-preflight.v1';
  v_result_schema_version constant text :=
    'dataset-flow-identity-scope-preflight-result.v1';
  v_top_keys constant text[] := array[
    'schema_version', 'request_id', 'environment', 'project_ref', 'actor',
    'target_visibility', 'operation_id', 'plan_sha256', 'freeze_sha256',
    'approval_identity_sha256', 'approval_text_sha256',
    'toolchain_evidence_sha256', 'compatibility_policy',
    'support_snapshot_set_sha256', 'support_snapshots',
    'source_universe_sha256', 'source_universe_count',
    'mapping_set_sha256', 'process_manifest_sha256',
    'protected_closure_sha256', 'mappings', 'processes',
    'protected_closure'
  ];
  v_policy_keys constant text[] := array[
    'schema_version', 'policy_sha256', 'evidence_resolution_sha256',
    'approved_at_utc', 'approval_text_sha256'
  ];
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_context jsonb;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_scope_id uuid;
  v_scope_request_sha256 text;
  v_scope_proof_sha256 text;
  v_mapping jsonb;
  v_mapping_index jsonb;
  v_manifest jsonb;
  v_validation jsonb;
  v_protected_proof jsonb;
  v_universe_proof jsonb;
  v_source_universe jsonb;
  v_support_validation jsonb;
  v_mapping_count integer;
  v_support_count integer;
  v_process_count integer;
  v_rewrite_count integer;
  v_approved_reference_count integer;
  v_process_reference_mismatch_count integer;
  v_unlisted_reference_count integer;
  v_audit_id bigint;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;

  if p_request is null
    or pg_column_size(p_request) > 134217728
    or not private.dataset_flow_identity_exact_keys(p_request, v_top_keys)
    or p_request->>'schema_version' <> v_schema_version
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'target_visibility' <> 'owner_draft'
    or nullif(btrim(p_request->>'operation_id'), '') is null
    or octet_length(p_request->>'operation_id') > 512
    or not private.dataset_flow_identity_exact_keys(
      p_request->'actor', array['user_id', 'email']
    )
    or p_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(p_request #>> '{actor,email}'))
      is distinct from v_actor_email
    or not private.dataset_flow_identity_exact_keys(
      p_request->'compatibility_policy', v_policy_keys
    )
    or p_request #>> '{compatibility_policy,schema_version}'
      <> 'dataset-flow-identity-compatibility-policy.v1'
    or jsonb_typeof(p_request->'mappings') <> 'array'
    or jsonb_typeof(p_request->'processes') <> 'array'
    or jsonb_typeof(p_request->'support_snapshots') <> 'array'
    or jsonb_array_length(p_request->'mappings') not between 1 and 305
    or jsonb_array_length(p_request->'processes') not between 1 and 12000
    or jsonb_array_length(p_request->'support_snapshots') not between 2 and 100
    or p_request->>'source_universe_count' <> '305'
    or exists (
      select 1
      from unnest(array[
        'plan_sha256', 'freeze_sha256', 'approval_identity_sha256',
        'approval_text_sha256', 'toolchain_evidence_sha256',
        'support_snapshot_set_sha256', 'source_universe_sha256',
        'mapping_set_sha256', 'process_manifest_sha256',
        'protected_closure_sha256'
      ]) as hash_field(name)
      where p_request->>hash_field.name !~ '^[a-f0-9]{64}$'
    )
    or exists (
      select 1
      from unnest(array[
        'policy_sha256', 'evidence_resolution_sha256',
        'approval_text_sha256'
      ]) as hash_field(name)
      where p_request->'compatibility_policy'->>hash_field.name
        !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_INVALID_REQUEST',
      'status', 400, 'message', 'Step 3 preflight request schema mismatch'
    );
  end if;

  begin
    perform (p_request #>>
      '{compatibility_policy,approved_at_utc}')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_POLICY_TIME_INVALID',
      'status', 400, 'message', 'Compatibility policy approval time is invalid'
    );
  end;

  v_context := util.dataset_alias_execution_server_context();
  if p_request->>'environment' is distinct from v_context->>'environment'
    or p_request->>'project_ref' is distinct from v_context->>'project_ref' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_ENVIRONMENT_MISMATCH',
      'status', 409, 'message', 'Request does not target this database branch'
    );
  end if;

  if util.dataset_flow_identity_restricted_sha256_v2(p_request->'mappings')
      is distinct from p_request->>'mapping_set_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'support_snapshots'
    )
      is distinct from p_request->>'support_snapshot_set_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(p_request->'processes')
      is distinct from p_request->>'process_manifest_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'protected_closure'
    )
      is distinct from p_request->>'protected_closure_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_ARTIFACT_HASH_MISMATCH',
      'status', 409, 'message', 'A sealed artifact hash does not match'
    );
  end if;

  v_scope_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(p_request);
  select scope.*
  into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.actor_user_id = v_actor
    and scope.operation_id = p_request->>'operation_id';

  if v_scope.id is not null then
    if v_scope.scope_request_sha256 is distinct from v_scope_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_OPERATION_REUSE_MISMATCH',
        'status', 409, 'message', 'Operation ID is already sealed differently'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'command', v_command,
      'schema_version', v_result_schema_version,
      'scope_id', v_scope.id,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', v_scope.status,
      'process_count', v_scope.process_count,
      'mapping_count', v_scope.mapping_count,
      'rewrite_count', v_scope.rewrite_count,
      'next_ordinal', coalesce((
        select min(ledger.ordinal)
        from util.dataset_flow_identity_process_ledger as ledger
        where ledger.scope_id = v_scope.id and ledger.status = 'pending'
      ), v_scope.process_count + 1),
      'replay', true
    );
  end if;

  -- Validate and deterministically lock only actor-owned mutable rows.  Public
  -- targets/support remain optimistic hash guards and are rechecked by every
  -- process plus finalize.  No relation-level lock may block unrelated actors.
  v_support_count := jsonb_array_length(p_request->'support_snapshots');
  for v_mapping in
    select item.value
    from jsonb_array_elements(p_request->'support_snapshots')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_support_validation :=
      util.dataset_flow_identity_validate_support_snapshot(v_actor, v_mapping);
    if coalesce((v_support_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_SUPPORT_REJECTED',
        'status', 409, 'message', 'A sealed FP/UG support row is invalid',
        'details', v_support_validation
      );
    end if;
  end loop;

  if (
    select count(distinct (
      (item.value->>'table') || ':' || (item.value->>'id') || '@'
        || (item.value->>'version')
    )) = v_support_count
      and min((item.value->>'ordinal')::integer) = 1
      and max((item.value->>'ordinal')::integer) = v_support_count
      and bool_and(
        (item.value->>'ordinal')::integer = item.ordinality::integer
      )
    from jsonb_array_elements(p_request->'support_snapshots')
      with ordinality as item(value, ordinality)
  ) is not true or (
    with claimed as (
      select distinct 'flowproperties'::text as support_table,
        guard.value->>'flow_property_id' as id,
        guard.value->>'flow_property_version' as version
      from jsonb_array_elements(p_request->'mappings') as mapping(value)
      cross join lateral jsonb_array_elements(
        jsonb_build_array(mapping.value->'source', mapping.value->'target')
      ) as guard(value)
      union
      select distinct 'unitgroups',
        guard.value->>'unit_group_id', guard.value->>'unit_group_version'
      from jsonb_array_elements(p_request->'mappings') as mapping(value)
      cross join lateral jsonb_array_elements(
        jsonb_build_array(mapping.value->'source', mapping.value->'target')
      ) as guard(value)
    ), sealed as (
      select item.value->>'table' as support_table,
        item.value->>'id' as id, item.value->>'version' as version
      from jsonb_array_elements(p_request->'support_snapshots') as item(value)
    )
    select count(*) from (
      (select * from claimed except select * from sealed)
      union all
      (select * from sealed except select * from claimed)
    ) as difference
  ) <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_SUPPORT_SET_MISMATCH',
      'status', 409,
      'message', 'Support snapshots must exactly cover every claimed FP/UG'
    );
  end if;

  perform 1
  from public.flows as flow
  join (
    select (mapping.value #>> '{source,id}')::uuid as id,
      mapping.value #>> '{source,version}' as version
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
    union
    select (item.value->>'source_id')::uuid,
      item.value->>'source_version'
    from jsonb_array_elements(
      coalesce(p_request #> '{protected_closure,pending}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,blockers}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,orphans}', '[]'::jsonb)
    ) as item(value)
  ) as wanted
    on wanted.id = flow.id and wanted.version = btrim(flow.version::text)
  where flow.user_id = v_actor and flow.state_code = 0
  order by flow.id, btrim(flow.version::text)
  for share of flow;

  perform 1
  from public.flowproperties as support
  join jsonb_array_elements(p_request->'support_snapshots') as item(value)
    on item.value->>'table' = 'flowproperties'
    and (item.value->>'id')::uuid = support.id
    and item.value->>'version' = btrim(support.version::text)
  where support.user_id = v_actor and support.state_code = 0
  order by support.id, btrim(support.version::text)
  for share of support;

  perform 1
  from public.unitgroups as support
  join jsonb_array_elements(p_request->'support_snapshots') as item(value)
    on item.value->>'table' = 'unitgroups'
    and (item.value->>'id')::uuid = support.id
    and item.value->>'version' = btrim(support.version::text)
  where support.user_id = v_actor and support.state_code = 0
  order by support.id, btrim(support.version::text)
  for share of support;

  perform 1
  from public.processes as process
  join jsonb_array_elements(p_request->'processes') as item(value)
    on (item.value->>'id')::uuid = process.id
    and item.value->>'version' = btrim(process.version::text)
  where process.user_id = v_actor and process.state_code = 0
  order by process.id, btrim(process.version::text)
  for share of process;

  v_mapping_count := jsonb_array_length(p_request->'mappings');
  for v_mapping in
    select item.value
    from jsonb_array_elements(p_request->'mappings')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_validation := util.dataset_flow_identity_validate_mapping(
      v_actor,
      v_mapping,
      p_request->'compatibility_policy',
      p_request->'support_snapshots',
      (v_mapping->>'ordinal')::integer
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_MAPPING_REJECTED',
        'status', 409, 'message', 'A public-flow identity mapping is invalid',
        'details', v_validation
      );
    end if;
  end loop;

  if (
    select count(*)
    from (
      select distinct item.value->>'mapping_id'
      from jsonb_array_elements(p_request->'mappings') as item(value)
    ) as unique_mapping
  ) <> v_mapping_count
    or (
      select count(distinct (
        (item.value #>> '{source,id}') || '@'
          || (item.value #>> '{source,version}')
      ))
      from jsonb_array_elements(p_request->'mappings') as item(value)
    ) <> v_mapping_count
    or (
      select min((item.value->>'ordinal')::integer) = 1
        and max((item.value->>'ordinal')::integer) = v_mapping_count
        and count(distinct (item.value->>'ordinal')::integer) = v_mapping_count
        and bool_and(
          (item.value->>'ordinal')::integer = item.ordinality::integer
        )
      from jsonb_array_elements(p_request->'mappings')
        with ordinality as item(value, ordinality)
    ) is not true then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_MAPPING_ORDINAL_MISMATCH',
      'status', 409, 'message', 'Mapping ordinals must be unique and contiguous'
    );
  end if;
  v_mapping_index := private.dataset_flow_identity_mapping_index_v2(
    p_request->'mappings'
  );

  v_process_count := jsonb_array_length(p_request->'processes');
  v_rewrite_count := 0;
  for v_manifest in
    select item.value
    from jsonb_array_elements(p_request->'processes')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_validation := util.dataset_flow_identity_dry_validate_process(
      v_actor, v_manifest, v_mapping_index,
      p_request->'compatibility_policy', p_request->'support_snapshots'
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false
      or v_manifest->>'pending_blocker_closure_sha256'
        is distinct from p_request->>'protected_closure_sha256' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_PROCESS_REJECTED',
        'status', 409, 'message', 'An owner-draft process baseline is invalid',
        'details', v_validation
      );
    end if;
    v_rewrite_count := v_rewrite_count
      + (v_manifest->>'rewrite_count')::integer;
  end loop;

  if (
    select min((item.value->>'ordinal')::integer) = 1
      and max((item.value->>'ordinal')::integer) = v_process_count
      and count(distinct (item.value->>'ordinal')::integer) = v_process_count
      and count(distinct (
        (item.value->>'id') || '@' || (item.value->>'version')
      )) = v_process_count
      and bool_and(
        (item.value->>'ordinal')::integer = item.ordinality::integer
      )
    from jsonb_array_elements(p_request->'processes')
      with ordinality as item(value, ordinality)
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_PROCESS_ORDINAL_MISMATCH',
      'status', 409, 'message', 'Process ordinals and identities must be unique'
    );
  end if;

  -- Prove that the compact process manifest is a complete closure of every
  -- currently approved source reference.  Exact locators remain sealed by
  -- each rewrite_set_sha256 and are independently checked by the process RPC,
  -- while these set-based counts reject omitted or phantom processes before
  -- the first business mutation can occur.
  with source_keys as (
    select
      mapping.value #>> '{source,id}' as source_id,
      mapping.value #>> '{source,version}' as source_version
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
  ), manifest as (
    select
      (item.value->>'id')::uuid as process_id,
      item.value->>'version' as process_version,
      (item.value->>'rewrite_count')::integer as expected_count
    from jsonb_array_elements(p_request->'processes') as item(value)
  ), approved_occurrences as (
    select
      process.id as process_id,
      btrim(process.version::text) as process_version
    from public.processes as process
    cross join lateral jsonb_array_elements(
      private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
    ) as exchange(value)
    join source_keys
      on source_keys.source_id = exchange.value #>>
          '{referenceToFlowDataSet,@refObjectId}'
      and source_keys.source_version = exchange.value #>>
          '{referenceToFlowDataSet,@version}'
    where process.user_id = v_actor and process.state_code = 0
  ), live_counts as (
    select
      manifest.process_id,
      manifest.process_version,
      manifest.expected_count,
      count(approved_occurrences.process_id)::integer as observed_count
    from manifest
    left join approved_occurrences using (process_id, process_version)
    group by
      manifest.process_id, manifest.process_version, manifest.expected_count
  )
  select
    (select count(*)::integer from approved_occurrences),
    (select count(*)::integer from live_counts
      where expected_count <> observed_count),
    (select count(*)::integer
      from approved_occurrences
      left join manifest using (process_id, process_version)
      where manifest.process_id is null)
  into
    v_approved_reference_count,
    v_process_reference_mismatch_count,
    v_unlisted_reference_count;

  if v_approved_reference_count <> v_rewrite_count
    or v_process_reference_mismatch_count <> 0
    or v_unlisted_reference_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_APPROVED_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Approved source references are omitted or mispartitioned',
      'approved_reference_count', v_approved_reference_count,
      'sealed_rewrite_count', v_rewrite_count,
      'process_mismatch_count', v_process_reference_mismatch_count,
      'unlisted_reference_count', v_unlisted_reference_count
    );
  end if;

  v_protected_proof := util.dataset_flow_identity_protected_closure(
    v_actor, p_request->'protected_closure'
  );
  if coalesce((v_protected_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_PROTECTED_CLOSURE_MISMATCH',
      'status', 409, 'message', 'Pending/blocker occurrence closure drifted',
      'details', v_protected_proof
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
    join jsonb_array_elements(
      coalesce(p_request #> '{protected_closure,pending}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,blockers}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,orphans}', '[]'::jsonb)
    ) as protected(value)
      on protected.value->>'source_id' = mapping.value #>> '{source,id}'
      and protected.value->>'source_version'
        = mapping.value #>> '{source,version}'
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_PARTITION_OVERLAP',
      'status', 409, 'message', 'Approved and protected source partitions overlap'
    );
  end if;

  with universe_entry as (
    select mapping.value #>> '{source,id}' as id,
      mapping.value #>> '{source,version}' as version
    from jsonb_array_elements(p_request->'mappings') as mapping(value)
    union all
    select item.value->>'source_id', item.value->>'source_version'
    from jsonb_array_elements(
      coalesce(p_request #> '{protected_closure,pending}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,blockers}', '[]'::jsonb)
      || coalesce(p_request #> '{protected_closure,orphans}', '[]'::jsonb)
    ) as item(value)
  ), universe_count as (
    select count(*)::integer as raw_count,
      count(distinct id || '@' || version)::integer as distinct_count
    from universe_entry
  )
  select case
    when universe_count.raw_count = 305
      and universe_count.distinct_count = 305 then (
        select jsonb_agg(jsonb_build_object(
          'id', entry.id::uuid,
          'version', entry.version,
          'user_id', v_actor,
          'state_code', 0,
          'flow_type', 'Elementary flow'
        ) order by entry.id::uuid, entry.version)
        from universe_entry as entry
      )
    else null
  end
  into v_source_universe
  from universe_count;

  if v_source_universe is null
    or util.dataset_flow_identity_sha256(v_source_universe)
      is distinct from p_request->>'source_universe_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_SOURCE_UNIVERSE_MISMATCH',
      'status', 409,
      'message', 'Mappings and protected partitions must form exactly 305 distinct sources'
    );
  end if;
  v_universe_proof := util.dataset_flow_identity_source_universe(
    v_actor, v_source_universe, p_request->>'source_universe_sha256'
  );
  if coalesce((v_universe_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_SOURCE_UNIVERSE_LIVE_MISMATCH',
      'status', 409,
      'message', 'The live actor-owned elementary-flow universe is not exact',
      'details', v_universe_proof
    );
  end if;

  v_scope_id := gen_random_uuid();
  v_scope_proof_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    jsonb_build_object(
      'schema_version', 'dataset-flow-identity-scope-proof.v1',
      'scope_id', v_scope_id,
      'actor_user_id', v_actor,
      'environment', p_request->>'environment',
      'project_ref', p_request->>'project_ref',
      'operation_id', p_request->>'operation_id',
      'plan_sha256', p_request->>'plan_sha256',
      'freeze_sha256', p_request->>'freeze_sha256',
      'approval_identity_sha256', p_request->>'approval_identity_sha256',
      'support_snapshot_set_sha256',
        p_request->>'support_snapshot_set_sha256',
      'source_universe_sha256', p_request->>'source_universe_sha256',
      'mapping_set_sha256', p_request->>'mapping_set_sha256',
      'process_manifest_sha256', p_request->>'process_manifest_sha256',
      'protected_closure_sha256', p_request->>'protected_closure_sha256',
      'scope_request_sha256', v_scope_request_sha256
    )
  );

  insert into util.dataset_flow_identity_scopes (
    id, actor_user_id, actor_email, request_id, environment, project_ref,
    target_visibility, operation_id, plan_sha256, freeze_sha256,
    approval_identity_sha256, approval_text_sha256,
    toolchain_evidence_sha256, compatibility_policy,
    support_snapshot_set_sha256, support_snapshots,
    source_universe_sha256, source_universe, source_universe_count,
    mapping_set_sha256,
    process_manifest_sha256, protected_closure_sha256, protected_closure,
    scope_request_sha256, scope_proof_sha256, mapping_count, process_count,
    rewrite_count
  ) values (
    v_scope_id, v_actor, v_actor_email, (p_request->>'request_id')::uuid,
    p_request->>'environment', p_request->>'project_ref',
    p_request->>'target_visibility', p_request->>'operation_id',
    p_request->>'plan_sha256', p_request->>'freeze_sha256',
    p_request->>'approval_identity_sha256', p_request->>'approval_text_sha256',
    p_request->>'toolchain_evidence_sha256',
    p_request->'compatibility_policy',
    p_request->>'support_snapshot_set_sha256',
    p_request->'support_snapshots',
    p_request->>'source_universe_sha256', v_source_universe, 305,
    p_request->>'mapping_set_sha256',
    p_request->>'process_manifest_sha256',
    p_request->>'protected_closure_sha256', p_request->'protected_closure',
    v_scope_request_sha256, v_scope_proof_sha256, v_mapping_count,
    v_process_count, v_rewrite_count
  );

  insert into util.dataset_flow_identity_mappings (
    scope_id, ordinal, mapping_id, source_id, source_version,
    target_id, target_version, mapping
  )
  select
    v_scope_id,
    (item.value->>'ordinal')::integer,
    item.value->>'mapping_id',
    (item.value #>> '{source,id}')::uuid,
    item.value #>> '{source,version}',
    (item.value #>> '{target,id}')::uuid,
    item.value #>> '{target,version}',
    item.value
  from jsonb_array_elements(p_request->'mappings') as item(value);

  insert into util.dataset_flow_identity_process_ledger (
    scope_id, ordinal, process_id, process_version, manifest,
    process_template_sha256, process_intent_proof_sha256,
    rewrite_count, before_payload_sha256
  )
  select
    v_scope_id,
    (item.value->>'ordinal')::integer,
    (item.value->>'id')::uuid,
    item.value->>'version',
    item.value,
    item.value->>'process_template_sha256',
    util.dataset_flow_identity_restricted_sha256_v2(item.value),
    (item.value->>'rewrite_count')::integer,
    item.value->>'before_payload_sha256'
  from jsonb_array_elements(p_request->'processes') as item(value);

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null,
    jsonb_build_object(
      'record_type', 'scope_seal',
      'schema_version', v_schema_version,
      'scope_id', v_scope_id,
      'operation_id', p_request->>'operation_id',
      'plan_sha256', p_request->>'plan_sha256',
      'freeze_sha256', p_request->>'freeze_sha256',
      'approval_identity_sha256', p_request->>'approval_identity_sha256',
      'support_snapshot_set_sha256',
        p_request->>'support_snapshot_set_sha256',
      'source_universe_sha256', p_request->>'source_universe_sha256',
      'source_universe_count', 305,
      'scope_request_sha256', v_scope_request_sha256,
      'scope_proof_sha256', v_scope_proof_sha256,
      'mapping_count', v_mapping_count,
      'process_count', v_process_count,
      'rewrite_count', v_rewrite_count,
      'protected_closure_proof', v_protected_proof,
      'source_universe_proof', v_universe_proof,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_audit_id;

  return jsonb_build_object(
    'ok', true,
    'command', v_command,
    'schema_version', v_result_schema_version,
    'scope_id', v_scope_id,
    'operation_id', p_request->>'operation_id',
    'plan_sha256', p_request->>'plan_sha256',
    'scope_proof_sha256', v_scope_proof_sha256,
    'status', 'sealed',
    'process_count', v_process_count,
    'mapping_count', v_mapping_count,
    'support_snapshot_count', v_support_count,
    'source_universe_count', 305,
    'rewrite_count', v_rewrite_count,
    'next_ordinal', 1,
    'audit_id', v_audit_id::text,
    'replay', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_LOCK_BUSY', 'status', 409,
      'message', 'Scope seal could not acquire its bounded validation locks'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_CONCURRENT_CONFLICT', 'status', 409,
      'message', 'Another active scope already owns an exact process or plan'
    );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)
  to authenticated;

comment on function public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb) is
  'Actor-only Step 3 scope seal. It performs a no-business-write full validation of exact owner-draft process baselines, exact non-owner public Elementary targets, compatibility evidence, and pending/blocker occurrence closure, then persists only private scope/ledger metadata.';

create or replace function private.dataset_flow_identity_collision_ledger(
  p_exchanges jsonb,
  p_rewrites jsonb
) returns jsonb
language sql
stable
strict
set search_path = ''
as $$
  with touched as (
    select distinct
      rewrite.value #>> '{target_reference,@refObjectId}' as target_id,
      rewrite.value #>> '{target_reference,@version}' as target_version
    from jsonb_array_elements(p_rewrites) as rewrite(value)
  ), matching as (
    select
      touched.target_id,
      touched.target_version,
      (exchange.ordinality - 1)::integer as exchange_index,
      exchange.value->>'@dataSetInternalID' as internal_id,
      rewrite.value->>'mapping_id' as mapping_id
    from touched
    join lateral jsonb_array_elements(p_exchanges)
      with ordinality as exchange(value, ordinality)
      on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
          = touched.target_id
        and exchange.value #>> '{referenceToFlowDataSet,@version}'
          = touched.target_version
    left join lateral (
      select candidate.value
      from jsonb_array_elements(p_rewrites) as candidate(value)
      where (candidate.value->>'exchange_index')::integer
        = exchange.ordinality - 1
      limit 1
    ) as rewrite on true
  ), grouped as (
    select
      matching.target_id,
      matching.target_version,
      count(*)::integer as multiplicity,
      jsonb_agg(matching.exchange_index order by matching.exchange_index)
        as exchange_indexes,
      jsonb_agg(matching.internal_id order by matching.exchange_index)
        as internal_ids,
      jsonb_agg(to_jsonb(matching.mapping_id)
        order by matching.exchange_index) as mapping_ids
    from matching
    group by matching.target_id, matching.target_version
    having count(*) > 1
  )
  select jsonb_build_object(
    'schema_version', 'dataset-flow-identity-collision-ledger.v1',
    'entries', coalesce(jsonb_agg(jsonb_build_object(
      'target_id', grouped.target_id,
      'target_version', grouped.target_version,
      'exchange_indexes', grouped.exchange_indexes,
      'internal_ids', grouped.internal_ids,
      'mapping_ids', grouped.mapping_ids,
      'preserve_rows', true
    ) order by grouped.target_id, grouped.target_version), '[]'::jsonb)
  )
  from grouped
$$;

alter function private.dataset_flow_identity_collision_ledger(jsonb, jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_collision_ledger(jsonb, jsonb)
  from public, anon, authenticated, service_role;

create or replace function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  p_scope_id uuid,
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '60s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_process_rewrite_guarded';
  v_schema_version constant text :=
    'dataset-flow-identity-process-rewrite.v1';
  v_result_schema_version constant text :=
    'dataset-flow-identity-process-rewrite-result.v1';
  v_top_keys constant text[] := array[
    'schema_version', 'request_id', 'scope_proof_sha256', 'ordinal',
    'process', 'rewrites', 'collision_ledger', 'collision_ledger_sha256',
    'process_request_sha256'
  ];
  v_rewrite_keys constant text[] := array[
    'ordinal', 'exchange_index', 'internal_id', 'direction', 'mapping_id',
    'source_reference', 'target_reference', 'before_reference_sha256',
    'after_reference_sha256'
  ];
  v_reference_keys constant text[] := array[
    '@refObjectId', '@type', '@uri', '@version',
    'common:shortDescription'
  ];
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_ledger util.dataset_flow_identity_process_ledger%rowtype;
  v_process public.processes%rowtype;
  v_committed public.processes%rowtype;
  v_mapping util.dataset_flow_identity_mappings%rowtype;
  v_rewrite jsonb;
  v_mapping_validation jsonb;
  v_process_validation jsonb;
  v_before_payload jsonb;
  v_desired_payload jsonb;
  v_before_exchanges jsonb;
  v_after_exchanges jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_before_reference jsonb;
  v_after_reference jsonb;
  v_collision_ledger jsonb;
  v_internal_request_sha256 text;
  v_request_sha256 text;
  v_before_payload_sha256 text;
  v_after_payload_sha256 text;
  v_after_exchange_sha256 text;
  v_baseline_snapshot jsonb;
  v_after_snapshot jsonb;
  v_derivative_targets jsonb;
  v_derivative_result jsonb;
  v_derivative_batch_id uuid;
  v_derivative_reason_code text;
  v_audit_id bigint;
  v_next_ordinal integer;
  v_remaining integer;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;

  if p_scope_id is null
    or p_request is null
    or pg_column_size(p_request) > 8388608
    or not private.dataset_flow_identity_exact_keys(p_request, v_top_keys)
    or p_request->>'schema_version' <> v_schema_version
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or p_request->>'process_request_sha256' !~ '^[a-f0-9]{64}$'
    or p_request->>'collision_ledger_sha256' !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_request->'ordinal') <> 'number'
    or (p_request->>'ordinal')::integer <= 0
    or jsonb_typeof(p_request->'rewrites') <> 'array'
    or jsonb_array_length(p_request->'rewrites') <= 0
    or jsonb_array_length(p_request->'rewrites') > 10000 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_INVALID_REQUEST',
      'status', 400, 'message', 'Step 3 process request schema mismatch'
    );
  end if;

  v_internal_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    p_request - 'process_request_sha256'
  );
  if v_internal_request_sha256 is distinct from
    p_request->>'process_request_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'collision_ledger'
    )
      is distinct from p_request->>'collision_ledger_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REQUEST_HASH_MISMATCH',
      'status', 409, 'message', 'Process or collision request hash mismatch'
    );
  end if;
  v_request_sha256 := coalesce(nullif(current_setting(
    'app.dataset_flow_identity_v2_process_request_sha256', true
  ), ''), v_internal_request_sha256);
  if v_request_sha256 !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REQUEST_HASH_MISMATCH',
      'status', 409, 'message', 'External v2 request hash is invalid'
    );
  end if;

  if not pg_try_advisory_xact_lock(
    hashtextextended('dataset-flow-identity:' || p_scope_id::text, 0)
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_SCOPE_BUSY', 'status', 409,
      'message', 'Another process transaction currently owns this scope'
    );
  end if;

  select scope.*
  into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id
    and scope.actor_user_id = v_actor
  for update;

  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  if v_scope.scope_proof_sha256 is distinct from
      p_request->>'scope_proof_sha256'
    or v_scope.status in ('failed', 'cancelled') then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Scope proof is invalid or scope is failed'
    );
  end if;

  select ledger.*
  into v_ledger
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = p_scope_id
    and ledger.ordinal = (p_request->>'ordinal')::integer
  for update;

  if v_ledger.scope_id is null
    or p_request->'process' is distinct from v_ledger.manifest
    or p_request->'rewrites' is distinct from v_ledger.manifest->'rewrites'
    or p_request->'collision_ledger'
      is distinct from v_ledger.manifest->'collision_ledger'
    or p_request #>> '{process,process_template_sha256}'
      is distinct from v_ledger.process_template_sha256 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_MISMATCH', 'status', 409,
      'message', 'Request does not match the sealed process manifest'
    );
  end if;

  if v_ledger.status = 'completed' then
    select process.*
    into v_committed
    from public.processes as process
    where process.id = v_ledger.process_id
      and btrim(process.version::text) = v_ledger.process_version
      and process.user_id = v_actor
      and process.state_code = 0;
    if v_ledger.process_request_sha256 is distinct from v_request_sha256
      or v_committed.id is null
      or util.dataset_flow_identity_sha256(v_committed.json_ordered::jsonb)
        is distinct from v_ledger.after_payload_sha256
      or util.dataset_flow_identity_sha256(
        private.dataset_flow_identity_exchanges(v_committed.json_ordered::jsonb)
      ) is distinct from v_ledger.after_exchange_set_sha256
      or not exists (
        select 1 from public.command_audit_log as audit
        where audit.id = v_ledger.audit_id
          and audit.actor_user_id = v_actor
          and audit.command = v_command
          and audit.payload->>'process_request_sha256' = v_request_sha256
          and audit.payload->>'derivative_batch_id'
            = v_ledger.derivative_batch_id::text
          and audit.payload->>'derivative_reason_code'
            = 'FLOW_IDENTITY_SCOPE:' || p_scope_id::text || ':'
              || v_ledger.ordinal::text
      )
      or not exists (
        select 1
        from util.dataset_derivative_rebuild_requests as child
        where child.actor_user_id = v_actor
          and child.batch_id = v_ledger.derivative_batch_id
          and child.target_table = 'processes'
          and child.target_id = v_ledger.process_id
          and child.target_version = v_ledger.process_version
          and child.reason_code = 'FLOW_IDENTITY_SCOPE:'
            || p_scope_id::text || ':' || v_ledger.ordinal::text
          and child.expected_json_ordered_sha256
            = util.dataset_derivative_rebuild_sha256(
              v_committed.json_ordered::jsonb::text
            )
      ) then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_REPLAY_PROOF_MISMATCH', 'status', 409,
        'message', 'Completed process no longer has exact audit/live proof'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', v_result_schema_version,
      'scope_id', p_scope_id, 'ordinal', v_ledger.ordinal,
      'process_id', v_ledger.process_id,
      'process_version', v_ledger.process_version,
      'process_request_sha256', v_request_sha256,
      'before_payload_sha256', v_ledger.before_payload_sha256,
      'after_payload_sha256', v_ledger.after_payload_sha256,
      'rewrite_count', v_ledger.rewrite_count,
      'audit_id', v_ledger.audit_id::text,
      'derivative_batch_id', v_ledger.derivative_batch_id,
      'status', 'completed', 'replay', true
    );
  elsif v_ledger.status <> 'pending' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_NOT_EXECUTABLE', 'status', 409,
      'message', 'Process ledger is not pending or exactly replayable'
    );
  end if;

  select min(ledger.ordinal)
  into v_next_ordinal
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = p_scope_id and ledger.status = 'pending';
  if v_next_ordinal is distinct from v_ledger.ordinal then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_ORDINAL_MISMATCH', 'status', 409,
      'message', 'Processes must execute in sealed ordinal order',
      'next_ordinal', v_next_ordinal
    );
  end if;

  if jsonb_array_length(p_request->'rewrites') <> v_ledger.rewrite_count
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_request->'rewrites'
    )
      is distinct from v_ledger.manifest->>'rewrite_set_sha256'
    or p_request->>'collision_ledger_sha256'
      is distinct from v_ledger.manifest->>'collision_ledger_sha256'
    or (
      select min((rewrite.value->>'ordinal')::integer) = 1
        and max((rewrite.value->>'ordinal')::integer) = v_ledger.rewrite_count
        and count(distinct (rewrite.value->>'ordinal')::integer)
          = v_ledger.rewrite_count
        and count(distinct (rewrite.value->>'exchange_index')::integer)
          = v_ledger.rewrite_count
      from jsonb_array_elements(p_request->'rewrites') as rewrite(value)
    ) is not true then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REWRITE_SET_MISMATCH', 'status', 409,
      'message', 'Rewrite set does not match the sealed manifest'
    );
  end if;

  v_process_validation := util.dataset_flow_identity_validate_process_guard(
    v_actor, v_ledger.manifest
  );
  if coalesce((v_process_validation->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_BASELINE_DRIFT', 'status', 409,
      'message', 'Owner-draft process drifted after scope seal',
      'details', v_process_validation
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = v_ledger.process_id
    and btrim(process.version::text) = v_ledger.process_version
    and process.user_id = v_actor
    and process.state_code = 0
  for update;

  v_before_payload := v_process.json_ordered::jsonb;
  v_before_exchanges := private.dataset_flow_identity_exchanges(v_before_payload);
  v_after_exchanges := v_before_exchanges;
  v_before_payload_sha256 := util.dataset_flow_identity_sha256(v_before_payload);
  begin
    v_baseline_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception when others then
    v_baseline_snapshot := null;
  end;
  if v_before_payload_sha256 is distinct from
      v_ledger.manifest->>'before_payload_sha256'
    or v_baseline_snapshot is null
    or v_baseline_snapshot->>'snapshot_sha256' is distinct from
      v_ledger.manifest->>'derivative_baseline_snapshot_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_LOCKED_BASELINE_DRIFT', 'status', 409,
      'message', 'Locked process no longer matches its baseline'
    );
  end if;

  for v_rewrite in
    select rewrite.value
    from jsonb_array_elements(p_request->'rewrites')
      with ordinality as rewrite(value, ordinality)
    order by rewrite.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(
        v_rewrite, v_rewrite_keys
      )
      or jsonb_typeof(v_rewrite->'ordinal') <> 'number'
      or jsonb_typeof(v_rewrite->'exchange_index') <> 'number'
      or (v_rewrite->>'exchange_index')::integer < 0
      or (v_rewrite->>'exchange_index')::integer
        >= jsonb_array_length(v_before_exchanges)
      or nullif(v_rewrite->>'internal_id', '') is null
      or v_rewrite->>'direction' not in ('Input', 'Output')
      or v_rewrite->>'mapping_id' !~ '^[a-f0-9]{64}$'
      or v_rewrite->>'before_reference_sha256' !~ '^[a-f0-9]{64}$'
      or v_rewrite->>'after_reference_sha256' !~ '^[a-f0-9]{64}$'
      or not private.dataset_flow_identity_exact_keys(
        v_rewrite->'source_reference', v_reference_keys
      )
      or not private.dataset_flow_identity_exact_keys(
        v_rewrite->'target_reference', v_reference_keys
      ) then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_REWRITE_SCHEMA_MISMATCH', 'status', 400,
        'message', 'A rewrite locator or reference is invalid'
      );
    end if;

    select mapping.*
    into v_mapping
    from util.dataset_flow_identity_mappings as mapping
    where mapping.scope_id = p_scope_id
      and mapping.mapping_id = v_rewrite->>'mapping_id';
    if v_mapping.scope_id is null then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_MAPPING_NOT_SEALED', 'status', 409,
        'message', 'Rewrite mapping is outside the sealed scope'
      );
    end if;

    v_mapping_validation := util.dataset_flow_identity_validate_mapping(
      v_actor, v_mapping.mapping, v_scope.compatibility_policy,
      v_scope.support_snapshots,
      v_mapping.ordinal
    );
    if coalesce((v_mapping_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_MAPPING_DRIFT', 'status', 409,
        'message', 'A source/public/support mapping guard drifted',
        'details', v_mapping_validation
      );
    end if;

    v_before_exchange := v_after_exchanges ->
      (v_rewrite->>'exchange_index')::integer;
    v_before_reference := private.dataset_flow_identity_reference(
      v_before_exchange
    );
    if v_before_exchange->>'@dataSetInternalID'
        is distinct from v_rewrite->>'internal_id'
      or v_before_exchange->>'exchangeDirection'
        is distinct from v_rewrite->>'direction'
      or v_before_reference is distinct from v_rewrite->'source_reference'
      or util.dataset_flow_identity_sha256(v_before_reference)
        is distinct from v_rewrite->>'before_reference_sha256'
      or v_before_reference->>'@refObjectId'
        is distinct from v_mapping.source_id::text
      or v_before_reference->>'@version'
        is distinct from v_mapping.source_version
      or v_rewrite->'target_reference'
        is distinct from v_mapping.mapping #> '{target,reference}'
      or util.dataset_flow_identity_sha256(v_rewrite->'target_reference')
        is distinct from v_rewrite->>'after_reference_sha256' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_EXCHANGE_LOCATOR_DRIFT', 'status', 409,
        'message', 'An exchange locator or exact reference drifted'
      );
    end if;

    v_after_exchange := jsonb_set(
      v_before_exchange,
      '{referenceToFlowDataSet}',
      (v_before_exchange->'referenceToFlowDataSet')
        || v_rewrite->'target_reference',
      false
    );
    v_after_reference := private.dataset_flow_identity_reference(
      v_after_exchange
    );
    if v_after_reference is distinct from v_rewrite->'target_reference'
      or v_after_exchange - 'referenceToFlowDataSet'
        is distinct from v_before_exchange - 'referenceToFlowDataSet'
      or (v_after_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription'
        is distinct from
        (v_before_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PROCESS_FIVE_FIELD_BOUNDARY_FAILED',
        'status', 409,
        'message', 'Rewrite would change a non-approved exchange field'
      );
    end if;

    v_after_exchanges := jsonb_set(
      v_after_exchanges,
      array[(v_rewrite->>'exchange_index')::integer::text],
      v_after_exchange,
      false
    );
  end loop;

  v_collision_ledger := private.dataset_flow_identity_collision_ledger(
    v_after_exchanges, p_request->'rewrites'
  );
  if v_collision_ledger is distinct from p_request->'collision_ledger' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_COLLISION_LEDGER_MISMATCH', 'status', 409,
      'message', 'Post-rewrite collision ledger is not exact'
    );
  end if;

  v_desired_payload := private.dataset_flow_identity_replace_exchanges(
    v_before_payload, v_after_exchanges
  );
  v_after_payload_sha256 := util.dataset_flow_identity_sha256(v_desired_payload);
  v_after_exchange_sha256 := util.dataset_flow_identity_sha256(v_after_exchanges);
  if v_desired_payload is null
    or jsonb_array_length(v_before_exchanges)
      <> jsonb_array_length(v_after_exchanges)
    or v_after_payload_sha256 is distinct from
      v_ledger.manifest->>'desired_payload_sha256'
    or v_after_exchange_sha256 is distinct from
      v_ledger.manifest->>'desired_exchange_set_sha256'
    or exists (
      select 1
      from util.dataset_flow_identity_mappings as mapping
      join lateral jsonb_array_elements(v_after_exchanges) as exchange(value)
        on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
          = mapping.source_id::text
        and exchange.value #>> '{referenceToFlowDataSet,@version}'
          = mapping.source_version
      where mapping.scope_id = p_scope_id
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_DESIRED_CLOSURE_MISMATCH', 'status', 409,
      'message', 'Server-reconstructed desired process does not match the seal'
    );
  end if;

  insert into util.dataset_flow_identity_mutation_permits (
    transaction_id, scope_id, ordinal, process_id, process_version,
    mutation_nonce, before_payload_sha256, after_payload_sha256
  ) values (
    txid_current(), p_scope_id, v_ledger.ordinal, v_process.id,
    btrim(v_process.version::text), v_ledger.mutation_nonce,
    v_before_payload_sha256, v_after_payload_sha256
  );

  update public.processes
  set json_ordered = v_desired_payload::json
  where id = v_process.id
    and btrim(version::text) = btrim(v_process.version::text)
    and user_id = v_actor
    and state_code = 0
    and modified_at = v_process.modified_at
  returning * into v_committed;

  if v_committed.id is null then
    raise exception using
      errcode = '40001',
      message = 'Step 3 locked process update precondition was lost';
  end if;
  begin
    v_after_snapshot := util.dataset_derivative_rebuild_snapshot(v_committed);
  exception when others then
    v_after_snapshot := null;
  end;
  if v_after_snapshot is null
    or v_after_snapshot->>'json_sha256'
      is distinct from v_after_snapshot->>'json_ordered_sha256'
    or util.dataset_flow_identity_sha256(v_committed.json_ordered::jsonb)
      is distinct from v_after_payload_sha256 then
    raise exception using
      errcode = 'P0001',
      message = 'Step 3 committed process primary hash mismatch';
  end if;

  -- Generate and bind the protected derivative batch before the primary audit.
  -- The audit, request ledger, and admission therefore share one exact causal
  -- identifier even though the admission itself happens after the audit row.
  v_derivative_batch_id := gen_random_uuid();
  v_derivative_reason_code := 'FLOW_IDENTITY_SCOPE:'
    || p_scope_id::text || ':' || v_ledger.ordinal::text;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, 'processes', v_process.id,
    btrim(v_process.version::text),
    jsonb_build_object(
      'record_type', 'process_rewrite',
      'schema_version', v_schema_version,
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'ordinal', v_ledger.ordinal,
      'process_request_sha256', v_request_sha256,
      'process_template_sha256', v_ledger.process_template_sha256,
      'rewrite_set_sha256', v_ledger.manifest->>'rewrite_set_sha256',
      'collision_ledger_sha256',
        v_ledger.manifest->>'collision_ledger_sha256',
      'before_payload_sha256', v_before_payload_sha256,
      'after_payload_sha256', v_after_payload_sha256,
      'after_exchange_set_sha256', v_after_exchange_sha256,
      'rewrite_count', v_ledger.rewrite_count,
      'derivative_batch_id', v_derivative_batch_id,
      'derivative_reason_code', v_derivative_reason_code,
      'committed_modified_at', v_committed.modified_at,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_audit_id;

  v_derivative_targets := jsonb_build_array(jsonb_build_object(
    'table', 'processes',
    'id', v_committed.id,
    'version', btrim(v_committed.version::text),
    'expected_json_ordered_sha256',
      v_after_snapshot->>'json_ordered_sha256',
    'baseline_snapshot_sha256',
      v_ledger.manifest->>'derivative_baseline_snapshot_sha256'
  ));
  v_derivative_result := util.admit_dataset_derivative_rebuild_batch(
    v_actor,
    v_derivative_batch_id,
    v_scope.plan_sha256,
    v_scope.operation_id,
    v_derivative_reason_code,
    v_derivative_targets
  );
  if coalesce((v_derivative_result->>'ok')::boolean, false) is false
    or v_derivative_result->>'target_count' is distinct from '1'
    or v_derivative_result->>'process_count' is distinct from '1' then
    raise exception using
      errcode = 'P0001',
      message = 'Step 3 derivative admission mismatch';
  end if;

  update util.dataset_flow_identity_process_ledger
  set
    status = 'completed',
    process_request_sha256 = v_request_sha256,
    audit_id = v_audit_id,
    after_payload_sha256 = v_after_payload_sha256,
    after_exchange_set_sha256 = v_after_exchange_sha256,
    derivative_batch_id = v_derivative_batch_id,
    derivative_admission = v_derivative_result,
    completed_at = clock_timestamp()
  where scope_id = p_scope_id and ordinal = v_ledger.ordinal;

  -- Ordinals execute strictly in order, so after committing ordinal N the
  -- remaining primary count is exact without rescanning the whole ledger.
  v_remaining := v_scope.process_count - v_ledger.ordinal;
  update util.dataset_flow_identity_scopes
  set
    status = case when v_remaining = 0
      then 'derivatives_pending' else 'running' end,
    primary_completed_at = case when v_remaining = 0
      then clock_timestamp() else primary_completed_at end,
    updated_at = clock_timestamp()
  where id = p_scope_id;

  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', v_result_schema_version,
    'scope_id', p_scope_id, 'ordinal', v_ledger.ordinal,
    'process_id', v_process.id,
    'process_version', btrim(v_process.version::text),
    'process_request_sha256', v_request_sha256,
    'before_payload_sha256', v_before_payload_sha256,
    'after_payload_sha256', v_after_payload_sha256,
    'rewrite_count', v_ledger.rewrite_count,
    'audit_id', v_audit_id::text,
    'derivative_batch_id', v_derivative_batch_id,
    'status', 'completed', 'replay', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_LOCK_BUSY', 'status', 409,
      'message', 'Process transaction could not acquire its bounded lock'
    );
  when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_TRANSACTION_FAILED', 'status', 409,
      'message', sqlerrm, 'sqlstate', sqlstate,
      'primary_rolled_back', true, 'automatic_retry', false
    );
end;
$$;

alter function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb
) owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb
) to authenticated;


comment on function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb
) is
  'Actor-only, scope-serialized Step 3 transaction. It reconstructs one desired process by changing exactly five flow-reference fields at exact exchange indexes, preserves collision rows, records one unique audit, and admits one protected derivative rebuild atomically. Exact replay is proof-only; failed/lost mutations are never blindly retried.';

create or replace function util.read_dataset_derivative_rebuild_batch_any(
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
  v_target_count integer;
  v_flow_count integer;
  v_process_count integer;
  v_completed_count integer;
  v_nonterminal_count integer;
  v_failed_count integer;
  v_distinct_ordinal_count integer;
  v_distinct_target_count integer;
  v_distinct_plan_count integer;
  v_distinct_summary_count integer;
  v_distinct_declared_count integer;
  v_declared_count integer;
  v_min_ordinal integer;
  v_max_ordinal integer;
  v_invalid_proof_count integer := 0;
  v_primary_ok boolean;
  v_snapshot_ok boolean;
  v_derivative_fresh boolean;
  v_lifecycle_ok boolean;
  v_proposals_ok boolean;
  v_terminal_audit_ok boolean;
  v_residue jsonb;
  v_target_ok boolean;
  v_status text;
  v_code text;
  v_batch_rows integer;
  v_single_rows integer;
  v_is_batch boolean;
begin
  if p_actor_user_id is null or p_batch_id is null then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_READ_INVALID_REQUEST',
      'causal_terminal_proof', false
    );
  end if;

  -- A Step 3 primary transaction always records a protected batch id.  If
  -- that child later reaches a terminal failed/stale state, a separately
  -- frozen and approved derivative-only compensation is admitted through the
  -- retained single-target RPC and is identified by its request id.  This
  -- reader accepts either exact identity, never a loose target lookup.
  select
    count(*) filter (where request.batch_id = p_batch_id)::integer,
    count(*) filter (
      where request.batch_id is null and request.id = p_batch_id
    )::integer
  into v_batch_rows, v_single_rows
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = p_actor_user_id
    and (request.batch_id = p_batch_id or request.id = p_batch_id);

  if (v_batch_rows > 0 and v_single_rows > 0)
    or (v_batch_rows = 0 and v_single_rows <> 1) then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'reference_id', p_batch_id,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_REFERENCE_AMBIGUOUS',
      'causal_terminal_proof', false
    );
  end if;
  v_is_batch := v_batch_rows > 0;

  select
    count(*)::integer,
    count(*) filter (where target_table = 'flows')::integer,
    count(*) filter (where target_table = 'processes')::integer,
    count(*) filter (where status = 'completed')::integer,
    count(*) filter (where status not in ('completed', 'stale', 'failed'))::integer,
    count(*) filter (where status in ('stale', 'failed'))::integer,
    count(distinct coalesce(batch_ordinal, 1))::integer,
    count(distinct (target_table || ':' || target_id::text || '@' || target_version))::integer,
    count(distinct plan_request_sha256)::integer,
    count(distinct summary_audit_id)::integer,
    count(distinct coalesce(batch_target_count, 1))::integer,
    min(coalesce(batch_target_count, 1))::integer,
    min(coalesce(batch_ordinal, 1))::integer,
    max(coalesce(batch_ordinal, 1))::integer
  into
    v_target_count, v_flow_count, v_process_count, v_completed_count,
    v_nonterminal_count, v_failed_count, v_distinct_ordinal_count,
    v_distinct_target_count, v_distinct_plan_count, v_distinct_summary_count,
    v_distinct_declared_count, v_declared_count, v_min_ordinal,
    v_max_ordinal
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = p_actor_user_id
    and (
      (v_is_batch and request.batch_id = p_batch_id)
      or (not v_is_batch and request.batch_id is null
        and request.id = p_batch_id)
    );

  if v_target_count not between 1 and 50
    or v_flow_count + v_process_count <> v_target_count
    or v_distinct_ordinal_count <> v_target_count
    or v_distinct_target_count <> v_target_count
    or v_distinct_plan_count <> 1
    or v_distinct_summary_count <> 1
    or v_distinct_declared_count <> 1
    or v_declared_count <> v_target_count
    or v_min_ordinal <> 1
    or v_max_ordinal <> v_target_count then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'reference_id', p_batch_id,
      'reference_kind', case when v_is_batch then 'batch' else 'request' end,
      'batch_id', case when v_is_batch then p_batch_id else null end,
      'request_id', case when v_is_batch then null else p_batch_id end,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_TARGET_SET_MISMATCH',
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count
    );
  end if;

  if v_nonterminal_count > 0 or v_failed_count > 0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', coalesce(request.batch_ordinal, 1),
      'request_id', request.id,
      'table', request.target_table,
      'id', request.target_id,
      'version', request.target_version,
      'status', request.status,
      'phase', request.phase,
      'error', request.last_error,
      'causal_terminal_proof', false
    ) order by request.batch_ordinal), '[]'::jsonb)
    into v_targets
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and (
        (v_is_batch and request.batch_id = p_batch_id)
        or (not v_is_batch and request.batch_id is null
          and request.id = p_batch_id)
      );

    return jsonb_build_object(
      'ok', v_failed_count = 0,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'reference_id', p_batch_id,
      'reference_kind', case when v_is_batch then 'batch' else 'request' end,
      'batch_id', case when v_is_batch then p_batch_id else null end,
      'request_id', case when v_is_batch then null else p_batch_id end,
      'status', case when v_failed_count > 0 then 'failed' else 'pending' end,
      'code', case when v_failed_count > 0
        then 'DERIVATIVE_BATCH_CHILD_FAILED'
        else 'DERIVATIVE_BATCH_PENDING' end,
      'proof_level', 'status_only',
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'completed_count', v_completed_count,
      'nonterminal_count', v_nonterminal_count,
      'failed_count', v_failed_count,
      'targets', v_targets
    );
  end if;

  for v_request in
    select request.*
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and (
        (v_is_batch and request.batch_id = p_batch_id)
        or (not v_is_batch and request.batch_id is null
          and request.id = p_batch_id)
      )
    order by coalesce(request.batch_ordinal, 1)
  loop
    begin
      v_snapshot := util.dataset_derivative_rebuild_snapshot(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );
    exception when others then
      v_snapshot := null;
    end;

    v_primary_ok := v_snapshot is not null
      and util.dataset_derivative_rebuild_primary_matches(v_request);
    v_snapshot_ok := coalesce(
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
        > coalesce(v_request.before_embedding_ft_at, '-infinity'::timestamp with time zone),
      false
    );
    v_lifecycle_ok := coalesce(
      v_request.phase = 'completed'
      and v_request.markdown_request_id is not null
      and v_request.markdown_dispatched_at is not null
      and v_request.markdown_response_status between 200 and 299
      and v_request.markdown_response_received_at >= v_request.markdown_dispatched_at
      and v_request.markdown_proposal_id is not null
      and v_request.accepted_extracted_md_sha256 is not null
      and v_request.embedding_queue_msg_id is not null
      and v_request.embedding_queued_at >= v_request.markdown_response_received_at
      and v_request.embedding_proposal_id is not null
      and v_request.completed_at >= v_request.embedding_queued_at
      and v_request.terminal_at >= v_request.completed_at
      and v_request.drained_at >= v_request.completed_at,
      false
    );

    select
      count(*) = 2
      and count(*) filter (
        where proposal.id = v_request.markdown_proposal_id
          and proposal.proposal_kind = 'markdown'
          and proposal.status = 'committed'
          and proposal.extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.extracted_md_sha256
            = v_snapshot->>'extracted_md_sha256'
      ) = 1
      and count(*) filter (
        where proposal.id = v_request.embedding_proposal_id
          and proposal.proposal_kind = 'embedding'
          and proposal.status = 'committed'
          and proposal.source_extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.embedding_ft_sha256
            = v_snapshot->>'embedding_ft_sha256'
          and proposal.embedding_ft_at
            = (v_snapshot->>'embedding_ft_at')::timestamp with time zone
      ) = 1
    into v_proposals_ok
    from util.dataset_derivative_rebuild_proposals as proposal
    where proposal.request_id = v_request.id
      and proposal.status <> 'discarded';

    select jsonb_build_object(
      'http_requests', (
        select count(*)
        from net.http_request_queue as request
        where util.dataset_derivative_rebuild_http_body_matches(
          request.body, v_request.target_table,
          v_request.target_id, v_request.target_version
        )
      ),
      'embedding_jobs', (
        select count(*)
        from pgmq.q_embedding_jobs as job
        where job.message->>'id' = v_request.target_id::text
          and btrim(job.message->>'version') = v_request.target_version
          and job.message->>'schema' = 'public'
          and job.message->>'table' = v_request.target_table
          and job.message->>'embeddingColumn' = 'embedding_ft'
      ),
      'pending_jobs', (
        select count(*)
        from util.pending_embedding_jobs as pending
        where pending.schema_name = 'public'
          and pending.table_name = v_request.target_table
          and pending.record_id = v_request.target_id::text
          and btrim(pending.record_version) = v_request.target_version
          and pending.embedding_column = 'embedding_ft'
          and pending.status = 'pending'
      ),
      'failure_rows', (
        select count(*)
        from util.embedding_job_failures as failure
        where failure.msg_id = v_request.embedding_queue_msg_id
      ),
      'other_active_fences', (
        select count(*)
        from util.dataset_derivative_rebuild_requests as active
        where active.id <> v_request.id
          and active.target_table = v_request.target_table
          and active.target_id = v_request.target_id
          and active.target_version = v_request.target_version
          and active.status not in ('completed', 'stale', 'failed')
      )
    ) into v_residue;

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

    v_target_ok := coalesce(
      v_primary_ok and v_snapshot_ok and v_derivative_fresh
      and v_lifecycle_ok and v_proposals_ok and v_terminal_audit_ok
      and (v_residue->>'http_requests')::integer = 0
      and (v_residue->>'embedding_jobs')::integer = 0
      and (v_residue->>'pending_jobs')::integer = 0
      and (v_residue->>'failure_rows')::integer = 0
      and (v_residue->>'other_active_fences')::integer = 0,
      false
    );
    if not v_target_ok then
      v_invalid_proof_count := v_invalid_proof_count + 1;
    end if;

    v_targets := v_targets || jsonb_build_array(jsonb_build_object(
      'ordinal', coalesce(v_request.batch_ordinal, 1),
      'request_id', v_request.id,
      'table', v_request.target_table,
      'id', v_request.target_id,
      'version', v_request.target_version,
      'status', v_request.status,
      'phase', v_request.phase,
      'source_baseline_snapshot_sha256',
        v_request.source_baseline_snapshot_sha256,
      'expected_snapshot_sha256', v_request.expected_snapshot_sha256,
      'completed_snapshot_sha256', v_request.completed_snapshot_sha256,
      'primary_matches', v_primary_ok,
      'terminal_snapshot_matches', v_snapshot_ok,
      'proposals_committed', v_proposals_ok,
      'derivative_fresh', v_derivative_fresh,
      'lifecycle_complete', v_lifecycle_ok,
      'terminal_audit_present', v_terminal_audit_ok,
      'residue', v_residue,
      'causal_terminal_proof', v_target_ok
    ));
  end loop;

  if v_completed_count = v_target_count and v_invalid_proof_count = 0 then
    v_status := 'completed';
    v_code := 'DERIVATIVE_BATCH_COMPLETED';
  else
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH';
  end if;

  return jsonb_build_object(
    'ok', v_status = 'completed',
    'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
    'reference_id', p_batch_id,
    'reference_kind', case when v_is_batch then 'batch' else 'request' end,
    'batch_id', case when v_is_batch then p_batch_id else null end,
    'request_id', case when v_is_batch then null else p_batch_id end,
    'status', v_status,
    'code', v_code,
    'proof_level', 'causal_terminal',
    'causal_terminal_proof', v_status = 'completed',
    'target_count', v_target_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'completed_count', v_completed_count,
    'nonterminal_count', v_nonterminal_count,
    'failed_count', v_failed_count,
    'invalid_proof_count', v_invalid_proof_count,
    'targets', v_targets,
    'proof_sha256', util.dataset_flow_identity_sha256(v_targets)
  );
end;
$$;

alter function util.read_dataset_derivative_rebuild_batch_any(uuid, uuid)
  owner to postgres;
revoke all on function util.read_dataset_derivative_rebuild_batch_any(uuid, uuid)
  from public, anon, authenticated, service_role;

comment on function util.read_dataset_derivative_rebuild_batch_any(uuid, uuid) is
  'Private dynamic derivative proof for either an exact 1..50 batch id or one exact unbatched compensation request id. Unlike the retained Step 2 reader, cardinality comes from the durable admission; terminal success still requires exact primary, proposals, audit, freshness, lifecycle, and zero-residue causal proof.';

create or replace function util.read_dataset_flow_identity_derivative_set(
  p_actor_user_id uuid,
  p_scope_id uuid
) returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  with scope_row as (
    select scope.*
    from util.dataset_flow_identity_scopes as scope
    where scope.id = p_scope_id
      and scope.actor_user_id = p_actor_user_id
  ), base as materialized (
    select
      ledger.ordinal,
      ledger.process_id,
      ledger.process_version,
      ledger.derivative_batch_id,
      ledger.after_payload_sha256,
      ledger.manifest,
      primary_audit.created_at as primary_audit_created_at,
      process.modified_at as current_modified_at,
      snapshot.value as current_snapshot,
      original as original_request,
      original_action.payload as original_action_payload,
      original_summary.payload as original_summary_payload,
      scope.plan_sha256 as scope_plan_sha256,
      scope.operation_id as scope_operation_id
    from scope_row as scope
    join util.dataset_flow_identity_process_ledger as ledger
      on ledger.scope_id = scope.id
    join public.processes as process
      on process.id = ledger.process_id
      and btrim(process.version::text) = ledger.process_version
      and process.user_id = p_actor_user_id
      and process.state_code = 0
    cross join lateral (
      select util.dataset_derivative_rebuild_snapshot(process) as value
    ) as snapshot
    join public.command_audit_log as primary_audit
      on primary_audit.id = ledger.audit_id
      and primary_audit.actor_user_id = p_actor_user_id
      and primary_audit.command
        = 'cmd_dataset_flow_identity_process_rewrite_guarded'
    left join util.dataset_derivative_rebuild_requests as original
      on original.actor_user_id = p_actor_user_id
      and original.batch_id = ledger.derivative_batch_id
      and original.target_table = 'processes'
      and original.target_id = ledger.process_id
      and original.target_version = ledger.process_version
    left join public.command_audit_log as original_action
      on original_action.id = original.action_audit_id
    left join public.command_audit_log as original_summary
      on original_summary.id = original.summary_audit_id
  ), compensation as materialized (
    select base.ordinal, candidate.request_row
    from base
    left join lateral (
      select request as request_row
      from util.dataset_derivative_rebuild_requests as request
      where request.actor_user_id = p_actor_user_id
        and request.batch_id is null
        and request.target_table = 'processes'
        and request.target_id = base.process_id
        and request.target_version = base.process_version
        and request.plan_sha256 <> base.scope_plan_sha256
        and request.expected_json_ordered_sha256
          = base.current_snapshot->>'json_ordered_sha256'
        and request.reason_code = 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
          || p_scope_id::text || ':' || base.ordinal::text
        and request.operation_id = request.action_id
        and request.operation_id ~ (
          '^FLOW_IDENTITY_SCOPE_COMPENSATION:' || p_scope_id::text || ':'
          || base.ordinal::text
          || ':[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'
        )
        and request.admitted_at > base.primary_audit_created_at
        and exists (
          select 1
          from public.command_audit_log as action_audit
          where action_audit.id = request.action_audit_id
            and action_audit.actor_user_id = p_actor_user_id
            and action_audit.command
              = 'cmd_dataset_derivative_rebuild_plan_guarded'
            and action_audit.target_table = 'processes'
            and action_audit.target_id = base.process_id
            and action_audit.target_version = base.process_version
            and action_audit.payload->>'request_id' = request.id::text
            and action_audit.payload->>'plan_sha256' = request.plan_sha256
            and action_audit.payload->>'operation_id' = request.operation_id
            and action_audit.payload->>'action_id' = request.action_id
            and action_audit.payload->>'reason_code' = request.reason_code
            and action_audit.payload->>'expected_snapshot_sha256'
              = request.expected_snapshot_sha256
        )
        and exists (
          select 1
          from public.command_audit_log as summary_audit
          where summary_audit.id = request.summary_audit_id
            and summary_audit.actor_user_id = p_actor_user_id
            and summary_audit.command
              = 'cmd_dataset_derivative_rebuild_plan_guarded'
            and summary_audit.target_table is null
            and summary_audit.payload->>'request_id' = request.id::text
            and summary_audit.payload->>'plan_sha256' = request.plan_sha256
            and summary_audit.payload->>'operation_id' = request.operation_id
            and summary_audit.payload->>'action_count' = '1'
            and summary_audit.payload->>'accepted_count' = '1'
        )
      order by request.admitted_at desc, request.id desc
      limit 1
    ) as candidate on true
    where (candidate.request_row).id is not null
  ), candidate as materialized (
    select
      base.ordinal,
      base.process_id,
      base.process_version,
      base.derivative_batch_id,
      base.after_payload_sha256,
      base.current_modified_at,
      base.current_snapshot,
      'protected_batch'::text as reference_kind,
      base.original_request as request_row,
      coalesce(
        (base.original_request).id is not null
        and (base.original_request).batch_id = base.derivative_batch_id
        and (base.original_request).batch_ordinal = 1
        and (base.original_request).batch_target_count = 1
        and (base.original_request).plan_sha256 = base.scope_plan_sha256
        and (base.original_request).operation_id = base.scope_operation_id
        and (base.original_request).reason_code = 'FLOW_IDENTITY_SCOPE:'
          || p_scope_id::text || ':' || base.ordinal::text
        and (base.original_request).admitted_at
          > base.primary_audit_created_at
        and base.original_action_payload->>'batch_id'
          = base.derivative_batch_id::text
        and base.original_action_payload->>'request_id'
          = (base.original_request).id::text
        and base.original_summary_payload->>'batch_id'
          = base.derivative_batch_id::text,
        false
      ) as lineage_ok
    from base
    union all
    select
      base.ordinal,
      base.process_id,
      base.process_version,
      base.derivative_batch_id,
      base.after_payload_sha256,
      base.current_modified_at,
      base.current_snapshot,
      'separate_compensation',
      compensation.request_row,
      true
    from base
    join compensation using (ordinal)
  ), proposal_proof as (
    select
      candidate.ordinal,
      candidate.reference_kind,
      count(proposal.*) filter (
        where proposal.status <> 'discarded'
      ) = 2
      and count(proposal.*) filter (
        where proposal.status <> 'discarded'
          and proposal.id = (candidate.request_row).markdown_proposal_id
          and proposal.proposal_kind = 'markdown'
          and proposal.status = 'committed'
          and proposal.extracted_md_sha256
            = (candidate.request_row).accepted_extracted_md_sha256
          and proposal.extracted_md_sha256
            = candidate.current_snapshot->>'extracted_md_sha256'
      ) = 1
      and count(proposal.*) filter (
        where proposal.status <> 'discarded'
          and proposal.id = (candidate.request_row).embedding_proposal_id
          and proposal.proposal_kind = 'embedding'
          and proposal.status = 'committed'
          and proposal.source_extracted_md_sha256
            = (candidate.request_row).accepted_extracted_md_sha256
          and proposal.embedding_ft_sha256
            = candidate.current_snapshot->>'embedding_ft_sha256'
          and proposal.embedding_ft_at =
            (candidate.current_snapshot->>'embedding_ft_at')::timestamp with time zone
      ) = 1 as proposals_ok
    from candidate
    left join util.dataset_derivative_rebuild_proposals as proposal
      on proposal.request_id = (candidate.request_row).id
    group by candidate.ordinal, candidate.reference_kind,
      candidate.request_row, candidate.current_snapshot
  ), terminal_audit_proof as (
    select candidate.ordinal, candidate.reference_kind,
      count(audit.*) = 1 as terminal_audit_ok
    from candidate
    left join public.command_audit_log as audit
      on audit.command = 'cmd_dataset_derivative_rebuild_terminal'
      and audit.actor_user_id = p_actor_user_id
      and audit.target_table = 'processes'
      and audit.target_id = candidate.process_id
      and audit.target_version = candidate.process_version
      and audit.payload->>'request_id' = (candidate.request_row).id::text
      and audit.payload->>'status' = 'completed'
    group by candidate.ordinal, candidate.reference_kind
  ), http_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(request.*)::integer as residue_count
    from candidate
    left join net.http_request_queue as request
      on util.dataset_derivative_rebuild_http_body_matches(
        request.body, 'processes', candidate.process_id,
        candidate.process_version
      )
    group by candidate.ordinal, candidate.reference_kind
  ), embedding_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(job.*)::integer as residue_count
    from candidate
    left join pgmq.q_embedding_jobs as job
      on job.message->>'id' = candidate.process_id::text
      and btrim(job.message->>'version') = candidate.process_version
      and job.message->>'schema' = 'public'
      and job.message->>'table' = 'processes'
      and job.message->>'embeddingColumn' = 'embedding_ft'
    group by candidate.ordinal, candidate.reference_kind
  ), pending_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(pending.*)::integer as residue_count
    from candidate
    left join util.pending_embedding_jobs as pending
      on pending.schema_name = 'public'
      and pending.table_name = 'processes'
      and pending.record_id = candidate.process_id::text
      and btrim(pending.record_version) = candidate.process_version
      and pending.embedding_column = 'embedding_ft'
      and pending.status = 'pending'
    group by candidate.ordinal, candidate.reference_kind
  ), failure_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(failure.*)::integer as residue_count
    from candidate
    left join util.embedding_job_failures as failure
      on failure.msg_id = (candidate.request_row).embedding_queue_msg_id
    group by candidate.ordinal, candidate.reference_kind
  ), fence_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(active.*)::integer as residue_count
    from candidate
    left join util.dataset_derivative_rebuild_requests as active
      on active.id <> (candidate.request_row).id
      and active.target_table = 'processes'
      and active.target_id = candidate.process_id
      and active.target_version = candidate.process_version
      and active.status not in ('completed', 'stale', 'failed')
    group by candidate.ordinal, candidate.reference_kind
  ), evaluated as materialized (
    select
      candidate.*,
      coalesce(proposal_proof.proposals_ok, false) as proposals_ok,
      coalesce(terminal_audit_proof.terminal_audit_ok, false)
        as terminal_audit_ok,
      jsonb_build_object(
        'http_requests', coalesce(http_residue.residue_count, 0),
        'embedding_jobs', coalesce(embedding_residue.residue_count, 0),
        'pending_jobs', coalesce(pending_residue.residue_count, 0),
        'failure_rows', coalesce(failure_residue.residue_count, 0),
        'other_active_fences', coalesce(fence_residue.residue_count, 0)
      ) as residue,
      coalesce(
        candidate.lineage_ok
        and (candidate.request_row).status = 'completed'
        and candidate.current_snapshot->>'user_id' = p_actor_user_id::text
        and candidate.current_snapshot->>'state_code' = '0'
        and candidate.current_modified_at
          = (candidate.request_row).expected_modified_at
        and candidate.current_snapshot->>'json_sha256'
          = (candidate.request_row).expected_json_sha256
        and candidate.current_snapshot->>'json_ordered_sha256'
          = (candidate.request_row).expected_json_ordered_sha256
        and candidate.current_snapshot->>'extracted_text_sha256'
          = (candidate.request_row).expected_extracted_text_sha256
        and candidate.current_snapshot->>'snapshot_sha256'
          = (candidate.request_row).completed_snapshot_sha256
        and candidate.current_snapshot->>'extracted_md_sha256' is not null
        and candidate.current_snapshot->>'embedding_ft_sha256' is not null
        and (candidate.current_snapshot->>'embedding_ft_at')::timestamp with time zone
          > coalesce((candidate.request_row).before_embedding_ft_at,
            '-infinity'::timestamp with time zone)
        and (candidate.request_row).phase = 'completed'
        and (candidate.request_row).markdown_request_id is not null
        and (candidate.request_row).markdown_dispatched_at is not null
        and (candidate.request_row).markdown_response_status between 200 and 299
        and (candidate.request_row).markdown_response_received_at
          >= (candidate.request_row).markdown_dispatched_at
        and (candidate.request_row).markdown_proposal_id is not null
        and (candidate.request_row).accepted_extracted_md_sha256 is not null
        and (candidate.request_row).embedding_queue_msg_id is not null
        and (candidate.request_row).embedding_queued_at
          >= (candidate.request_row).markdown_response_received_at
        and (candidate.request_row).embedding_proposal_id is not null
        and (candidate.request_row).completed_at
          >= (candidate.request_row).embedding_queued_at
        and (candidate.request_row).terminal_at
          >= (candidate.request_row).completed_at
        and (candidate.request_row).drained_at
          >= (candidate.request_row).completed_at
        and coalesce(proposal_proof.proposals_ok, false)
        and coalesce(terminal_audit_proof.terminal_audit_ok, false)
        and coalesce(http_residue.residue_count, 0) = 0
        and coalesce(embedding_residue.residue_count, 0) = 0
        and coalesce(pending_residue.residue_count, 0) = 0
        and coalesce(failure_residue.residue_count, 0) = 0
        and coalesce(fence_residue.residue_count, 0) = 0,
        false
      ) as causal_terminal_proof
    from candidate
    left join proposal_proof using (ordinal, reference_kind)
    left join terminal_audit_proof using (ordinal, reference_kind)
    left join http_residue using (ordinal, reference_kind)
    left join embedding_residue using (ordinal, reference_kind)
    left join pending_residue using (ordinal, reference_kind)
    left join failure_residue using (ordinal, reference_kind)
    left join fence_residue using (ordinal, reference_kind)
  ), ranked as (
    select evaluated.*,
      case
        when evaluated.reference_kind = 'protected_batch'
          and (
            evaluated.causal_terminal_proof
            or (evaluated.request_row).status
              not in ('completed', 'stale', 'failed')
          ) then 0
        when evaluated.reference_kind = 'separate_compensation' then 1
        else 2
      end as proof_rank
    from evaluated
  ), effective as (
    select distinct on (ranked.ordinal) ranked.*
    from ranked
    order by ranked.ordinal, ranked.proof_rank
  ), target_payload as (
    select effective.ordinal,
      jsonb_build_object(
        'ordinal', effective.ordinal,
        'id', effective.process_id,
        'version', effective.process_version,
        'original_batch_id', effective.derivative_batch_id,
        'effective_reference_id', (effective.request_row).id,
        'effective_reference_kind', effective.reference_kind,
        'status', case
          when effective.causal_terminal_proof then 'completed'
          when (effective.request_row).status
            not in ('completed', 'stale', 'failed') then 'pending'
          else 'failed' end,
        'request_status', coalesce((effective.request_row).status, 'missing'),
        'phase', coalesce((effective.request_row).phase, 'missing'),
        'lineage_ok', effective.lineage_ok,
        'proposals_committed', effective.proposals_ok,
        'terminal_audit_present', effective.terminal_audit_ok,
        'residue', effective.residue,
        'current_snapshot_sha256',
          effective.current_snapshot->>'snapshot_sha256',
        'current_json_ordered_sha256',
          effective.current_snapshot->>'json_ordered_sha256',
        'causal_terminal_proof', effective.causal_terminal_proof
      ) as value,
      jsonb_build_object(
        'ordinal', effective.ordinal,
        'table', 'processes',
        'id', effective.process_id,
        'version', effective.process_version,
        'original_batch_id', effective.derivative_batch_id,
        'original_status', case
          when (base.original_request).id is null then 'missing'
          when (base.original_request).status = 'stale' then 'stale'
          else 'failed' end,
        'original_code', coalesce(
          nullif(btrim((base.original_request).last_error->>'code'), ''),
          case
            when (base.original_request).id is null
              then 'DERIVATIVE_BATCH_CHILD_MISSING'
            when (base.original_request).status = 'stale'
              then 'DERIVATIVE_BATCH_CHILD_STALE'
            when (base.original_request).status = 'failed'
              then 'DERIVATIVE_BATCH_CHILD_FAILED'
            else 'DERIVATIVE_CAUSAL_TERMINAL_PROOF_FAILED'
          end
        ),
        'latest_compensation_request_id', case
          when effective.reference_kind = 'separate_compensation'
            then (effective.request_row).id else null end,
        'latest_compensation_status', case
          when effective.reference_kind = 'separate_compensation'
            then (effective.request_row).status else null end,
        'latest_compensation_plan_sha256', case
          when effective.reference_kind = 'separate_compensation'
            then (effective.request_row).plan_sha256 else null end,
        'desired_payload_sha256', effective.after_payload_sha256,
        'current_json_ordered_sha256',
          effective.current_snapshot->>'json_ordered_sha256',
        'current_snapshot_sha256',
          effective.current_snapshot->>'snapshot_sha256',
        'current_modified_at', effective.current_snapshot->>'modified_at',
        'components', jsonb_build_array('extracted_md', 'embedding_ft'),
        'reason_code', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
          || p_scope_id::text || ':' || effective.ordinal::text,
        'operation_id_prefix', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
          || p_scope_id::text || ':' || effective.ordinal::text || ':',
        'requires_new_plan_freeze_approval', true,
        'automatic_retry', false
      ) as compensation_value,
      effective.causal_terminal_proof,
      coalesce(
        (effective.request_row).status
          not in ('completed', 'stale', 'failed'),
        false
      ) as is_pending
    from effective
    join base using (ordinal)
  ), aggregate_payload as (
    select
      count(*)::integer as target_count,
      count(*) filter (where causal_terminal_proof)::integer
        as completed_count,
      count(*) filter (
        where not causal_terminal_proof and is_pending
      )::integer as pending_count,
      count(*) filter (
        where not causal_terminal_proof and not is_pending
      )::integer as failed_count,
      coalesce(jsonb_agg(value order by ordinal), '[]'::jsonb) as targets,
      coalesce(jsonb_agg(compensation_value order by ordinal) filter (
        where not causal_terminal_proof and not is_pending
      ), '[]'::jsonb) as compensation_targets,
      coalesce((select process_count from scope_row), 0)::integer
        as expected_target_count
    from target_payload
  )
  select jsonb_build_object(
    'ok', aggregate_payload.target_count > 0
      and aggregate_payload.target_count
        = aggregate_payload.expected_target_count
      and aggregate_payload.completed_count
        + aggregate_payload.pending_count
        + aggregate_payload.failed_count
        = aggregate_payload.target_count
      and aggregate_payload.failed_count = 0,
    'schema_version', 'dataset-flow-identity-derivative-set-proof.v1',
    'scope_id', p_scope_id,
    'status', case
      when aggregate_payload.target_count = 0
        or aggregate_payload.target_count
          <> aggregate_payload.expected_target_count
        or aggregate_payload.completed_count
          + aggregate_payload.pending_count
          + aggregate_payload.failed_count
          <> aggregate_payload.target_count then 'failed'
      when aggregate_payload.failed_count > 0 then 'compensation_required'
      when aggregate_payload.pending_count > 0 then 'pending'
      else 'completed' end,
    'target_count', aggregate_payload.target_count,
    'completed_count', aggregate_payload.completed_count,
    'pending_count', aggregate_payload.pending_count,
    'failed_count', aggregate_payload.failed_count,
    'causal_terminal_proof', aggregate_payload.target_count > 0
      and aggregate_payload.target_count
        = aggregate_payload.expected_target_count
      and aggregate_payload.completed_count = aggregate_payload.target_count
      and aggregate_payload.pending_count = 0
      and aggregate_payload.failed_count = 0,
    'targets', aggregate_payload.targets,
    'compensation_targets', aggregate_payload.compensation_targets,
    'proof_sha256', util.dataset_flow_identity_sha256(
      aggregate_payload.targets
    )
  )
  from aggregate_payload
$$;

alter function util.read_dataset_flow_identity_derivative_set(uuid, uuid)
  owner to postgres;
revoke all on function util.read_dataset_flow_identity_derivative_set(uuid, uuid)
  from public, anon, authenticated, service_role;

comment on function util.read_dataset_flow_identity_derivative_set(uuid, uuid) is
  'Private set-based Step 3 derivative proof. One relational pass evaluates all protected children and exact separately approved compensation requests, aggregates proposal/audit/queue/fence residue once, and emits ordered proof arrays without per-child reader calls or repeated JSON concatenation.';

create or replace function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  p_scope_id uuid,
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '30s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_cancel_guarded';
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_request_sha256 text;
  v_completed_count integer;
  v_audit_id bigint;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null
    or p_request is null
    or not private.dataset_flow_identity_exact_keys(
      p_request,
      array[
        'schema_version', 'request_id', 'scope_proof_sha256',
        'reason', 'evidence_sha256'
      ]
    )
    or p_request->>'schema_version'
      <> 'dataset-flow-identity-scope-cancel.v1'
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or p_request->>'evidence_sha256' !~ '^[a-f0-9]{64}$'
    or nullif(btrim(p_request->>'reason'), '') is null
    or octet_length(p_request->>'reason') > 512 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_INVALID_REQUEST', 'status', 400,
      'message', 'Cancel request schema mismatch'
    );
  end if;
  if not pg_try_advisory_xact_lock(
    hashtextextended('dataset-flow-identity:' || p_scope_id::text, 0)
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction currently owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  if v_scope.scope_proof_sha256
      is distinct from p_request->>'scope_proof_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Cancel request does not match the sealed scope'
    );
  end if;
  v_request_sha256 := util.dataset_flow_identity_sha256(p_request);
  if v_scope.status = 'cancelled' then
    return jsonb_build_object(
      'ok', v_scope.cancel_request_sha256 = v_request_sha256,
      'command', v_command,
      'code', case when v_scope.cancel_request_sha256 = v_request_sha256
        then 'FLOW_IDENTITY_SCOPE_CANCELLED'
        else 'FLOW_IDENTITY_CANCEL_REPLAY_MISMATCH' end,
      'status', case when v_scope.cancel_request_sha256 = v_request_sha256
        then 'cancelled' else 'conflict' end,
      'scope_id', p_scope_id, 'replay', true
    );
  end if;
  select count(*) filter (where ledger.status = 'completed')::integer
  into v_completed_count
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = p_scope_id;
  if v_scope.status = 'completed' or v_completed_count > 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PARTIAL_SCOPE_MUST_CONTINUE', 'status', 409,
      'message', 'A scope with any committed primary process cannot cancel',
      'completed_process_count', v_completed_count,
      'automatic_retry', false
    );
  end if;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null,
    jsonb_build_object(
      'record_type', 'scope_cancel',
      'schema_version', p_request->>'schema_version',
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'cancel_request_sha256', v_request_sha256,
      'reason', btrim(p_request->>'reason'),
      'evidence_sha256', p_request->>'evidence_sha256',
      'completed_process_count', 0,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_audit_id;
  update util.dataset_flow_identity_process_ledger
  set active = false
  where scope_id = p_scope_id;
  update util.dataset_flow_identity_scopes
  set status = 'cancelled', cancel_request_sha256 = v_request_sha256,
    last_error = jsonb_build_object(
      'code', 'FLOW_IDENTITY_SCOPE_CANCELLED',
      'reason', btrim(p_request->>'reason'),
      'evidence_sha256', p_request->>'evidence_sha256'
    ), updated_at = clock_timestamp()
  where id = p_scope_id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-cancel-result.v1',
    'scope_id', p_scope_id, 'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'status', 'cancelled', 'completed_process_count', 0,
    'audit_id', v_audit_id::text, 'replay', false
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_CANCEL_LOCK_BUSY', 'status', 409,
    'message', 'Cancel could not acquire its bounded lock'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid, jsonb)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) to authenticated;

comment on function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) is
  'Actor-only Step 3 cancellation. It releases private active-scope fences only while zero process primaries are committed; any partial scope is non-cancellable and must continue under its sealed order.';

create or replace function public.cmd_dataset_flow_identity_scope_read(
  p_scope_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '60s'
as $$
declare
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_processes jsonb;
  v_protected_proof jsonb;
  v_derivative_set_proof jsonb;
  v_completed integer;
  v_pending integer;
  v_failed integer;
  v_derivative_pending integer;
  v_derivative_failed integer;
  v_compensation_targets jsonb := '[]'::jsonb;
  v_next_ordinal integer;
  v_status text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', 'cmd_dataset_flow_identity_scope_read',
      'code', 'AUTH_REQUIRED', 'status', 401,
      'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null then
    return jsonb_build_object(
      'ok', false, 'command', 'cmd_dataset_flow_identity_scope_read',
      'code', 'FLOW_IDENTITY_SCOPE_READ_INVALID_REQUEST', 'status', 400,
      'message', 'Scope ID is required'
    );
  end if;

  select scope.*
  into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', 'cmd_dataset_flow_identity_scope_read',
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;

  select
    count(*) filter (where ledger.status = 'completed')::integer,
    count(*) filter (where ledger.status = 'pending')::integer,
    count(*) filter (where ledger.status = 'failed')::integer,
    count(*) filter (
      where child.status is not null
        and child.status not in ('completed', 'stale', 'failed')
    )::integer,
    count(*) filter (where child.status in ('stale', 'failed'))::integer,
    min(ledger.ordinal) filter (where ledger.status = 'pending')::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ledger.ordinal,
      'id', ledger.process_id,
      'version', ledger.process_version,
      'status', ledger.status,
      'process_request_sha256', ledger.process_request_sha256,
      'rewrite_count', ledger.rewrite_count,
      'audit_id', case when ledger.audit_id is null
        then null else ledger.audit_id::text end,
      'before_payload_sha256', ledger.before_payload_sha256,
      'after_payload_sha256', ledger.after_payload_sha256,
      'derivative_batch_id', ledger.derivative_batch_id,
      'derivative_request_id', child.id,
      'derivative_status', case when ledger.derivative_batch_id is null
        then null else coalesce(child.status, 'missing') end,
      'causal_terminal_proof', false,
      'completed_at', ledger.completed_at,
      'last_error', coalesce(ledger.last_error, child.last_error)
    ) order by ledger.ordinal), '[]'::jsonb)
  into
    v_completed, v_pending, v_failed, v_derivative_pending,
    v_derivative_failed, v_next_ordinal, v_processes
  from util.dataset_flow_identity_process_ledger as ledger
  left join util.dataset_derivative_rebuild_requests as child
    on child.actor_user_id = v_actor
    and child.batch_id = ledger.derivative_batch_id
    and child.target_table = 'processes'
    and child.target_id = ledger.process_id
    and child.target_version = ledger.process_version
  where ledger.scope_id = p_scope_id;

  v_derivative_set_proof :=
    util.read_dataset_flow_identity_derivative_set(v_actor, p_scope_id);
  v_derivative_pending := coalesce(
    (v_derivative_set_proof->>'pending_count')::integer, 0
  );
  v_derivative_failed := coalesce(
    (v_derivative_set_proof->>'failed_count')::integer, 0
  );

  select coalesce(jsonb_agg(
    target.value || jsonb_build_object(
      'original_request_id', child.id,
      'original_error', child.last_error
    ) order by (target.value->>'ordinal')::integer
  ), '[]'::jsonb)
  into v_compensation_targets
  from jsonb_array_elements(
    coalesce(
      v_derivative_set_proof->'compensation_targets', '[]'::jsonb
    )
  ) as target(value)
  join util.dataset_flow_identity_process_ledger as ledger
    on ledger.scope_id = p_scope_id
    and ledger.ordinal = (target.value->>'ordinal')::integer
  left join util.dataset_derivative_rebuild_requests as child
    on child.actor_user_id = v_actor
    and child.batch_id = ledger.derivative_batch_id
    and child.target_table = 'processes'
    and child.target_id = ledger.process_id
    and child.target_version = ledger.process_version;

  v_protected_proof := util.dataset_flow_identity_protected_closure(
    v_actor, v_scope.protected_closure
  );
  v_status := case
    when v_scope.status = 'cancelled' then 'cancelled'
    when v_scope.status = 'failed' or v_failed > 0 then 'failed'
    when v_completed = 0 then 'sealed'
    when v_pending > 0 then 'running'
    when v_completed = v_scope.process_count
      and coalesce(
        (v_derivative_set_proof->>'causal_terminal_proof')::boolean,
        false
      )
      then case when v_scope.status = 'completed'
        then 'completed' else 'primary_complete' end
    when v_completed = v_scope.process_count then 'derivatives_pending'
    else v_scope.status
  end;

  return jsonb_build_object(
    'ok', v_status <> 'failed',
    'command', 'cmd_dataset_flow_identity_scope_read',
    'schema_version', 'dataset-flow-identity-scope-status.v1',
    'scope_id', v_scope.id,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_status,
    'process_count', v_scope.process_count,
    'completed_process_count', v_completed,
    'pending_process_count', v_pending,
    'failed_process_count', v_failed,
    'next_ordinal', coalesce(v_next_ordinal, v_scope.process_count + 1),
    'rewrite_count', v_scope.rewrite_count,
    'completed_rewrite_count', coalesce((
      select sum(ledger.rewrite_count)::integer
      from util.dataset_flow_identity_process_ledger as ledger
      where ledger.scope_id = p_scope_id and ledger.status = 'completed'
    ), 0),
    'primary_complete', v_completed = v_scope.process_count,
    'cancellable', v_completed = 0
      and v_scope.status not in ('completed', 'cancelled', 'failed'),
    'strict_continuation_required', v_completed > 0
      and v_completed < v_scope.process_count,
    'derivatives_current', v_status = 'completed',
    'derivative_pending_count', v_derivative_pending,
    'derivative_failed_count', v_derivative_failed,
    'derivative_set_proof', v_derivative_set_proof,
    'derivative_proof_set_sha256',
      v_derivative_set_proof->>'proof_sha256',
    'compensation_required', v_derivative_failed > 0,
    'compensation_targets', v_compensation_targets,
    'protected_closure_current', coalesce(
      (v_protected_proof->>'ok')::boolean, false
    ),
    'protected_closure_proof', v_protected_proof,
    'processes', v_processes,
    'terminal_proof_sha256', case when v_status = 'completed'
      then v_scope.terminal_proof_sha256 else null end,
    'completed_at', case when v_status = 'completed'
      then v_scope.completed_at else null end
  ) || case when v_derivative_failed > 0 then jsonb_build_object(
    'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
  ) else '{}'::jsonb end;
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_read(uuid)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_read(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_read(uuid)
  to authenticated;

comment on function public.cmd_dataset_flow_identity_scope_read(uuid) is
  'Actor-only, read-only Step 3 resume/status proof. It exposes the exact ordered process ledger and live pending/blocker closure; failed/stale derivatives include exact derivative-only compensation targets, but this function never retries a mutation or admits/dispatches compensation.';

create or replace function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  p_scope_id uuid,
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '180s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_finalize_guarded';
  v_schema_version constant text :=
    'dataset-flow-identity-scope-finalize.v1';
  v_result_schema_version constant text :=
    'dataset-flow-identity-scope-finalize-result.v1';
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_mapping util.dataset_flow_identity_mappings%rowtype;
  v_mapping_validation jsonb;
  v_support_proof jsonb;
  v_universe_proof jsonb;
  v_protected_proof jsonb;
  v_derivative_set_proof jsonb;
  v_primary_entries jsonb;
  v_derivative_targets jsonb;
  v_primary_closure_sha256 text;
  v_derivative_target_set_sha256 text;
  v_request_sha256 text;
  v_terminal_proof_sha256 text;
  v_completed integer;
  v_rewrite_count integer;
  v_audit_count integer;
  v_primary_drift_count integer;
  v_approved_reference_residue bigint;
  v_final_audit_id bigint;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null
    or p_request is null
    or not private.dataset_flow_identity_exact_keys(
      p_request,
      array['schema_version', 'request_id', 'scope_proof_sha256', 'expected']
    )
    or p_request->>'schema_version' <> v_schema_version
    or p_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or not private.dataset_flow_identity_exact_keys(
      p_request->'expected',
      array[
        'process_count', 'rewrite_count', 'completed_process_count',
        'primary_closure_sha256', 'protected_closure_sha256',
        'derivative_target_set_sha256'
      ]
    )
    or p_request #>> '{expected,process_count}' !~ '^[1-9][0-9]*$'
    or p_request #>> '{expected,rewrite_count}' !~ '^[1-9][0-9]*$'
    or p_request #>> '{expected,completed_process_count}'
      !~ '^[1-9][0-9]*$'
    or p_request #>> '{expected,primary_closure_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_request #>> '{expected,protected_closure_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_request #>> '{expected,derivative_target_set_sha256}'
      !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 finalize request schema mismatch'
    );
  end if;
  if not pg_try_advisory_xact_lock(
    hashtextextended('dataset-flow-identity:' || p_scope_id::text, 0)
  ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction currently owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  if v_scope.status = 'cancelled' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_CANCELLED', 'status', 409,
      'message', 'A cancelled zero-write scope cannot be finalized'
    );
  end if;
  if v_scope.scope_proof_sha256
      is distinct from p_request->>'scope_proof_sha256'
    or (p_request #>> '{expected,process_count}')::integer
      <> v_scope.process_count
    or (p_request #>> '{expected,completed_process_count}')::integer
      <> v_scope.process_count
    or (p_request #>> '{expected,rewrite_count}')::integer
      <> v_scope.rewrite_count
    or p_request #>> '{expected,protected_closure_sha256}'
      is distinct from v_scope.protected_closure_sha256 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Finalize request does not match the sealed scope'
    );
  end if;

  v_request_sha256 := coalesce(nullif(current_setting(
    'app.dataset_flow_identity_v2_finalize_request_sha256', true
  ), ''), util.dataset_flow_identity_restricted_sha256_v2(p_request));
  if v_scope.status = 'completed' then
    if v_scope.final_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_FINALIZE_REPLAY_MISMATCH', 'status', 409,
        'message', 'Completed scope was finalized by a different request'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', v_result_schema_version,
      'scope_id', v_scope.id, 'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', 'completed', 'process_count', v_scope.process_count,
      'completed_process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'primary_closure_sha256',
        p_request #>> '{expected,primary_closure_sha256}',
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'derivative_target_set_sha256',
        p_request #>> '{expected,derivative_target_set_sha256}',
      'derivatives_current', true,
      'terminal_proof_sha256', v_scope.terminal_proof_sha256,
      'replay', true
    );
  end if;

  v_support_proof := util.dataset_flow_identity_validate_support_set(
    v_actor, v_scope.support_snapshots,
    v_scope.support_snapshot_set_sha256
  );
  v_universe_proof := util.dataset_flow_identity_source_universe(
    v_actor, v_scope.source_universe, v_scope.source_universe_sha256
  );
  if coalesce((v_support_proof->>'ok')::boolean, false) is false
    or coalesce((v_universe_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_GUARD_DRIFT', 'status', 409,
      'message', 'Source universe or FP/UG support set drifted',
      'support_proof', v_support_proof,
      'source_universe_proof', v_universe_proof
    );
  end if;

  with primary_row as materialized (
    select
      ledger.*,
      process.id as live_id,
      util.dataset_flow_identity_sha256(process.json_ordered::jsonb)
        as live_payload_sha256,
      util.dataset_flow_identity_sha256(
        private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
      ) as live_exchange_sha256,
      process.model_id as live_model_id,
      process.rule_verification as live_rule_verification,
      audit.id as live_audit_id
    from util.dataset_flow_identity_process_ledger as ledger
    left join public.processes as process
      on process.id = ledger.process_id
      and btrim(process.version::text) = ledger.process_version
      and process.user_id = v_actor and process.state_code = 0
      and process.json is not null and process.json_ordered is not null
      and process.json::jsonb = process.json_ordered::jsonb
    left join public.command_audit_log as audit
      on audit.id = ledger.audit_id
      and audit.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
      and audit.actor_user_id = v_actor
      and audit.target_table = 'processes'
      and audit.target_id = ledger.process_id
      and audit.target_version = ledger.process_version
      and audit.payload->>'scope_id' = p_scope_id::text
      and audit.payload->>'process_request_sha256'
        = ledger.process_request_sha256
      and audit.payload->>'derivative_batch_id'
        = ledger.derivative_batch_id::text
      and audit.payload->>'derivative_reason_code'
        = 'FLOW_IDENTITY_SCOPE:' || p_scope_id::text || ':'
          || ledger.ordinal::text
    where ledger.scope_id = p_scope_id
  )
  select
    count(*) filter (where status = 'completed')::integer,
    coalesce(sum(rewrite_count) filter (where status = 'completed'), 0)::integer,
    count(*) filter (where status = 'completed'
      and live_audit_id is not null)::integer,
    count(*) filter (
      where status <> 'completed'
        or live_id is null
        or live_payload_sha256
          is distinct from manifest->>'desired_payload_sha256'
        or live_exchange_sha256
          is distinct from manifest->>'desired_exchange_set_sha256'
        or coalesce(to_jsonb(live_model_id), 'null'::jsonb)
          is distinct from manifest->'model_id'
        or coalesce(to_jsonb(live_rule_verification), 'null'::jsonb)
          is distinct from manifest->'rule_verification'
        or live_audit_id is null
    )::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ordinal,
      'id', process_id,
      'version', process_version,
      'json_ordered_sha256', after_payload_sha256,
      'exchange_set_sha256', after_exchange_set_sha256,
      'audit_id', audit_id::text,
      'wrapper_invocation_id', wrapper_invocation_id,
      'permit_generation_before', permit_generation_before
    ) order by ordinal), '[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ordinal,
      'id', process_id,
      'version', process_version,
      'desired_json_ordered_sha256', manifest->>'desired_payload_sha256',
      'baseline_snapshot_sha256',
        manifest->>'derivative_baseline_snapshot_sha256'
    ) order by ordinal), '[]'::jsonb)
  into v_completed, v_rewrite_count, v_audit_count,
    v_primary_drift_count, v_primary_entries, v_derivative_targets
  from primary_row;

  if v_completed <> v_scope.process_count
    or v_rewrite_count <> v_scope.rewrite_count
    or v_audit_count <> v_scope.process_count
    or v_primary_drift_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_PRIMARY_INCOMPLETE', 'status', 409,
      'message', 'Not every sealed process has exact live primary/audit proof',
      'primary_drift_count', v_primary_drift_count
    );
  end if;
  v_primary_closure_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    v_primary_entries
  );
  v_derivative_target_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    v_derivative_targets
  );
  if v_primary_closure_sha256 is distinct from
      p_request #>> '{expected,primary_closure_sha256}'
    or v_derivative_target_set_sha256 is distinct from
      p_request #>> '{expected,derivative_target_set_sha256}' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_CLOSURE_HASH_MISMATCH', 'status', 409,
      'message', 'Primary or derivative target closure hash is not exact',
      'primary_closure_sha256', v_primary_closure_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256
    );
  end if;

  for v_mapping in
    select mapping.*
    from util.dataset_flow_identity_mappings as mapping
    where mapping.scope_id = p_scope_id
    order by mapping.ordinal
  loop
    v_mapping_validation := util.dataset_flow_identity_validate_mapping(
      v_actor, v_mapping.mapping, v_scope.compatibility_policy,
      v_scope.support_snapshots, v_mapping.ordinal
    );
    if coalesce((v_mapping_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_FINALIZE_MAPPING_DRIFT', 'status', 409,
        'message', 'A source/public/support row changed after scope seal',
        'details', v_mapping_validation
      );
    end if;
  end loop;

  select count(*)::bigint into v_approved_reference_residue
  from public.processes as process
  cross join lateral jsonb_array_elements(
    private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
  ) as exchange(value)
  join util.dataset_flow_identity_mappings as mapping
    on mapping.scope_id = p_scope_id
    and exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
      = mapping.source_id::text
    and exchange.value #>> '{referenceToFlowDataSet,@version}'
      = mapping.source_version
  where process.user_id = v_actor and process.state_code = 0;
  v_protected_proof := util.dataset_flow_identity_protected_closure(
    v_actor, v_scope.protected_closure
  );
  if v_approved_reference_residue <> 0
    or coalesce((v_protected_proof->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_REFERENCE_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Approved references remain or protected closure changed',
      'approved_reference_residue', v_approved_reference_residue,
      'protected_closure_proof', v_protected_proof
    );
  end if;

  v_derivative_set_proof :=
    util.read_dataset_flow_identity_derivative_set(v_actor, p_scope_id);
  if (v_derivative_set_proof->>'target_count')::integer
      is distinct from v_scope.process_count
    or coalesce(
      (v_derivative_set_proof->>'causal_terminal_proof')::boolean,
      false
    ) is false then
    return jsonb_build_object(
      'ok', (v_derivative_set_proof->>'failed_count')::integer = 0,
      'command', v_command,
      'schema_version', v_result_schema_version,
      'code', case when (v_derivative_set_proof->>'failed_count')::integer > 0
        then 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
        else 'FLOW_IDENTITY_DERIVATIVES_PENDING' end,
      'scope_id', p_scope_id,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', 'derivatives_pending',
      'process_count', v_scope.process_count,
      'completed_process_count', v_completed,
      'rewrite_count', v_scope.rewrite_count,
      'primary_closure_sha256', v_primary_closure_sha256,
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivatives_current', false,
      'derivatives_pending_count',
        (v_derivative_set_proof->>'pending_count')::integer,
      'derivatives_failed_count',
        (v_derivative_set_proof->>'failed_count')::integer,
      'compensation_required',
        (v_derivative_set_proof->>'failed_count')::integer > 0,
      'compensation_targets',
        v_derivative_set_proof->'compensation_targets',
      'derivative_proofs', v_derivative_set_proof->'targets',
      'derivative_proof_set_sha256',
        v_derivative_set_proof->>'proof_sha256',
      'terminal_proof_sha256', null,
      'automatic_retry', false,
      'replay', false
    );
  end if;

  v_terminal_proof_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(
    jsonb_build_object(
      'schema_version', 'dataset-flow-identity-terminal-proof.v1',
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'plan_sha256', v_scope.plan_sha256,
      'primary_closure_sha256', v_primary_closure_sha256,
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'protected_observed_sha256', v_protected_proof->>'observed_sha256',
      'source_universe_sha256', v_scope.source_universe_sha256,
      'support_snapshot_set_sha256', v_scope.support_snapshot_set_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivative_proof_set_sha256',
        v_derivative_set_proof->>'proof_sha256',
      'process_audit_count', v_audit_count,
      'rewrite_count', v_scope.rewrite_count
    )
  );
  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null,
    jsonb_build_object(
      'record_type', 'scope_terminal',
      'schema_version', v_schema_version,
      'scope_id', p_scope_id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'final_request_sha256', v_request_sha256,
      'primary_closure_sha256', v_primary_closure_sha256,
      'protected_closure_sha256', v_scope.protected_closure_sha256,
      'source_universe_sha256', v_scope.source_universe_sha256,
      'support_snapshot_set_sha256', v_scope.support_snapshot_set_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivative_proof_set_sha256',
        v_derivative_set_proof->>'proof_sha256',
      'terminal_proof_sha256', v_terminal_proof_sha256,
      'process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'process_audit_count', v_audit_count,
      'hash_algorithm', 'sorted-key-compact-json-v1-sha256'
    )
  ) returning id into v_final_audit_id;
  update util.dataset_flow_identity_process_ledger
  set active = false where scope_id = p_scope_id;
  update util.dataset_flow_identity_scopes
  set status = 'completed', final_request_sha256 = v_request_sha256,
    terminal_proof_sha256 = v_terminal_proof_sha256,
    completed_at = clock_timestamp(), updated_at = clock_timestamp()
  where id = p_scope_id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', v_result_schema_version,
    'scope_id', p_scope_id, 'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', 'completed',
    'process_count', v_scope.process_count,
    'completed_process_count', v_completed,
    'rewrite_count', v_scope.rewrite_count,
    'primary_closure_sha256', v_primary_closure_sha256,
    'protected_closure_sha256', v_scope.protected_closure_sha256,
    'derivative_target_set_sha256', v_derivative_target_set_sha256,
    'derivative_proof_set_sha256',
      v_derivative_set_proof->>'proof_sha256',
    'derivatives_current', true,
    'terminal_proof_sha256', v_terminal_proof_sha256,
    'audit_id', v_final_audit_id::text,
    'replay', false
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_FINALIZE_LOCK_BUSY', 'status', 409,
    'message', 'Finalization could not acquire its bounded lock'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb
) owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb
) to authenticated;

comment on function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb
) is
  'Actor-only set-based Step 3 terminal verifier. It aggregates all primary and derivative proof rows once, requires exact source/support/protected closure, and never invokes a per-child reader or builds arrays by repeated concatenation.';

-- Step 3 v2 capture/attestation.  All full-row hashes below originate in the
-- database and are never accepted from an artifact or recomputed by a client.

create or replace function private.dataset_flow_identity_build_flow_guard_v2(
  p_actor uuid,
  p_endpoint jsonb,
  p_target boolean
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_flow public.flows%rowtype;
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_flow_properties jsonb;
  v_reference_internal_id text;
  v_fp_id uuid;
  v_fp_version text;
  v_ug_id uuid;
  v_ug_version text;
  v_reference_unit_internal_id text;
  v_payload_sha256 text;
  v_category_sha256 text;
  v_row_sha256 text;
  v_guard jsonb;
  v_reference jsonb;
  v_expected_uri text;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(
      p_endpoint,
      case when p_target
        then array['id', 'version', 'reference']
        else array['id', 'version', 'source_trace_sha256']
      end
    )
    or p_endpoint->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_endpoint->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or (not p_target
      and p_endpoint->>'source_trace_sha256' !~ '^[a-f0-9]{64}$') then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_ENDPOINT_SCHEMA_MISMATCH';
  end if;

  if p_target then
    v_reference := p_endpoint->'reference';
    v_expected_uri := '../flows/' || (p_endpoint->>'id') || '_'
      || (p_endpoint->>'version') || '.xml';
    if not private.dataset_flow_identity_exact_keys(
        v_reference,
        array[
          '@refObjectId', '@type', '@uri', '@version',
          'common:shortDescription'
        ]
      )
      or exists (
        select 1
        from unnest(array[
          '@refObjectId', '@type', '@uri', '@version'
        ]) as field(name)
        where jsonb_typeof(v_reference->field.name) <> 'string'
      )
      or v_reference->>'@refObjectId' <> p_endpoint->>'id'
      or v_reference->>'@version' <> p_endpoint->>'version'
      or v_reference->>'@type' <> 'flow data set'
      or v_reference->>'@uri' <> v_expected_uri
      or not private.dataset_flow_identity_short_description_v2(
        v_reference->'common:shortDescription'
      ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_TARGET_REFERENCE_MISMATCH';
    end if;
  end if;

  select flow.* into v_flow
  from public.flows as flow
  where flow.id = (p_endpoint->>'id')::uuid
    and btrim(flow.version::text) = p_endpoint->>'version';

  if v_flow.id is null
    or v_flow.json is null or v_flow.json_ordered is null
    or v_flow.json::jsonb is distinct from v_flow.json_ordered::jsonb
    or (p_target and v_flow.state_code <> 100)
    or (not p_target and v_flow.state_code <> 0)
    or (p_target and (v_flow.user_id is null or v_flow.user_id = p_actor))
    or (not p_target and v_flow.user_id is distinct from p_actor)
    or v_flow.json #>>
      '{flowDataSet,flowInformation,dataSetInformation,common:UUID}'
      is distinct from p_endpoint->>'id'
    or v_flow.json #>>
      '{flowDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from p_endpoint->>'version'
    or v_flow.json #>>
      '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      is distinct from 'Elementary flow' then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_FLOW_LIVE_MISMATCH';
  end if;

  v_flow_properties := v_flow.json #> '{flowDataSet,flowProperties,flowProperty}';
  if jsonb_typeof(v_flow_properties) = 'object' then
    v_flow_properties := jsonb_build_array(v_flow_properties);
  end if;
  v_reference_internal_id := v_flow.json #>>
    '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  if jsonb_typeof(v_flow_properties) <> 'array'
    or nullif(v_reference_internal_id, '') is null then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_FLOW_PROPERTY_MISSING';
  end if;

  select
    (item.value #>> '{referenceToFlowPropertyDataSet,@refObjectId}')::uuid,
    item.value #>> '{referenceToFlowPropertyDataSet,@version}'
  into v_fp_id, v_fp_version
  from jsonb_array_elements(v_flow_properties) as item(value)
  where item.value->>'@dataSetInternalID' = v_reference_internal_id;

  select support.* into v_flowproperty
  from public.flowproperties as support
  where support.id = v_fp_id
    and btrim(support.version::text) = v_fp_version
    and (
      (
        p_target
        and support.state_code = 100
        and support.user_id is not null
        and support.user_id <> p_actor
      )
      or (
        not p_target
        and (
          support.state_code = 100
          or (support.user_id = p_actor and support.state_code = 0)
        )
      )
    );
  if v_flowproperty.id is null
    or v_flowproperty.json is null or v_flowproperty.json_ordered is null
    or v_flowproperty.json::jsonb
      is distinct from v_flowproperty.json_ordered::jsonb then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_FLOW_PROPERTY_INVALID';
  end if;

  begin
    v_ug_id := (v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@refObjectId}')::uuid;
    v_ug_version := v_flowproperty.json #>>
      '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@version}';
  exception when others then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_UNIT_GROUP_REFERENCE_INVALID';
  end;

  select support.* into v_unitgroup
  from public.unitgroups as support
  where support.id = v_ug_id
    and btrim(support.version::text) = v_ug_version
    and (
      (
        p_target
        and support.state_code = 100
        and support.user_id is not null
        and support.user_id <> p_actor
      )
      or (
        not p_target
        and (
          support.state_code = 100
          or (support.user_id = p_actor and support.state_code = 0)
        )
      )
    );
  v_reference_unit_internal_id := v_unitgroup.json #>>
    '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}';
  if v_unitgroup.id is null
    or v_unitgroup.json is null or v_unitgroup.json_ordered is null
    or v_unitgroup.json::jsonb is distinct from v_unitgroup.json_ordered::jsonb
    or nullif(v_reference_unit_internal_id, '') is null
    or not exists (
      select 1
      from jsonb_array_elements(
        case jsonb_typeof(v_unitgroup.json #> '{unitGroupDataSet,units,unit}')
          when 'array' then v_unitgroup.json #> '{unitGroupDataSet,units,unit}'
          when 'object' then jsonb_build_array(
            v_unitgroup.json #> '{unitGroupDataSet,units,unit}'
          )
          else '[]'::jsonb
        end
      ) as unit_item(value)
      where unit_item.value->>'@dataSetInternalID'
          = v_reference_unit_internal_id
        and (unit_item.value->>'meanValue')::numeric = 1
    ) then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_REFERENCE_UNIT_INVALID';
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(
    v_flow.json_ordered::jsonb
  );
  v_category_sha256 := util.dataset_flow_identity_sha256(coalesce(
    v_flow.json #>
      '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}',
    'null'::jsonb
  ));
  v_row_sha256 := private.dataset_flow_identity_row_sha256(
    v_flow.id, btrim(v_flow.version::text), v_flow.user_id,
    v_flow.state_code, v_flow.modified_at, v_payload_sha256
  );
  v_guard := jsonb_build_object(
    'id', v_flow.id,
    'version', btrim(v_flow.version::text),
    'user_id', v_flow.user_id,
    'state_code', v_flow.state_code,
    'modified_at', v_flow.modified_at,
    'payload_sha256', v_payload_sha256,
    'row_sha256', v_row_sha256,
    'flow_type', 'Elementary flow',
    'flow_property_id', v_fp_id,
    'flow_property_version', v_fp_version,
    'unit_group_id', v_ug_id,
    'unit_group_version', v_ug_version,
    'category_path_sha256', v_category_sha256
  );
  if p_target then
    if not (
      private.dataset_flow_identity_text_values(
        v_reference->'common:shortDescription'
      ) && private.dataset_flow_identity_text_values(
        v_flow.json #>
          '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
      )
    ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_TARGET_NAME_MISMATCH';
    end if;
    return v_guard || jsonb_build_object('reference', v_reference);
  end if;
  return v_guard || jsonb_build_object(
    'source_trace_sha256', p_endpoint->>'source_trace_sha256'
  );
end;
$$;

alter function private.dataset_flow_identity_build_flow_guard_v2(
  uuid, jsonb, boolean
) owner to postgres;
revoke all on function private.dataset_flow_identity_build_flow_guard_v2(
  uuid, jsonb, boolean
) from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_build_support_set_v2(
  p_actor uuid,
  p_mappings jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_result jsonb := '[]'::jsonb;
  v_claim record;
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_payload jsonb;
  v_user_id uuid;
  v_state_code integer;
  v_modified_at timestamp with time zone;
  v_payload_sha256 text;
  v_row_sha256 text;
  v_ordinal integer := 0;
  v_validation jsonb;
begin
  for v_claim in
    with endpoint as (
      select guard.value
      from jsonb_array_elements(p_mappings) as mapping(value)
      cross join lateral jsonb_array_elements(jsonb_build_array(
        mapping.value->'source', mapping.value->'target'
      )) as guard(value)
    ), claimed as (
      select 'flowproperties'::text as support_table,
        (value->>'flow_property_id')::uuid as id,
        value->>'flow_property_version' as version
      from endpoint
      union
      select 'unitgroups', (value->>'unit_group_id')::uuid,
        value->>'unit_group_version'
      from endpoint
    )
    select * from claimed order by support_table, id, version
  loop
    v_ordinal := v_ordinal + 1;
    if v_claim.support_table = 'flowproperties' then
      select support.* into v_flowproperty
      from public.flowproperties as support
      where support.id = v_claim.id
        and btrim(support.version::text) = v_claim.version;
      if v_flowproperty.id is null
        or v_flowproperty.json is null or v_flowproperty.json_ordered is null
        or v_flowproperty.json::jsonb
          is distinct from v_flowproperty.json_ordered::jsonb then
        raise exception using errcode = '22023',
          message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_INVALID';
      end if;
      v_payload := v_flowproperty.json_ordered::jsonb;
      v_user_id := v_flowproperty.user_id;
      v_state_code := v_flowproperty.state_code;
      v_modified_at := v_flowproperty.modified_at;
    else
      select support.* into v_unitgroup
      from public.unitgroups as support
      where support.id = v_claim.id
        and btrim(support.version::text) = v_claim.version;
      if v_unitgroup.id is null
        or v_unitgroup.json is null or v_unitgroup.json_ordered is null
        or v_unitgroup.json::jsonb
          is distinct from v_unitgroup.json_ordered::jsonb then
        raise exception using errcode = '22023',
          message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_INVALID';
      end if;
      v_payload := v_unitgroup.json_ordered::jsonb;
      v_user_id := v_unitgroup.user_id;
      v_state_code := v_unitgroup.state_code;
      v_modified_at := v_unitgroup.modified_at;
    end if;
    if v_state_code not in (0, 100)
      or (v_state_code = 0 and v_user_id is distinct from p_actor) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_VISIBILITY_MISMATCH';
    end if;
    v_payload_sha256 := util.dataset_flow_identity_sha256(v_payload);
    v_row_sha256 := private.dataset_flow_identity_row_sha256(
      v_claim.id, v_claim.version, v_user_id, v_state_code,
      v_modified_at, v_payload_sha256
    );
    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'ordinal', v_ordinal,
      'table', v_claim.support_table,
      'id', v_claim.id,
      'version', v_claim.version,
      'user_id', v_user_id,
      'state_code', v_state_code,
      'modified_at', v_modified_at,
      'payload_sha256', v_payload_sha256,
      'row_sha256', v_row_sha256
    ));
  end loop;
  v_validation := util.dataset_flow_identity_validate_support_set(
    p_actor, v_result, util.dataset_flow_identity_sha256(v_result)
  );
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_SET_INVALID';
  end if;
  return v_result;
end;
$$;

alter function private.dataset_flow_identity_build_support_set_v2(
  uuid, jsonb
) owner to postgres;
revoke all on function private.dataset_flow_identity_build_support_set_v2(
  uuid, jsonb
) from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_build_protected_v2(
  p_actor uuid,
  p_intent jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_partition text;
  v_entry jsonb;
  v_occurrence jsonb;
  v_process public.processes%rowtype;
  v_exchanges jsonb;
  v_exchange jsonb;
  v_reference jsonb;
  v_occurrences jsonb;
  v_entries jsonb;
  v_pending jsonb := '[]'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_orphans jsonb := '[]'::jsonb;
  v_result jsonb;
  v_validation jsonb;
begin
  if not private.dataset_flow_identity_exact_keys(
      p_intent, array['schema_version', 'pending', 'blockers', 'orphans']
    )
    or p_intent->>'schema_version'
      <> 'dataset-flow-identity-protected-intent.v2'
    or jsonb_typeof(p_intent->'pending') <> 'array'
    or jsonb_typeof(p_intent->'blockers') <> 'array'
    or jsonb_typeof(p_intent->'orphans') <> 'array' then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_SCHEMA_MISMATCH';
  end if;

  foreach v_partition in array array['pending', 'blockers'] loop
    v_entries := '[]'::jsonb;
    for v_entry in
      select item.value
      from jsonb_array_elements(p_intent->v_partition)
        with ordinality as item(value, ordinality)
      order by item.ordinality
    loop
      if not private.dataset_flow_identity_exact_keys(v_entry, array[
          'source_id', 'source_version', 'expected_reference_count',
          'occurrences', 'evidence_sha256'
        ])
        or v_entry->>'source_id'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or v_entry->>'source_version'
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or v_entry->>'evidence_sha256' !~ '^[a-f0-9]{64}$'
        or jsonb_typeof(v_entry->'expected_reference_count') <> 'number'
        or (v_entry->>'expected_reference_count')::integer < 0
        or jsonb_typeof(v_entry->'occurrences') <> 'array'
        or jsonb_array_length(v_entry->'occurrences')
          <> (v_entry->>'expected_reference_count')::integer then
        raise exception using errcode = '22023',
          message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_ENTRY_MISMATCH';
      end if;
      v_occurrences := '[]'::jsonb;
      for v_occurrence in
        select item.value
        from jsonb_array_elements(v_entry->'occurrences') as item(value)
        order by item.value->>'process_id', item.value->>'process_version',
          (item.value->>'exchange_index')::integer
      loop
        if not private.dataset_flow_identity_exact_keys(v_occurrence, array[
            'process_id', 'process_version', 'exchange_index', 'internal_id',
            'direction'
          ])
          or v_occurrence->>'process_id'
            !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or v_occurrence->>'process_version'
            !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
          or jsonb_typeof(v_occurrence->'exchange_index') <> 'number'
          or (v_occurrence->>'exchange_index')::integer < 0
          or nullif(v_occurrence->>'internal_id', '') is null
          or v_occurrence->>'direction' not in ('Input', 'Output') then
          raise exception using errcode = '22023',
            message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_OCCURRENCE_MISMATCH';
        end if;
        select process.* into v_process
        from public.processes as process
        where process.id = (v_occurrence->>'process_id')::uuid
          and btrim(process.version::text) = v_occurrence->>'process_version'
          and process.user_id = p_actor and process.state_code = 0;
        v_exchanges := private.dataset_flow_identity_exchanges(
          v_process.json_ordered::jsonb
        );
        if v_process.id is null
          or v_process.json is null or v_process.json_ordered is null
          or v_process.json::jsonb is distinct from v_process.json_ordered::jsonb
          or (v_occurrence->>'exchange_index')::integer
            >= coalesce(jsonb_array_length(v_exchanges), 0) then
          raise exception using errcode = '22023',
            message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_PROCESS_MISMATCH';
        end if;
        v_exchange := v_exchanges->(v_occurrence->>'exchange_index')::integer;
        v_reference := private.dataset_flow_identity_reference(v_exchange);
        if v_exchange->>'@dataSetInternalID'
            is distinct from v_occurrence->>'internal_id'
          or v_exchange->>'exchangeDirection'
            is distinct from v_occurrence->>'direction'
          or v_reference->>'@refObjectId'
            is distinct from v_entry->>'source_id'
          or v_reference->>'@version'
            is distinct from v_entry->>'source_version'
          or not private.dataset_flow_identity_short_description_v2(
            v_reference->'common:shortDescription'
          ) then
          raise exception using errcode = '22023',
            message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_LOCATOR_DRIFT';
        end if;
        v_occurrences := v_occurrences || jsonb_build_array(
          v_occurrence || jsonb_build_object(
            'reference_sha256', util.dataset_flow_identity_sha256(v_reference)
          )
        );
      end loop;
      v_entries := v_entries || jsonb_build_array(jsonb_build_object(
        'source_id', v_entry->>'source_id',
        'source_version', v_entry->>'source_version',
        'expected_reference_count', jsonb_array_length(v_occurrences),
        'occurrences', v_occurrences,
        'occurrence_set_sha256',
          util.dataset_flow_identity_restricted_sha256_v2(v_occurrences),
        'evidence_sha256', v_entry->>'evidence_sha256'
      ));
    end loop;
    if v_partition = 'pending' then
      v_pending := v_entries;
    else
      v_blockers := v_entries;
    end if;
  end loop;

  select coalesce(jsonb_agg(jsonb_build_object(
    'source_id', item.value->>'source_id',
    'source_version', item.value->>'source_version',
    'evidence_sha256', item.value->>'evidence_sha256'
  ) order by item.ordinality), '[]'::jsonb)
  into v_orphans
  from jsonb_array_elements(p_intent->'orphans')
    with ordinality as item(value, ordinality)
  where private.dataset_flow_identity_exact_keys(
      item.value, array['source_id', 'source_version', 'evidence_sha256']
    )
    and item.value->>'source_id'
      ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and item.value->>'source_version'
      ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    and item.value->>'evidence_sha256' ~ '^[a-f0-9]{64}$';
  if jsonb_array_length(v_orphans)
      <> jsonb_array_length(p_intent->'orphans') then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_ORPHAN_SCHEMA_MISMATCH';
  end if;
  v_result := jsonb_build_object(
    'schema_version', 'dataset-flow-identity-protected-closure.v1',
    'pending', v_pending,
    'blockers', v_blockers,
    'orphans', v_orphans,
    'pending_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_pending),
    'blocker_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_blockers),
    'orphan_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_orphans),
    'total_expected_reference_count',
      coalesce((select sum((item.value->>'expected_reference_count')::integer)
        from jsonb_array_elements(v_pending || v_blockers) as item(value)), 0)
  );
  v_validation := util.dataset_flow_identity_protected_closure(p_actor, v_result);
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_LIVE_MISMATCH';
  end if;
  return v_result;
end;
$$;

alter function private.dataset_flow_identity_build_protected_v2(uuid, jsonb)
  owner to postgres;
revoke all on function private.dataset_flow_identity_build_protected_v2(
  uuid, jsonb
) from public, anon, authenticated, service_role;

create or replace function private.dataset_flow_identity_build_process_v2(
  p_actor uuid,
  p_intent jsonb,
  p_mappings jsonb,
  p_policy jsonb,
  p_supports jsonb,
  p_protected_closure_sha256 text
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_process public.processes%rowtype;
  v_before_payload jsonb;
  v_before_exchanges jsonb;
  v_after_exchanges jsonb;
  v_desired_payload jsonb;
  v_thin jsonb;
  v_mapping jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_source_reference jsonb;
  v_target_reference jsonb;
  v_rewrite jsonb;
  v_rewrites jsonb := '[]'::jsonb;
  v_collision jsonb;
  v_snapshot jsonb;
  v_before_payload_sha256 text;
  v_before_exchange_sha256 text;
  v_desired_payload_sha256 text;
  v_desired_exchange_sha256 text;
  v_manifest jsonb;
  v_validation jsonb;
begin
  if jsonb_typeof(p_mappings) <> 'object'
    or not private.dataset_flow_identity_exact_keys(p_mappings, array[
      'schema_version', 'mapping_count', 'mapping_guard_set_sha256',
      'mappings', 'by_ordinal', 'by_id', 'by_source'
    ])
    or p_mappings->>'schema_version'
      <> 'dataset-flow-identity-mapping-index.v2'
    or jsonb_typeof(p_mappings->'by_ordinal') <> 'object'
    or jsonb_typeof(p_mappings->'by_id') <> 'object'
    or jsonb_typeof(p_mappings->'by_source') <> 'object'
    or not private.dataset_flow_identity_exact_keys(p_intent, array[
      'ordinal', 'id', 'version', 'rewrites', 'process_schema'
    ])
    or jsonb_typeof(p_intent->'ordinal') <> 'number'
    or (p_intent->>'ordinal')::integer <= 0
    or p_intent->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_intent->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(p_intent->'rewrites') <> 'array'
    or jsonb_array_length(p_intent->'rewrites') <= 0
    or not private.dataset_flow_identity_exact_keys(
      p_intent->'process_schema', array['status', 'evidence_sha256']
    )
    or p_intent #>> '{process_schema,status}' <> 'pass'
    or p_intent #>> '{process_schema,evidence_sha256}'
      !~ '^[a-f0-9]{64}$' then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_INTENT_SCHEMA_MISMATCH';
  end if;

  select process.* into v_process
  from public.processes as process
  where process.id = (p_intent->>'id')::uuid
    and btrim(process.version::text) = p_intent->>'version'
    and process.user_id = p_actor and process.state_code = 0;
  if v_process.id is null
    or v_process.json is null or v_process.json_ordered is null
    or v_process.json::jsonb is distinct from v_process.json_ordered::jsonb then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_LIVE_MISMATCH';
  end if;
  v_before_payload := v_process.json_ordered::jsonb;
  v_before_exchanges := private.dataset_flow_identity_exchanges(v_before_payload);
  v_after_exchanges := v_before_exchanges;
  if v_before_exchanges is null then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_EXCHANGES_INVALID';
  end if;

  for v_thin in
    select item.value
    from jsonb_array_elements(p_intent->'rewrites')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(v_thin, array[
        'ordinal', 'exchange_index', 'internal_id', 'direction',
        'mapping_ordinal'
      ])
      or jsonb_typeof(v_thin->'ordinal') <> 'number'
      or (v_thin->>'ordinal')::integer <= 0
      or jsonb_typeof(v_thin->'exchange_index') <> 'number'
      or (v_thin->>'exchange_index')::integer < 0
      or (v_thin->>'exchange_index')::integer
        >= jsonb_array_length(v_before_exchanges)
      or jsonb_typeof(v_thin->'mapping_ordinal') <> 'number'
      or (v_thin->>'mapping_ordinal')::integer <= 0
      or nullif(v_thin->>'internal_id', '') is null
      or v_thin->>'direction' not in ('Input', 'Output') then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_REWRITE_INTENT_MISMATCH';
    end if;
    v_mapping := p_mappings #> array[
      'by_ordinal', (v_thin->>'mapping_ordinal')::integer::text
    ];
    if v_mapping is null then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_REWRITE_MAPPING_MISSING';
    end if;
    v_before_exchange := v_after_exchanges->(v_thin->>'exchange_index')::integer;
    v_source_reference := private.dataset_flow_identity_reference(v_before_exchange);
    v_target_reference := v_mapping #> '{target,reference}';
    if v_before_exchange->>'@dataSetInternalID'
        is distinct from v_thin->>'internal_id'
      or v_before_exchange->>'exchangeDirection'
        is distinct from v_thin->>'direction'
      or v_source_reference->>'@refObjectId'
        is distinct from v_mapping #>> '{source,id}'
      or v_source_reference->>'@version'
        is distinct from v_mapping #>> '{source,version}'
      or not private.dataset_flow_identity_short_description_v2(
        v_source_reference->'common:shortDescription'
      )
      or not private.dataset_flow_identity_short_description_v2(
        v_target_reference->'common:shortDescription'
      ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_REWRITE_LOCATOR_DRIFT';
    end if;
    v_rewrite := jsonb_build_object(
      'ordinal', v_thin->'ordinal',
      'exchange_index', v_thin->'exchange_index',
      'internal_id', v_thin->>'internal_id',
      'direction', v_thin->>'direction',
      'mapping_id', v_mapping->>'mapping_id',
      'source_reference', v_source_reference,
      'target_reference', v_target_reference,
      'before_reference_sha256',
        util.dataset_flow_identity_sha256(v_source_reference),
      'after_reference_sha256',
        util.dataset_flow_identity_sha256(v_target_reference)
    );
    v_rewrites := v_rewrites || jsonb_build_array(v_rewrite);
    v_after_exchange := jsonb_set(
      v_before_exchange,
      '{referenceToFlowDataSet}',
      (v_before_exchange->'referenceToFlowDataSet') || v_target_reference,
      false
    );
    if v_after_exchange - 'referenceToFlowDataSet'
        is distinct from v_before_exchange - 'referenceToFlowDataSet'
      or (v_after_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription'
        is distinct from
        (v_before_exchange->'referenceToFlowDataSet')
          - '@refObjectId' - '@type' - '@uri' - '@version'
          - 'common:shortDescription' then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_FIVE_FIELD_BOUNDARY_FAILED';
    end if;
    v_after_exchanges := jsonb_set(
      v_after_exchanges,
      array[(v_thin->>'exchange_index')::integer::text],
      v_after_exchange,
      false
    );
  end loop;
  if jsonb_array_length(v_rewrites)
      <> jsonb_array_length(p_intent->'rewrites') then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_REWRITE_COUNT_MISMATCH';
  end if;

  v_collision := private.dataset_flow_identity_collision_ledger(
    v_after_exchanges, v_rewrites
  );
  v_desired_payload := private.dataset_flow_identity_replace_exchanges(
    v_before_payload, v_after_exchanges
  );
  v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  if v_desired_payload is null or v_snapshot is null then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_DESIRED_OR_DERIVATIVE_INVALID';
  end if;
  v_before_payload_sha256 := util.dataset_flow_identity_sha256(v_before_payload);
  v_before_exchange_sha256 := util.dataset_flow_identity_sha256(v_before_exchanges);
  v_desired_payload_sha256 := util.dataset_flow_identity_sha256(v_desired_payload);
  v_desired_exchange_sha256 := util.dataset_flow_identity_sha256(v_after_exchanges);
  v_manifest := jsonb_build_object(
    'ordinal', p_intent->'ordinal',
    'id', v_process.id,
    'version', btrim(v_process.version::text),
    'user_id', v_process.user_id,
    'state_code', v_process.state_code,
    'modified_at', v_process.modified_at,
    'model_id', v_process.model_id,
    'rule_verification', v_process.rule_verification,
    'before_row_sha256', util.dataset_flow_identity_sha256(jsonb_build_object(
      'id', v_process.id,
      'version', btrim(v_process.version::text),
      'user_id', v_process.user_id,
      'state_code', v_process.state_code,
      'modified_at', v_process.modified_at,
      'model_id', v_process.model_id,
      'rule_verification', v_process.rule_verification,
      'payload_sha256', v_before_payload_sha256
    )),
    'before_payload_sha256', v_before_payload_sha256,
    'before_exchange_set_sha256', v_before_exchange_sha256,
    'before_exchange_count', jsonb_array_length(v_before_exchanges),
    'desired_payload_sha256', v_desired_payload_sha256,
    'desired_exchange_set_sha256', v_desired_exchange_sha256,
    'rewrite_count', jsonb_array_length(v_rewrites),
    'rewrite_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_rewrites),
    'rewrites', v_rewrites,
    'collision_ledger', v_collision,
    'collision_ledger_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_collision),
    'derivative_baseline_snapshot_sha256', v_snapshot->>'snapshot_sha256',
    'process_schema', p_intent->'process_schema',
    'pending_blocker_closure_sha256', p_protected_closure_sha256
  );
  v_manifest := v_manifest || jsonb_build_object(
    'process_template_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_manifest)
  );
  v_validation := util.dataset_flow_identity_dry_validate_process(
    p_actor, v_manifest, p_mappings, p_policy, p_supports
  );
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_DRY_VALIDATION_FAILED';
  end if;
  return v_manifest;
end;
$$;

alter function private.dataset_flow_identity_build_process_v2(
  uuid, jsonb, jsonb, jsonb, jsonb, text
) owner to postgres;
revoke all on function private.dataset_flow_identity_build_process_v2(
  uuid, jsonb, jsonb, jsonb, jsonb, text
) from public, anon, authenticated, service_role;

create or replace function public.cmd_dataset_flow_identity_capture_attest_guarded(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '180s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_capture_attest_guarded';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_request_sha256 text;
  v_context jsonb;
  v_existing util.dataset_flow_identity_capture_receipts%rowtype;
  v_receipt_id uuid := gen_random_uuid();
  v_captured_at timestamp with time zone := clock_timestamp();
  v_expires_at timestamp with time zone;
  v_mapping_intent jsonb;
  v_mapping jsonb;
  v_mapping_body jsonb;
  v_mappings jsonb := '[]'::jsonb;
  v_mapping_index jsonb;
  v_source_guard jsonb;
  v_target_guard jsonb;
  v_source_records jsonb := '[]'::jsonb;
  v_target_records jsonb := '[]'::jsonb;
  v_source_ordinal integer := 0;
  v_partition text;
  v_protected_entry jsonb;
  v_protected jsonb;
  v_supports jsonb;
  v_process_records jsonb := '[]'::jsonb;
  v_processes jsonb := '[]'::jsonb;
  v_source_universe jsonb;
  v_source_guard_set_sha256 text;
  v_target_guard_set_sha256 text;
  v_mapping_guard_set_sha256 text;
  v_process_intent_set_sha256 text;
  v_support_set_sha256 text;
  v_protected_sha256 text;
  v_source_universe_sha256 text;
  v_mapping_set_sha256 text;
  v_process_manifest_sha256 text;
  v_receipt_proof_sha256 text;
  v_whole_scope_proof_sha256 text;
  v_rewrite_count integer;
  v_validation jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if v_request is null
    or pg_column_size(v_request) > 134217728
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'environment', 'project_ref', 'actor',
      'target_visibility', 'operation_id', 'compatibility_policy',
      'artifact_evidence', 'mappings', 'process_intents', 'protected_closure'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-capture-attest.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}'))
      is distinct from v_actor_email
    or not private.dataset_flow_identity_exact_keys(
      v_request->'artifact_evidence', array[
        'review_ledger_sha256', 'live_capture_artifact_sha256',
        'toolchain_evidence_sha256'
      ]
    )
    or exists (
      select 1 from unnest(array[
        'review_ledger_sha256', 'live_capture_artifact_sha256',
        'toolchain_evidence_sha256'
      ]) as field(name)
      where v_request->'artifact_evidence'->>field.name
        !~ '^[a-f0-9]{64}$'
    )
    or jsonb_typeof(v_request->'mappings') <> 'array'
    or jsonb_array_length(v_request->'mappings') not between 1 and 305
    or jsonb_typeof(v_request->'process_intents') <> 'array'
    or jsonb_array_length(v_request->'process_intents') not between 1 and 12000
    or not private.dataset_flow_identity_exact_keys(
      v_request->'compatibility_policy', array[
        'schema_version', 'policy_sha256', 'evidence_resolution_sha256',
        'approved_at_utc', 'approval_text_sha256'
      ]
    )
    or v_request #>> '{compatibility_policy,schema_version}'
      <> 'dataset-flow-identity-compatibility-policy.v1'
    or exists (
      select 1 from unnest(array[
        'policy_sha256', 'evidence_resolution_sha256', 'approval_text_sha256'
      ]) as field(name)
      where v_request->'compatibility_policy'->>field.name
        !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CAPTURE_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 capture request schema or safe-JSON domain mismatch'
    );
  end if;
  begin
    perform (v_request #>>
      '{compatibility_policy,approved_at_utc}')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CAPTURE_POLICY_TIME_INVALID', 'status', 400,
      'message', 'Compatibility policy approval time is invalid'
    );
  end;
  v_context := util.dataset_alias_execution_server_context();
  if v_request->>'environment' is distinct from v_context->>'environment'
    or v_request->>'project_ref' is distinct from v_context->>'project_ref' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CAPTURE_ENVIRONMENT_MISMATCH', 'status', 409,
      'message', 'Capture does not target this database branch'
    );
  end if;
  -- Capture and every owner-row trigger share this actor fence.  No process
  -- or elementary-flow write can straddle the final receipt attestation.
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  v_request_sha256 := util.dataset_flow_identity_restricted_sha256_v2(v_request);
  select receipt.* into v_existing
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.actor_user_id = v_actor
    and receipt.request_id = (v_request->>'request_id')::uuid;
  if v_existing.id is not null then
    if v_existing.capture_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CAPTURE_REQUEST_REUSE_MISMATCH', 'status', 409,
        'message', 'Capture request ID is already bound differently'
      );
    elsif v_existing.expires_at <= clock_timestamp() then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CAPTURE_RECEIPT_EXPIRED', 'status', 409,
        'message', 'Capture receipt expired; create a fresh request ID'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', 'dataset-flow-identity-capture-attest-result.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'receipt_id', v_existing.id,
      'receipt_proof_sha256', v_existing.receipt_proof_sha256,
      'operation_id', v_existing.operation_id,
      'environment', v_existing.environment,
      'project_ref', v_existing.project_ref,
      'captured_at', v_existing.captured_at,
      'expires_at', v_existing.expires_at,
      'source_guard_set_sha256', v_existing.source_guard_set_sha256,
      'support_guard_set_sha256', v_existing.support_snapshot_set_sha256,
      'target_guard_set_sha256', v_existing.target_guard_set_sha256,
      'mapping_guard_set_sha256', v_existing.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_existing.process_intent_set_sha256,
      'protected_closure_sha256', v_existing.protected_closure_sha256,
      'whole_scope_proof_sha256', v_existing.whole_scope_proof_sha256,
      'policy_sha256', v_existing.compatibility_policy->>'policy_sha256',
      'policy_approval_text_sha256',
        v_existing.policy_approval_text_sha256,
      'source_count', v_existing.source_count,
      'target_count', v_existing.target_count,
      'support_count', v_existing.support_count,
      'mapping_count', v_existing.mapping_count,
      'process_count', v_existing.process_count,
      'rewrite_count', v_existing.rewrite_count,
      'capture_request_sha256', v_existing.capture_request_sha256,
      'replay', true
    );
  end if;

  v_protected := private.dataset_flow_identity_build_protected_v2(
    v_actor, v_request->'protected_closure'
  );
  v_protected_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_protected);

  for v_mapping_intent in
    select item.value
    from jsonb_array_elements(v_request->'mappings')
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    if not private.dataset_flow_identity_exact_keys(v_mapping_intent, array[
        'ordinal', 'source', 'target', 'compatibility'
      ])
      or jsonb_typeof(v_mapping_intent->'ordinal') <> 'number'
      or (v_mapping_intent->>'ordinal')::integer
        <> jsonb_array_length(v_mappings) + 1
      or not private.dataset_flow_identity_exact_keys(
        v_mapping_intent->'compatibility', array[
          'policy_sha256', 'mode', 'confidence',
          'flow_property_compatible', 'unit_group_compatible',
          'direction_compatible', 'compartment_compatible',
          'conversion_factor', 'evidence_sha256', 'flow_schema',
          'process_schema_required'
        ]
      )
      or v_mapping_intent #>> '{compatibility,policy_sha256}'
        is distinct from v_request #>> '{compatibility_policy,policy_sha256}'
      or v_mapping_intent #>> '{compatibility,mode}' <> 'identity'
      or v_mapping_intent #>> '{compatibility,confidence}' <> 'approved'
      or v_mapping_intent #>> '{compatibility,conversion_factor}' <> '1'
      or v_mapping_intent #>> '{compatibility,evidence_sha256}'
        !~ '^[a-f0-9]{64}$'
      or v_mapping_intent #>> '{compatibility,process_schema_required}'
        <> 'pass'
      or not private.dataset_flow_identity_exact_keys(
        v_mapping_intent #> '{compatibility,flow_schema}',
        array['status', 'warning_set_sha256']
      )
      or v_mapping_intent #>> '{compatibility,flow_schema,status}'
        not in ('pass', 'legacy_warning')
      or v_mapping_intent #>>
        '{compatibility,flow_schema,warning_set_sha256}'
        !~ '^[a-f0-9]{64}$'
      or exists (
        select 1 from unnest(array[
          'flow_property_compatible', 'unit_group_compatible',
          'direction_compatible', 'compartment_compatible'
        ]) as field(name)
        where v_mapping_intent->'compatibility'->>field.name <> 'true'
      ) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_MAPPING_INTENT_MISMATCH';
    end if;
    v_source_guard := private.dataset_flow_identity_build_flow_guard_v2(
      v_actor, v_mapping_intent->'source', false
    );
    v_target_guard := private.dataset_flow_identity_build_flow_guard_v2(
      v_actor, v_mapping_intent->'target', true
    );
    if v_source_guard->>'flow_property_id'
        is distinct from v_target_guard->>'flow_property_id'
      or v_source_guard->>'flow_property_version'
        is distinct from v_target_guard->>'flow_property_version'
      or v_source_guard->>'unit_group_id'
        is distinct from v_target_guard->>'unit_group_id'
      or v_source_guard->>'unit_group_version'
        is distinct from v_target_guard->>'unit_group_version' then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_MAPPING_NOT_IDENTITY_ONLY';
    end if;
    v_mapping_body := jsonb_build_object(
      'source', v_source_guard,
      'target', v_target_guard,
      'compatibility', v_mapping_intent->'compatibility'
    );
    v_mapping := jsonb_build_object(
      'ordinal', v_mapping_intent->'ordinal',
      'mapping_id',
        util.dataset_flow_identity_restricted_sha256_v2(v_mapping_body)
    ) || v_mapping_body;
    v_mappings := v_mappings || jsonb_build_array(v_mapping);
    v_source_ordinal := v_source_ordinal + 1;
    v_source_records := v_source_records || jsonb_build_array(
      jsonb_build_object(
        'ordinal', v_source_ordinal, 'disposition', 'mapped',
        'source_id', v_source_guard->>'id',
        'source_version', v_source_guard->>'version',
        'guard', v_source_guard,
        'evidence_sha256', v_source_guard->>'source_trace_sha256'
      )
    );
  end loop;

  foreach v_partition in array array['pending', 'blockers', 'orphans'] loop
    for v_protected_entry in
      select item.value
      from jsonb_array_elements(v_request->'protected_closure'->v_partition)
        with ordinality as item(value, ordinality)
      order by item.ordinality
    loop
      v_source_guard := private.dataset_flow_identity_build_flow_guard_v2(
        v_actor,
        jsonb_build_object(
          'id', v_protected_entry->>'source_id',
          'version', v_protected_entry->>'source_version',
          'source_trace_sha256', v_protected_entry->>'evidence_sha256'
        ),
        false
      );
      v_source_ordinal := v_source_ordinal + 1;
      v_source_records := v_source_records || jsonb_build_array(
        jsonb_build_object(
          'ordinal', v_source_ordinal,
          'disposition', case v_partition
            when 'blockers' then 'blocker'
            when 'orphans' then 'orphan'
            else 'pending' end,
          'source_id', v_source_guard->>'id',
          'source_version', v_source_guard->>'version',
          'guard', v_source_guard,
          'evidence_sha256', v_protected_entry->>'evidence_sha256'
        )
      );
    end loop;
  end loop;
  if jsonb_array_length(v_source_records) <> 305
    or (select count(distinct (item.value->>'source_id') || '@'
          || (item.value->>'source_version'))
        from jsonb_array_elements(v_source_records) as item(value)) <> 305 then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_SOURCE_CLOSURE_MISMATCH';
  end if;

  select coalesce(jsonb_agg(target order by target_id, target_version), '[]'::jsonb)
  into v_target_records
  from (
    select distinct on (
      item.value #>> '{target,id}', item.value #>> '{target,version}'
    )
      item.value #>> '{target,id}' as target_id,
      item.value #>> '{target,version}' as target_version,
      item.value->'target' as target
    from jsonb_array_elements(v_mappings) as item(value)
    order by item.value #>> '{target,id}', item.value #>> '{target,version}'
  ) as distinct_target;
  v_target_records := coalesce((
    select jsonb_agg(jsonb_build_object(
      'ordinal', item.ordinality,
      'target_id', item.value->>'id',
      'target_version', item.value->>'version',
      'guard', item.value
    ) order by item.ordinality)
    from jsonb_array_elements(v_target_records)
      with ordinality as item(value, ordinality)
  ), '[]'::jsonb);

  v_supports := private.dataset_flow_identity_build_support_set_v2(
    v_actor, v_mappings
  );
  for v_mapping in select item.value
    from jsonb_array_elements(v_mappings) as item(value)
  loop
    v_validation := util.dataset_flow_identity_validate_mapping(
      v_actor, v_mapping, v_request->'compatibility_policy', v_supports,
      (v_mapping->>'ordinal')::integer
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_MAPPING_LIVE_MISMATCH';
    end if;
  end loop;
  v_mapping_index :=
    private.dataset_flow_identity_mapping_index_v2(v_mappings);

  if exists (
    select 1
    from jsonb_array_elements(v_request->'process_intents')
      with ordinality as item(value, ordinality)
    where case when jsonb_typeof(item.value->'ordinal') = 'number'
      then (item.value->>'ordinal')::numeric = item.ordinality::numeric
      else false end is not true
  ) then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_ORDINAL_MISMATCH';
  end if;
  with built as materialized (
    select item.ordinality::integer as ordinal,
      private.dataset_flow_identity_build_process_v2(
        v_actor, item.value, v_mapping_index,
        v_request->'compatibility_policy', v_supports, v_protected_sha256
      ) as manifest
    from jsonb_array_elements(v_request->'process_intents')
      with ordinality as item(value, ordinality)
  )
  select
    coalesce(jsonb_agg(built.manifest order by built.ordinal), '[]'::jsonb),
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', built.manifest->'ordinal',
      'process_id', built.manifest->>'id',
      'process_version', built.manifest->>'version',
      'intent_proof_sha256',
        util.dataset_flow_identity_restricted_sha256_v2(built.manifest),
      'manifest', built.manifest
    ) order by built.ordinal), '[]'::jsonb),
    coalesce(sum((built.manifest->>'rewrite_count')::integer), 0)::integer
  into v_processes, v_process_records, v_rewrite_count
  from built;
  if v_rewrite_count <= 0 then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROCESS_CLOSURE_EMPTY';
  end if;

  select jsonb_agg(jsonb_build_object(
    'id', (item.value->>'source_id')::uuid,
    'version', item.value->>'source_version',
    'user_id', v_actor,
    'state_code', 0,
    'flow_type', 'Elementary flow'
  ) order by item.value->>'source_id', item.value->>'source_version')
  into v_source_universe
  from jsonb_array_elements(v_source_records) as item(value);
  v_source_universe_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_source_universe);
  v_validation := util.dataset_flow_identity_source_universe(
    v_actor, v_source_universe, v_source_universe_sha256
  );
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_SOURCE_UNIVERSE_LIVE_MISMATCH';
  end if;

  v_source_guard_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_source_records);
  v_target_guard_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_target_records);
  v_mapping_guard_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_mappings);
  v_process_intent_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_process_records);
  v_support_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_supports);
  v_mapping_set_sha256 := v_mapping_guard_set_sha256;
  v_process_manifest_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_processes);
  v_expires_at := v_captured_at + interval '7 days';
  v_whole_scope_proof_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
      'schema_version', 'dataset-flow-identity-capture-whole-scope.v2',
      'receipt_id', v_receipt_id,
      'actor_user_id', v_actor,
      'environment', v_request->>'environment',
      'project_ref', v_request->>'project_ref',
      'operation_id', v_request->>'operation_id',
      'capture_request_sha256', v_request_sha256,
      'source_guard_set_sha256', v_source_guard_set_sha256,
      'support_guard_set_sha256', v_support_set_sha256,
      'target_guard_set_sha256', v_target_guard_set_sha256,
      'mapping_guard_set_sha256', v_mapping_guard_set_sha256,
      'process_intent_set_sha256', v_process_intent_set_sha256,
      'protected_closure_sha256', v_protected_sha256,
      'captured_at', v_captured_at,
      'expires_at', v_expires_at
    ));
  v_receipt_proof_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'receipt_id', v_receipt_id,
      'whole_scope_proof_sha256', v_whole_scope_proof_sha256,
      'policy_sha256',
        v_request #>> '{compatibility_policy,policy_sha256}',
      'policy_approval_text_sha256',
        v_request #>> '{compatibility_policy,approval_text_sha256}'
    ));

  insert into util.dataset_flow_identity_capture_receipts (
    id, actor_user_id, actor_email, request_id, environment, project_ref,
    target_visibility, operation_id, compatibility_policy,
    policy_approval_text_sha256, artifact_evidence, protected_closure,
    protected_closure_sha256, source_universe, source_universe_sha256,
    support_snapshot_set_sha256, source_guard_set_sha256,
    target_guard_set_sha256, mapping_guard_set_sha256,
    process_intent_set_sha256, mapping_set_sha256,
    process_manifest_sha256, capture_request_sha256, receipt_proof_sha256,
    whole_scope_proof_sha256, source_count, target_count, support_count,
    mapping_count, process_count, rewrite_count, captured_at, expires_at
  ) values (
    v_receipt_id, v_actor, v_actor_email, (v_request->>'request_id')::uuid,
    v_request->>'environment', v_request->>'project_ref',
    'owner_draft', v_request->>'operation_id',
    v_request->'compatibility_policy',
    v_request #>> '{compatibility_policy,approval_text_sha256}',
    v_request->'artifact_evidence', v_protected, v_protected_sha256,
    v_source_universe, v_source_universe_sha256, v_support_set_sha256,
    v_source_guard_set_sha256, v_target_guard_set_sha256,
    v_mapping_guard_set_sha256, v_process_intent_set_sha256,
    v_mapping_set_sha256, v_process_manifest_sha256, v_request_sha256,
    v_receipt_proof_sha256, v_whole_scope_proof_sha256, 305,
    jsonb_array_length(v_target_records), jsonb_array_length(v_supports),
    jsonb_array_length(v_mappings), jsonb_array_length(v_processes),
    v_rewrite_count, v_captured_at, v_expires_at
  );
  insert into util.dataset_flow_identity_capture_source_guards (
    receipt_id, ordinal, disposition, source_id, source_version,
    guard, evidence_sha256
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      item.value->>'disposition', (item.value->>'source_id')::uuid,
      item.value->>'source_version', item.value->'guard',
      item.value->>'evidence_sha256'
    from jsonb_array_elements(v_source_records) as item(value);
  insert into util.dataset_flow_identity_capture_target_guards (
    receipt_id, ordinal, target_id, target_version, guard
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      (item.value->>'target_id')::uuid, item.value->>'target_version',
      item.value->'guard'
    from jsonb_array_elements(v_target_records) as item(value);
  insert into util.dataset_flow_identity_capture_support_guards (
    receipt_id, ordinal, support_table, support_id, support_version, guard
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      item.value->>'table', (item.value->>'id')::uuid,
      item.value->>'version', item.value
    from jsonb_array_elements(v_supports) as item(value);
  insert into util.dataset_flow_identity_capture_mapping_guards (
    receipt_id, ordinal, mapping_id, source_id, source_version,
    target_id, target_version, mapping
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      item.value->>'mapping_id', (item.value #>> '{source,id}')::uuid,
      item.value #>> '{source,version}',
      (item.value #>> '{target,id}')::uuid,
      item.value #>> '{target,version}', item.value
    from jsonb_array_elements(v_mappings) as item(value);
  insert into util.dataset_flow_identity_capture_process_intents (
    receipt_id, ordinal, process_id, process_version,
    intent_proof_sha256, manifest
  ) select v_receipt_id, (item.value->>'ordinal')::integer,
      (item.value->>'process_id')::uuid, item.value->>'process_version',
      item.value->>'intent_proof_sha256', item.value->'manifest'
    from jsonb_array_elements(v_process_records) as item(value);

  -- The helper is declared later in this migration, so use runtime resolution
  -- here.  It locks and revalidates all 305 sources, every support/target,
  -- every protected occurrence, and every intended process after the receipt
  -- relation has been fully inserted but before this transaction can return.
  execute
    'select private.dataset_flow_identity_whole_scope_proof_v2($1,$2,null,true)'
    into v_validation using v_actor, v_receipt_id;
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '40001',
      message = 'FLOW_IDENTITY_CAPTURE_FINAL_WHOLE_SCOPE_DRIFT';
  end if;

  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-capture-attest-result.v2',
    'proof_domain', 'dataset-flow-identity-db-proof.v2',
    'receipt_id', v_receipt_id,
    'receipt_proof_sha256', v_receipt_proof_sha256,
    'operation_id', v_request->>'operation_id',
    'environment', v_request->>'environment',
    'project_ref', v_request->>'project_ref',
    'captured_at', v_captured_at, 'expires_at', v_expires_at,
    'source_guard_set_sha256', v_source_guard_set_sha256,
    'support_guard_set_sha256', v_support_set_sha256,
    'target_guard_set_sha256', v_target_guard_set_sha256,
    'mapping_guard_set_sha256', v_mapping_guard_set_sha256,
    'process_intent_set_sha256', v_process_intent_set_sha256,
    'protected_closure_sha256', v_protected_sha256,
    'whole_scope_proof_sha256', v_whole_scope_proof_sha256,
    'policy_sha256',
      v_request #>> '{compatibility_policy,policy_sha256}',
    'policy_approval_text_sha256',
      v_request #>> '{compatibility_policy,approval_text_sha256}',
    'source_count', 305,
    'target_count', jsonb_array_length(v_target_records),
    'support_count', jsonb_array_length(v_supports),
    'mapping_count', jsonb_array_length(v_mappings),
    'process_count', jsonb_array_length(v_processes),
    'rewrite_count', v_rewrite_count,
    'capture_request_sha256', v_request_sha256,
    'replay', false
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', case when sqlstate = '55P03'
      then 'FLOW_IDENTITY_CAPTURE_LOCK_BUSY'
      else 'FLOW_IDENTITY_CAPTURE_FAILED' end,
    'status', case when sqlstate = '55P03' then 409 else 400 end,
    'message', sqlerrm, 'sqlstate', sqlstate
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_capture_attest_guarded(jsonb)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_capture_attest_guarded(
  jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_capture_attest_guarded(
  jsonb
) to authenticated;

comment on function public.cmd_dataset_flow_identity_capture_attest_guarded(
  jsonb
) is
  'Authenticated Step 3 v2 read-only capture attestation. It accepts typed semantic intent plus opaque artifact evidence, computes every live/before/desired/category/row hash in PostgreSQL, and persists an immutable relation-shaped receipt without mutating LCA datasets.';

create or replace function private.dataset_flow_identity_receipt_immutable_v2()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  raise exception using errcode = '55000',
    message = 'FLOW_IDENTITY_CAPTURE_RECEIPT_IMMUTABLE';
end;
$$;

alter function private.dataset_flow_identity_receipt_immutable_v2()
  owner to postgres;
revoke all on function private.dataset_flow_identity_receipt_immutable_v2()
  from public, anon, authenticated, service_role;

create trigger dataset_flow_identity_capture_receipts_immutable
before update or delete on util.dataset_flow_identity_capture_receipts
for each row execute function private.dataset_flow_identity_receipt_immutable_v2();
create trigger dataset_flow_identity_capture_sources_immutable
before update or delete on util.dataset_flow_identity_capture_source_guards
for each row execute function private.dataset_flow_identity_receipt_immutable_v2();
create trigger dataset_flow_identity_capture_targets_immutable
before update or delete on util.dataset_flow_identity_capture_target_guards
for each row execute function private.dataset_flow_identity_receipt_immutable_v2();
create trigger dataset_flow_identity_capture_support_immutable
before update or delete on util.dataset_flow_identity_capture_support_guards
for each row execute function private.dataset_flow_identity_receipt_immutable_v2();
create trigger dataset_flow_identity_capture_mappings_immutable
before update or delete on util.dataset_flow_identity_capture_mapping_guards
for each row execute function private.dataset_flow_identity_receipt_immutable_v2();
create trigger dataset_flow_identity_capture_processes_immutable
before update or delete on util.dataset_flow_identity_capture_process_intents
for each row execute function private.dataset_flow_identity_receipt_immutable_v2();

create or replace function private.dataset_flow_identity_active_fence_v2()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_old_id uuid;
  v_new_id uuid;
  v_old_version text;
  v_new_version text;
  v_old_user_id uuid;
  v_new_user_id uuid;
  v_old_state_code integer;
  v_new_state_code integer;
  v_actor uuid;
  v_permit_scope uuid;
  v_old_payload jsonb;
  v_new_payload jsonb;
  v_is_exact_process boolean := false;
  v_is_guarded boolean := false;
begin
  if tg_op <> 'INSERT' then
    v_old_id := old.id;
    v_old_version := btrim(old.version::text);
    v_old_user_id := old.user_id;
    v_old_state_code := old.state_code;
    if tg_table_name in ('processes', 'flows') then
      v_old_payload := old.json_ordered::jsonb;
    end if;
  end if;
  if tg_op <> 'DELETE' then
    v_new_id := new.id;
    v_new_version := btrim(new.version::text);
    v_new_user_id := new.user_id;
    v_new_state_code := new.state_code;
    if tg_table_name in ('processes', 'flows') then
      v_new_payload := new.json_ordered::jsonb;
    end if;
  end if;

  if tg_table_name = 'processes' then
    -- Derivative workers may continue while a scope is active, but only when
    -- every primary identity/payload/policy column is byte-for-byte stable.
    if tg_op = 'UPDATE'
      and new.id is not distinct from old.id
      and new.version is not distinct from old.version
      and new.user_id is not distinct from old.user_id
      and new.state_code is not distinct from old.state_code
      and new.json::jsonb is not distinct from old.json::jsonb
      and new.json_ordered::jsonb
        is not distinct from old.json_ordered::jsonb
      and new.model_id is not distinct from old.model_id
      and new.rule_verification is not distinct from old.rule_verification then
      return new;
    end if;

    -- Serialize both ownership domains.  This closes UPDATEs that move a row
    -- from a foreign owner into the actor (or away from the actor).
    for v_actor in
      select distinct actor_id
      from (values (v_old_user_id), (v_new_user_id)) as actors(actor_id)
      where actor_id is not null
      order by actor_id
    loop
      if not pg_try_advisory_xact_lock(hashtextextended(
        'dataset-flow-identity-actor:' || v_actor::text, 0
      )) then
        raise exception using errcode = '55P03',
          message = 'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY';
      end if;
    end loop;
    -- The only primary mutation permit is a private, transaction-scoped row
    -- minted by the rewrite core and consumed exactly once here.  Caller-set
    -- custom GUCs have no authority.
    if tg_op = 'UPDATE'
      and new.id is not distinct from old.id
      and new.version is not distinct from old.version
      and new.user_id is not distinct from old.user_id
      and new.state_code is not distinct from old.state_code
      and new.model_id is not distinct from old.model_id
      and new.rule_verification is not distinct from old.rule_verification
      and new.json::jsonb is not distinct from old.json::jsonb then
      with consumed as (
        delete from util.dataset_flow_identity_mutation_permits as permit
        using util.dataset_flow_identity_process_ledger as ledger,
          util.dataset_flow_identity_scopes as scope
        where permit.transaction_id = txid_current()
          and permit.process_id = old.id
          and permit.process_version = btrim(old.version::text)
          and permit.before_payload_sha256
            = util.dataset_flow_identity_sha256(old.json_ordered::jsonb)
          and permit.after_payload_sha256
            = util.dataset_flow_identity_sha256(new.json_ordered::jsonb)
          and ledger.scope_id = permit.scope_id
          and ledger.ordinal = permit.ordinal
          and ledger.process_id = permit.process_id
          and ledger.process_version = permit.process_version
          and ledger.mutation_nonce = permit.mutation_nonce
          and ledger.status = 'pending' and ledger.active
          and scope.id = ledger.scope_id
          and scope.actor_user_id = old.user_id
          and scope.status in (
            'sealed', 'running', 'primary_complete', 'derivatives_pending'
          )
        returning permit.scope_id
      )
      select consumed.scope_id into v_permit_scope from consumed;
      if v_permit_scope is not null then
        return new;
      end if;
    end if;

    -- An exact captured process stays fenced after its source references have
    -- been removed.  Source-reference inspection alone is not sufficient.
    select exists (
      select 1
      from util.dataset_flow_identity_scopes as scope
      join util.dataset_flow_identity_process_ledger as ledger
        on ledger.scope_id = scope.id and ledger.active
      where scope.status in (
          'sealed', 'running', 'primary_complete', 'derivatives_pending'
        )
        and (
          (scope.actor_user_id = v_old_user_id
            and ledger.process_id = v_old_id
            and ledger.process_version = v_old_version)
          or (scope.actor_user_id = v_new_user_id
            and ledger.process_id = v_new_id
            and ledger.process_version = v_new_version)
        )
    ) into v_is_exact_process;
    if v_is_exact_process then
      raise exception using errcode = '55000',
        message = 'FLOW_IDENTITY_ACTIVE_SCOPE_PROCESS_FENCE';
    end if;

    select exists (
      select 1
      from util.dataset_flow_identity_scopes as scope
      join util.dataset_flow_identity_capture_source_guards as guard
        on guard.receipt_id = scope.receipt_id
      cross join lateral jsonb_array_elements(
        case when scope.actor_user_id = v_old_user_id
          then coalesce(private.dataset_flow_identity_exchanges(v_old_payload), '[]'::jsonb)
          else '[]'::jsonb end
        || case when scope.actor_user_id = v_new_user_id
          then coalesce(private.dataset_flow_identity_exchanges(v_new_payload), '[]'::jsonb)
          else '[]'::jsonb end
      ) as exchange(value)
      where scope.status in (
          'sealed', 'running', 'primary_complete', 'derivatives_pending'
        )
        and exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
          = guard.source_id::text
        and exchange.value #>> '{referenceToFlowDataSet,@version}'
          = guard.source_version
    ) into v_is_guarded;
    if v_is_guarded then
      raise exception using errcode = '55000',
        message = 'FLOW_IDENTITY_ACTIVE_SCOPE_PROCESS_FENCE';
    end if;
    return case when tg_op = 'DELETE' then old else new end;
  end if;

  if tg_table_name = 'flows' then
    -- Acquire every OLD/NEW and guarded-scope actor in one globally sorted
    -- pass.  Splitting those sets into two loops permits A->Z and Z->A lock
    -- inversion for cross-owner public-target updates.
    for v_actor in
      select distinct actor_id
      from (
        values (v_old_user_id), (v_new_user_id)
        union all
        select scope.actor_user_id
        from util.dataset_flow_identity_scopes as scope
        where scope.status in (
            'sealed', 'running', 'primary_complete', 'derivatives_pending'
          )
          and (
            scope.actor_user_id in (v_old_user_id, v_new_user_id)
            or exists (
              select 1
              from util.dataset_flow_identity_capture_source_guards as guard
              where guard.receipt_id = scope.receipt_id
                and ((guard.source_id = v_old_id
                    and guard.source_version = v_old_version)
                  or (guard.source_id = v_new_id
                    and guard.source_version = v_new_version))
            )
            or exists (
              select 1
              from util.dataset_flow_identity_capture_target_guards as guard
              where guard.receipt_id = scope.receipt_id
                and ((guard.target_id = v_old_id
                    and guard.target_version = v_old_version)
                  or (guard.target_id = v_new_id
                    and guard.target_version = v_new_version))
            )
          )
      ) as actors(actor_id)
      where actor_id is not null
      order by actor_id
    loop
      if not pg_try_advisory_xact_lock(hashtextextended(
        'dataset-flow-identity-actor:' || v_actor::text, 0
      )) then
        raise exception using errcode = '55P03',
          message = 'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY';
      end if;
    end loop;
    select exists (
      select 1
      from util.dataset_flow_identity_scopes as scope
      where scope.status in (
          'sealed', 'running', 'primary_complete', 'derivatives_pending'
        )
        and (
          (scope.actor_user_id = v_old_user_id
            and v_old_state_code = 0
            and v_old_payload #>>
              '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
              = 'Elementary flow')
          or (scope.actor_user_id = v_new_user_id
            and v_new_state_code = 0
            and v_new_payload #>>
              '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
              = 'Elementary flow')
          or exists (
            select 1
            from util.dataset_flow_identity_capture_source_guards as guard
            where guard.receipt_id = scope.receipt_id
              and ((guard.source_id = v_old_id
                  and guard.source_version = v_old_version)
                or (guard.source_id = v_new_id
                  and guard.source_version = v_new_version))
          )
          or exists (
            select 1
            from util.dataset_flow_identity_capture_target_guards as guard
            where guard.receipt_id = scope.receipt_id
              and ((guard.target_id = v_old_id
                  and guard.target_version = v_old_version)
                or (guard.target_id = v_new_id
                  and guard.target_version = v_new_version))
          )
        )
    ) into v_is_guarded;
    if v_is_guarded then
      raise exception using errcode = '55000',
        message = 'FLOW_IDENTITY_ACTIVE_SCOPE_FLOW_FENCE';
    end if;
    return case when tg_op = 'DELETE' then old else new end;
  end if;

  if tg_table_name in ('flowproperties', 'unitgroups') then
    for v_actor in
      select distinct scope.actor_user_id
      from util.dataset_flow_identity_scopes as scope
      join util.dataset_flow_identity_capture_support_guards as guard
        on guard.receipt_id = scope.receipt_id
        and guard.support_table = tg_table_name
        and ((guard.support_id = v_old_id
            and guard.support_version = v_old_version)
          or (guard.support_id = v_new_id
            and guard.support_version = v_new_version))
      where scope.status in (
        'sealed', 'running', 'primary_complete', 'derivatives_pending'
      )
      order by scope.actor_user_id
    loop
      if not pg_try_advisory_xact_lock(hashtextextended(
        'dataset-flow-identity-actor:' || v_actor::text, 0
      )) then
        raise exception using errcode = '55P03',
          message = 'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY';
      end if;
    end loop;
    select exists (
      select 1
      from util.dataset_flow_identity_scopes as scope
      join util.dataset_flow_identity_capture_support_guards as guard
        on guard.receipt_id = scope.receipt_id
        and guard.support_table = tg_table_name
        and ((guard.support_id = v_old_id
            and guard.support_version = v_old_version)
          or (guard.support_id = v_new_id
            and guard.support_version = v_new_version))
      where scope.status in (
        'sealed', 'running', 'primary_complete', 'derivatives_pending'
      )
    ) into v_is_guarded;
    if v_is_guarded then
      raise exception using errcode = '55000',
        message = 'FLOW_IDENTITY_ACTIVE_SCOPE_SUPPORT_FENCE';
    end if;
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

alter function private.dataset_flow_identity_active_fence_v2()
  owner to postgres;
revoke all on function private.dataset_flow_identity_active_fence_v2()
  from public, anon, authenticated, service_role;

create trigger dataset_flow_identity_process_active_fence
before insert or update or delete on public.processes
for each row execute function private.dataset_flow_identity_active_fence_v2();
create trigger dataset_flow_identity_flow_active_fence
before insert or update or delete on public.flows
for each row execute function private.dataset_flow_identity_active_fence_v2();
create trigger dataset_flow_identity_flowproperty_active_fence
before update or delete on public.flowproperties
for each row execute function private.dataset_flow_identity_active_fence_v2();
create trigger dataset_flow_identity_unitgroup_active_fence
before update or delete on public.unitgroups
for each row execute function private.dataset_flow_identity_active_fence_v2();

create or replace function private.dataset_flow_identity_whole_scope_proof_v2(
  p_actor uuid,
  p_receipt_id uuid,
  p_scope_id uuid default null,
  p_lock_rows boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_supports jsonb;
  v_source_guards_current boolean := false;
  v_target_guards_current boolean := false;
  v_support_guards_current boolean := false;
  v_mapping_guards_current boolean := false;
  v_source_universe_current boolean := false;
  v_protected_proof jsonb;
  v_protected_current boolean := false;
  v_occurrence_current boolean := false;
  v_primary_current boolean := false;
  v_scope_audit_current boolean := false;
  v_audit_current boolean := false;
  v_terminal_audit_current boolean := true;
  v_derivative_proof jsonb;
  v_raw_derivatives_current boolean := false;
  v_derivatives_current boolean := false;
  v_expected_residue bigint := 0;
  v_observed_residue bigint := 0;
  v_primary_entries jsonb := '[]'::jsonb;
  v_primary_closure_sha256 text;
  v_derivative_targets jsonb;
  v_derivative_target_set_sha256 text;
  v_expected_terminal_proof_sha256 text;
  v_expected_terminal_audit_payload jsonb;
  v_terminal_audit_payload jsonb;
  v_terminal_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_terminal_audit_count integer := 0;
  v_proof jsonb;
  v_proof_sha256 text;
  v_completed integer := 0;
  v_process_count integer := 0;
begin
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = p_receipt_id and receipt.actor_user_id = p_actor;
  if v_receipt.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_RECEIPT_NOT_FOUND'
    );
  end if;
  if p_scope_id is not null then
    select scope.* into v_scope
    from util.dataset_flow_identity_scopes as scope
    where scope.id = p_scope_id and scope.actor_user_id = p_actor
      and scope.receipt_id = p_receipt_id;
    if v_scope.id is null then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND'
      );
    end if;
  end if;

  if p_lock_rows then
    perform 1
    from public.flows as flow
    join (
      select guard.source_id as id, guard.source_version as version
      from util.dataset_flow_identity_capture_source_guards as guard
      where guard.receipt_id = p_receipt_id
      union
      select guard.target_id, guard.target_version
      from util.dataset_flow_identity_capture_target_guards as guard
      where guard.receipt_id = p_receipt_id
    ) as wanted
      on wanted.id = flow.id and wanted.version = btrim(flow.version::text)
    order by flow.id, btrim(flow.version::text)
    for share of flow;
    perform 1
    from public.flowproperties as support
    join util.dataset_flow_identity_capture_support_guards as guard
      on guard.receipt_id = p_receipt_id
      and guard.support_table = 'flowproperties'
      and guard.support_id = support.id
      and guard.support_version = btrim(support.version::text)
    order by support.id, btrim(support.version::text)
    for share of support;
    perform 1
    from public.unitgroups as support
    join util.dataset_flow_identity_capture_support_guards as guard
      on guard.receipt_id = p_receipt_id
      and guard.support_table = 'unitgroups'
      and guard.support_id = support.id
      and guard.support_version = btrim(support.version::text)
    order by support.id, btrim(support.version::text)
    for share of support;
    perform 1
    from public.processes as process
    join (
      select intent.process_id as id, intent.process_version as version
      from util.dataset_flow_identity_capture_process_intents as intent
      where intent.receipt_id = p_receipt_id
      union
      select (occurrence.value->>'process_id')::uuid,
        occurrence.value->>'process_version'
      from jsonb_array_elements(
        coalesce(v_receipt.protected_closure->'pending', '[]'::jsonb)
        || coalesce(v_receipt.protected_closure->'blockers', '[]'::jsonb)
      ) as entry(value)
      cross join lateral jsonb_array_elements(
        entry.value->'occurrences'
      ) as occurrence(value)
    ) as wanted
      on wanted.id = process.id
      and wanted.version = btrim(process.version::text)
    order by process.id, btrim(process.version::text)
    for share of process;
  end if;

  select coalesce(jsonb_agg(guard.guard order by guard.ordinal), '[]'::jsonb)
  into v_supports
  from util.dataset_flow_identity_capture_support_guards as guard
  where guard.receipt_id = p_receipt_id;
  v_support_guards_current := coalesce((
    util.dataset_flow_identity_validate_support_set(
      p_actor, v_supports, v_receipt.support_snapshot_set_sha256
    )->>'ok'
  )::boolean, false);
  select count(*) = 305 and bool_and(coalesce((
    util.dataset_flow_identity_validate_flow_guard(
      p_actor, guard.guard, false, v_supports
    )->>'ok'
  )::boolean, false))
  into v_source_guards_current
  from util.dataset_flow_identity_capture_source_guards as guard
  where guard.receipt_id = p_receipt_id;
  select count(*) = v_receipt.target_count and bool_and(coalesce((
    util.dataset_flow_identity_validate_flow_guard(
      p_actor, guard.guard, true, v_supports
    )->>'ok'
  )::boolean, false))
  into v_target_guards_current
  from util.dataset_flow_identity_capture_target_guards as guard
  where guard.receipt_id = p_receipt_id;
  select count(*) = v_receipt.mapping_count and bool_and(coalesce((
    util.dataset_flow_identity_validate_mapping(
      p_actor, guard.mapping, v_receipt.compatibility_policy,
      v_supports, guard.ordinal
    )->>'ok'
  )::boolean, false))
  into v_mapping_guards_current
  from util.dataset_flow_identity_capture_mapping_guards as guard
  where guard.receipt_id = p_receipt_id;
  v_source_universe_current := coalesce((
    util.dataset_flow_identity_source_universe(
      p_actor, v_receipt.source_universe, v_receipt.source_universe_sha256
    )->>'ok'
  )::boolean, false);
  v_protected_proof := util.dataset_flow_identity_protected_closure(
    p_actor, v_receipt.protected_closure
  );
  v_protected_current := coalesce(
    (v_protected_proof->>'ok')::boolean, false
  );

  if p_scope_id is null then
    select
      count(*)::integer,
      count(*) filter (
        where process.id is not null
          and util.dataset_flow_identity_sha256(process.json_ordered::jsonb)
            = intent.manifest->>'before_payload_sha256'
          and util.dataset_flow_identity_sha256(
            private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
          ) = intent.manifest->>'before_exchange_set_sha256'
      )::integer
    into v_process_count, v_completed
    from util.dataset_flow_identity_capture_process_intents as intent
    left join public.processes as process
      on process.id = intent.process_id
      and btrim(process.version::text) = intent.process_version
      and process.user_id = p_actor and process.state_code = 0
      and process.json::jsonb = process.json_ordered::jsonb
    where intent.receipt_id = p_receipt_id;
    v_primary_current := v_process_count = v_receipt.process_count
      and v_completed = v_receipt.process_count;
    v_audit_current := true;
    v_expected_residue := v_receipt.rewrite_count;
  else
    select count(*) = 1 and bool_and(
      audit.payload = jsonb_build_object(
        'record_type', 'scope_seal',
        'schema_version', 'dataset-flow-identity-scope-preflight.v2',
        'proof_domain', 'dataset-flow-identity-db-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'operation_id', v_scope.operation_id,
        'plan_sha256', v_scope.plan_sha256,
        'freeze_sha256', v_scope.freeze_sha256,
        'capture_request_sha256', v_receipt.capture_request_sha256,
        'capture_whole_scope_proof_sha256',
          v_receipt.whole_scope_proof_sha256,
        'source_guard_set_sha256', v_receipt.source_guard_set_sha256,
        'support_guard_set_sha256', v_receipt.support_snapshot_set_sha256,
        'target_guard_set_sha256', v_receipt.target_guard_set_sha256,
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'policy_sha256', v_receipt.compatibility_policy->>'policy_sha256',
        'policy_approval_text_sha256',
          v_scope.policy_approval_text_sha256,
        'execution_approval_request_sha256',
          v_scope.execution_approval_request_sha256,
        'execution_approval_text_sha256', v_scope.approval_text_sha256,
        'execution_approval_identity_sha256',
          v_scope.approval_identity_sha256,
        'toolchain_evidence_sha256', v_scope.toolchain_evidence_sha256,
        'user_state_claim', v_scope.user_state_claim,
        'maximum_wrapper_invocations', 1,
        'maximum_process_posts', v_scope.process_count,
        'maximum_finalize_posts', 1,
        'maximum_cli_apply_spawns', 1,
        'approval_reusable', false,
        'automatic_retry', false,
        'preflight_request_sha256', v_scope.preflight_request_sha256,
        'scope_request_sha256', v_scope.scope_request_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'source_universe_count', 305,
        'mapping_count', v_scope.mapping_count,
        'process_count', v_scope.process_count,
        'rewrite_count', v_scope.rewrite_count,
        'hash_algorithm', 'restricted-safe-json-v2-sha256'
      )
    )
    into v_scope_audit_current
    from public.command_audit_log as audit
    where audit.command = 'cmd_dataset_flow_identity_scope_preflight_guarded'
      and audit.actor_user_id = p_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = p_scope_id::text;

    with live as (
      select ledger.*, process.id as live_id,
        util.dataset_flow_identity_sha256(process.json_ordered::jsonb)
          as live_payload_sha256,
        util.dataset_flow_identity_sha256(
          private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
        ) as live_exchange_sha256,
        audit.id as live_audit_id,
        audit.payload as live_audit_payload,
        invocation.id as live_invocation_id,
        invocation.generation as live_invocation_generation,
        invocation.approval_kind as live_approval_kind,
        invocation.approval_identity_sha256
          as live_approval_identity_sha256,
        invocation.admission_request_sha256
          as live_admission_request_sha256
      from util.dataset_flow_identity_process_ledger as ledger
      left join public.processes as process
        on process.id = ledger.process_id
        and btrim(process.version::text) = ledger.process_version
        and process.user_id = p_actor and process.state_code = 0
        and process.json is not null and process.json_ordered is not null
        and process.json::jsonb = process.json_ordered::jsonb
      left join public.command_audit_log as audit
        on audit.id = ledger.audit_id
        and audit.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
        and audit.actor_user_id = p_actor
        and audit.target_table = 'processes'
        and audit.target_id = ledger.process_id
        and audit.target_version = ledger.process_version
        and audit.payload->>'scope_id' = p_scope_id::text
        and audit.payload->>'process_request_sha256'
          = ledger.process_request_sha256
      left join util.dataset_flow_identity_wrapper_invocations as invocation
        on invocation.id = ledger.wrapper_invocation_id
        and invocation.scope_id = ledger.scope_id
        and invocation.actor_user_id = p_actor
      where ledger.scope_id = p_scope_id
    )
    select
      count(*)::integer,
      count(*) filter (where status = 'completed')::integer,
      bool_and(
        live_id is not null and case when status = 'completed'
          then live_payload_sha256 = manifest->>'desired_payload_sha256'
            and live_exchange_sha256 = manifest->>'desired_exchange_set_sha256'
          else live_payload_sha256 = manifest->>'before_payload_sha256'
            and live_exchange_sha256 = manifest->>'before_exchange_set_sha256'
        end
      ),
      bool_and(case when status = 'completed' then
        live_audit_id is not null
        and live_invocation_id is not null
        and permit_generation_before is not null
        and live_invocation_generation > permit_generation_before
        and live_audit_payload = jsonb_build_object(
          'record_type', 'process_rewrite',
          'schema_version', 'dataset-flow-identity-process-rewrite.v2',
          'proof_domain', 'dataset-flow-identity-db-proof.v2',
          'scope_id', v_scope.id,
          'receipt_id', v_receipt.id,
          'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
          'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
          'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
          'scope_proof_sha256', v_scope.scope_proof_sha256,
          'operation_id', v_scope.operation_id,
          'plan_sha256', v_scope.plan_sha256,
          'wrapper_invocation_id', live_invocation_id,
          'wrapper_approval_kind', live_approval_kind,
          'wrapper_approval_identity_sha256',
            live_approval_identity_sha256,
          'wrapper_admission_request_sha256',
            live_admission_request_sha256,
          'permit_generation_before', permit_generation_before,
          'ordinal', ordinal,
          'process_intent_proof_sha256', process_intent_proof_sha256,
          'process_request_sha256', process_request_sha256,
          'process_template_sha256', process_template_sha256,
          'rewrite_set_sha256', manifest->>'rewrite_set_sha256',
          'collision_ledger_sha256', manifest->>'collision_ledger_sha256',
          'before_payload_sha256', before_payload_sha256,
          'before_exchange_set_sha256',
            manifest->>'before_exchange_set_sha256',
          'desired_payload_sha256', manifest->>'desired_payload_sha256',
          'desired_exchange_set_sha256',
            manifest->>'desired_exchange_set_sha256',
          'after_payload_sha256', after_payload_sha256,
          'after_exchange_set_sha256', after_exchange_set_sha256,
          'rewrite_count', rewrite_count,
          'derivative_batch_id', derivative_batch_id,
          'derivative_reason_code', 'FLOW_IDENTITY_SCOPE:'
            || v_scope.id::text || ':' || ordinal::text,
          'hash_algorithm', 'restricted-safe-json-v2-sha256'
        )
        else live_audit_id is null end),
      coalesce(sum(rewrite_count) filter (where status = 'pending'), 0)::bigint,
      coalesce(jsonb_agg(jsonb_build_object(
        'ordinal', ordinal,
        'id', process_id,
        'version', process_version,
        'json_ordered_sha256', after_payload_sha256,
        'exchange_set_sha256', after_exchange_set_sha256,
        'audit_id', case when audit_id is null then null else audit_id::text end,
        'wrapper_invocation_id', wrapper_invocation_id,
        'permit_generation_before', permit_generation_before
      ) order by ordinal) filter (where status = 'completed'), '[]'::jsonb)
    into v_process_count, v_completed, v_primary_current,
      v_audit_current, v_expected_residue, v_primary_entries
    from live;
    v_audit_current := coalesce(v_scope_audit_current, false)
      and coalesce(v_audit_current, false);
  end if;
  select count(*)::bigint into v_observed_residue
  from public.processes as process
  cross join lateral jsonb_array_elements(
    private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
  ) as exchange(value)
  join util.dataset_flow_identity_capture_mapping_guards as mapping
    on mapping.receipt_id = p_receipt_id
    and exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
      = mapping.source_id::text
    and exchange.value #>> '{referenceToFlowDataSet,@version}'
      = mapping.source_version
  where process.user_id = p_actor and process.state_code = 0;
  v_occurrence_current := v_observed_residue = v_expected_residue
    and v_protected_current;
  v_primary_closure_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_primary_entries);
  if p_scope_id is null then
    v_derivative_proof := jsonb_build_object(
      'proof_sha256', repeat('0', 64), 'causal_terminal_proof', false
    );
  else
    v_derivative_proof :=
      util.read_dataset_flow_identity_derivative_set(p_actor, p_scope_id);
  end if;
  v_raw_derivatives_current := p_scope_id is not null
    and v_completed = v_process_count
    and coalesce((v_derivative_proof->>'causal_terminal_proof')::boolean, false);
  -- A derivative can be terminal while a primary row or immutable guard has
  -- drifted.  Never expose that raw child terminality as scope currency.
  v_derivatives_current := v_raw_derivatives_current
    and coalesce(v_primary_current, false)
    and coalesce(v_audit_current, false)
    and coalesce(v_source_guards_current, false)
    and coalesce(v_source_universe_current, false)
    and coalesce(v_mapping_guards_current, false)
    and coalesce(v_support_guards_current, false)
    and coalesce(v_target_guards_current, false)
    and v_protected_current and v_occurrence_current;

  v_proof := jsonb_build_object(
    'schema_version', 'dataset-flow-identity-whole-scope-proof.v2',
    'scope_id', p_scope_id,
    'receipt_id', p_receipt_id,
    'primary_current', coalesce(v_primary_current, false),
    'audit_current', coalesce(v_audit_current, false),
    'source_guards_current', coalesce(v_source_guards_current, false)
      and coalesce(v_source_universe_current, false)
      and coalesce(v_mapping_guards_current, false),
    'support_guards_current', coalesce(v_support_guards_current, false),
    'target_guards_current', coalesce(v_target_guards_current, false),
    'approved_reference_residue_count', v_observed_residue,
    'protected_closure_current', v_protected_current,
    'occurrence_closure_current', v_occurrence_current,
    'derivatives_current', v_derivatives_current,
    'primary_closure_sha256', v_primary_closure_sha256,
    'source_guard_set_sha256', v_receipt.source_guard_set_sha256,
    'support_guard_set_sha256', v_receipt.support_snapshot_set_sha256,
    'target_guard_set_sha256', v_receipt.target_guard_set_sha256,
    'protected_closure_sha256', v_receipt.protected_closure_sha256,
    'derivative_proof_set_sha256',
      coalesce(v_derivative_proof->>'proof_sha256', repeat('0', 64)),
    'causal_terminal_proof', false,
    'proof_sha256', ''
  );
  v_proof := jsonb_set(
    v_proof, '{causal_terminal_proof}',
    to_jsonb(
      coalesce(v_primary_current, false)
      and coalesce(v_audit_current, false)
      and coalesce(v_source_guards_current, false)
      and coalesce(v_source_universe_current, false)
      and coalesce(v_mapping_guards_current, false)
      and coalesce(v_support_guards_current, false)
      and coalesce(v_target_guards_current, false)
      and v_protected_current and v_occurrence_current
      and v_derivatives_current and v_observed_residue = 0
    ), false
  );
  v_proof_sha256 := util.dataset_flow_identity_restricted_sha256_v2(v_proof);
  if p_scope_id is not null and v_scope.status = 'completed' then
    select invocation.* into v_terminal_invocation
    from util.dataset_flow_identity_wrapper_invocations as invocation
    where invocation.id = v_scope.final_wrapper_invocation_id
      and invocation.scope_id = v_scope.id
      and invocation.actor_user_id = p_actor;
    select coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ledger.ordinal,
      'id', ledger.process_id,
      'version', ledger.process_version,
      'desired_json_ordered_sha256',
        ledger.manifest->>'desired_payload_sha256',
      'baseline_snapshot_sha256',
        ledger.manifest->>'derivative_baseline_snapshot_sha256'
    ) order by ledger.ordinal), '[]'::jsonb)
    into v_derivative_targets
    from util.dataset_flow_identity_process_ledger as ledger
    where ledger.scope_id = p_scope_id;
    v_derivative_target_set_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(v_derivative_targets);
    select count(*)::integer, jsonb_agg(audit.payload order by audit.id)->0
    into v_terminal_audit_count, v_terminal_audit_payload
    from public.command_audit_log as audit
    where audit.command = 'cmd_dataset_flow_identity_scope_finalize_guarded'
      and audit.actor_user_id = p_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = p_scope_id::text;
    v_expected_terminal_proof_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
        'schema_version', 'dataset-flow-identity-terminal-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'plan_sha256', v_scope.plan_sha256,
        'final_request_sha256', v_scope.final_request_sha256,
        'wrapper_invocation_id', v_terminal_invocation.id,
        'wrapper_approval_kind', v_terminal_invocation.approval_kind,
        'wrapper_approval_identity_sha256',
          v_terminal_invocation.approval_identity_sha256,
        'wrapper_admission_request_sha256',
          v_terminal_invocation.admission_request_sha256,
        'permit_generation_before',
          v_scope.final_permit_generation_before,
        'whole_scope_proof_sha256',
          v_terminal_audit_payload->>'whole_scope_proof_sha256',
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'derivative_target_set_sha256', v_derivative_target_set_sha256,
        'derivative_proof_set_sha256',
          v_terminal_audit_payload->>'derivative_proof_set_sha256'
      ));
    v_expected_terminal_audit_payload := jsonb_build_object(
      'record_type', 'scope_terminal',
      'schema_version', 'dataset-flow-identity-scope-finalize.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'scope_id', v_scope.id,
      'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'final_request_sha256', v_scope.final_request_sha256,
      'wrapper_invocation_id', v_terminal_invocation.id,
      'wrapper_approval_kind', v_terminal_invocation.approval_kind,
      'wrapper_approval_identity_sha256',
        v_terminal_invocation.approval_identity_sha256,
      'wrapper_admission_request_sha256',
        v_terminal_invocation.admission_request_sha256,
      'permit_generation_before', v_scope.final_permit_generation_before,
      'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
      'protected_closure_sha256', v_receipt.protected_closure_sha256,
      'primary_closure_sha256', v_primary_closure_sha256,
      'derivative_target_set_sha256', v_derivative_target_set_sha256,
      'derivative_proof_set_sha256',
        v_terminal_audit_payload->>'derivative_proof_set_sha256',
      'whole_scope_proof_sha256',
        v_terminal_audit_payload->>'whole_scope_proof_sha256',
      'terminal_proof_sha256', v_expected_terminal_proof_sha256,
      'process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
    -- Completion-time derivative/whole-proof hashes are historical facts.
    -- Bind them through the terminal proof, but do not compare them with the
    -- current derivative proof: a later stale child must become compensation,
    -- not masquerade as primary/audit drift.
    v_terminal_audit_current := v_terminal_audit_count = 1
      and v_terminal_invocation.id is not null
      and v_scope.final_permit_generation_before is not null
      and v_terminal_invocation.status = 'completed'
      and v_terminal_invocation.successful_finalize_posts = 1
      and v_terminal_invocation.generation =
        v_scope.final_permit_generation_before + 1
      and v_terminal_audit_payload = v_expected_terminal_audit_payload
      and v_terminal_audit_payload->>'whole_scope_proof_sha256'
        ~ '^[a-f0-9]{64}$'
      and v_terminal_audit_payload->>'derivative_proof_set_sha256'
        ~ '^[a-f0-9]{64}$'
      and v_scope.terminal_proof_sha256
        is not distinct from v_expected_terminal_proof_sha256;
    if not v_terminal_audit_current then
      v_audit_current := false;
      v_derivatives_current := false;
      v_proof := jsonb_set(v_proof, '{audit_current}', 'false'::jsonb, false);
      v_proof := jsonb_set(
        v_proof, '{derivatives_current}', 'false'::jsonb, false
      );
      v_proof := jsonb_set(
        v_proof, '{causal_terminal_proof}', 'false'::jsonb, false
      );
      v_proof_sha256 :=
        util.dataset_flow_identity_restricted_sha256_v2(v_proof);
    end if;
  end if;
  v_proof := jsonb_set(
    v_proof, '{proof_sha256}', to_jsonb(v_proof_sha256), false
  );
  return jsonb_build_object(
    'ok', coalesce(v_primary_current, false)
      and coalesce(v_audit_current, false)
      and coalesce(v_source_guards_current, false)
      and coalesce(v_source_universe_current, false)
      and coalesce(v_mapping_guards_current, false)
      and coalesce(v_support_guards_current, false)
      and coalesce(v_target_guards_current, false)
      and v_protected_current and v_occurrence_current,
    'whole_scope_proof', v_proof,
    'whole_scope_proof_sha256', v_proof_sha256,
    'derivative_set_proof', v_derivative_proof
  );
end;
$$;

alter function private.dataset_flow_identity_whole_scope_proof_v2(
  uuid, uuid, uuid, boolean
) owner to postgres;
revoke all on function private.dataset_flow_identity_whole_scope_proof_v2(
  uuid, uuid, uuid, boolean
) from public, anon, authenticated, service_role;

-- Keep the already reviewed v1 materializers as private implementation cores.
-- The authenticated surface below is v2-only and never accepts caller-owned
-- live snapshots, desired payloads, mappings, or closure proofs.
alter function public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)
  rename to dataset_flow_identity_scope_preflight_core_v1;
alter function public.dataset_flow_identity_scope_preflight_core_v1(jsonb)
  set schema private;
revoke all on function private.dataset_flow_identity_scope_preflight_core_v1(
  jsonb
) from public, anon, authenticated, service_role;

alter function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb
) rename to dataset_flow_identity_process_rewrite_core_v1;
alter function public.dataset_flow_identity_process_rewrite_core_v1(uuid, jsonb)
  set schema private;
revoke all on function private.dataset_flow_identity_process_rewrite_core_v1(
  uuid, jsonb
) from public, anon, authenticated, service_role;

alter function public.cmd_dataset_flow_identity_scope_read(uuid)
  rename to dataset_flow_identity_scope_read_core_v1;
alter function public.dataset_flow_identity_scope_read_core_v1(uuid)
  set schema private;
revoke all on function private.dataset_flow_identity_scope_read_core_v1(uuid)
  from public, anon, authenticated, service_role;

alter function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb
) rename to dataset_flow_identity_scope_finalize_core_v1;
alter function public.dataset_flow_identity_scope_finalize_core_v1(uuid, jsonb)
  set schema private;
revoke all on function private.dataset_flow_identity_scope_finalize_core_v1(
  uuid, jsonb
) from public, anon, authenticated, service_role;

alter function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) rename to dataset_flow_identity_scope_cancel_core_v1;
alter function public.dataset_flow_identity_scope_cancel_core_v1(uuid, jsonb)
  set schema private;
revoke all on function private.dataset_flow_identity_scope_cancel_core_v1(
  uuid, jsonb
) from public, anon, authenticated, service_role;

create or replace function public.cmd_dataset_flow_identity_scope_preflight_guarded(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '180s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_preflight_guarded';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_request_sha256 text;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_internal jsonb;
  v_core_result jsonb;
  v_live jsonb;
  v_mappings jsonb;
  v_supports jsonb;
  v_processes jsonb;
  v_scope_proof_sha256 text;
  v_audit_id bigint;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
  v_invocation_id uuid;
  v_permit_token text;
  v_execution_permit jsonb := null;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'receipt_id', 'receipt_proof_sha256',
      'environment', 'project_ref', 'actor', 'target_visibility',
      'user_state_claim',
      'operation_id', 'plan_sha256', 'freeze_sha256',
      'policy_approval_text_sha256', 'execution_approval_request_sha256',
      'execution_approval_text_sha256',
      'execution_approval_identity_sha256', 'toolchain_evidence_sha256',
      'maximum_wrapper_invocations', 'maximum_process_posts',
      'maximum_finalize_posts', 'maximum_cli_apply_spawns',
      'approval_reusable', 'automatic_retry'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-preflight.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'receipt_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or v_request->>'user_state_claim'
      <> 'authenticated_actor_state_100_plus_own_state_0'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}'))
      is distinct from v_actor_email
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or jsonb_typeof(v_request->'maximum_wrapper_invocations') <> 'number'
    or (v_request->>'maximum_wrapper_invocations')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_process_posts') <> 'number'
    or (v_request->>'maximum_process_posts')::numeric <= 0
    or (v_request->>'maximum_process_posts')::numeric > 2147483647
    or jsonb_typeof(v_request->'maximum_finalize_posts') <> 'number'
    or (v_request->>'maximum_finalize_posts')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_cli_apply_spawns') <> 'number'
    or (v_request->>'maximum_cli_apply_spawns')::numeric <> 1
    or v_request->'approval_reusable' is distinct from 'false'::jsonb
    or v_request->'automatic_retry' is distinct from 'false'::jsonb
    or exists (
      select 1 from unnest(array[
        'receipt_proof_sha256', 'plan_sha256', 'freeze_sha256',
        'policy_approval_text_sha256', 'execution_approval_request_sha256',
        'execution_approval_text_sha256',
        'execution_approval_identity_sha256', 'toolchain_evidence_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    )
    or (
      select count(distinct value)
      from unnest(array[
        v_request->>'policy_approval_text_sha256',
        v_request->>'execution_approval_request_sha256',
        v_request->>'execution_approval_text_sha256',
        v_request->>'execution_approval_identity_sha256'
      ]) as approval(value)
    ) <> 4 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 preflight request schema mismatch'
    );
  end if;
  v_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);

  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = (v_request->>'receipt_id')::uuid
    and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_request->>'receipt_proof_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_RECEIPT_INVALID', 'status', 409,
      'message', 'Capture receipt is missing, foreign, or forged'
    );
  end if;
  if v_receipt.environment is distinct from v_request->>'environment'
    or v_receipt.project_ref is distinct from v_request->>'project_ref'
    or v_receipt.target_visibility
      is distinct from v_request->>'target_visibility'
    or v_receipt.operation_id is distinct from v_request->>'operation_id'
    or v_receipt.policy_approval_text_sha256
      is distinct from v_request->>'policy_approval_text_sha256'
    or v_receipt.artifact_evidence->>'toolchain_evidence_sha256'
      is distinct from v_request->>'toolchain_evidence_sha256'
    or (v_request->>'maximum_process_posts')::integer
      is distinct from v_receipt.process_count then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_RECEIPT_BINDING_MISMATCH',
      'status', 409,
      'message', 'Preflight identity does not exactly bind the receipt'
    );
  end if;
  -- The owner fence is held through activation.  A concurrent row UPDATE
  -- blocked by the deterministic FOR SHARE proof resumes into this VOLATILE
  -- BEFORE trigger under READ COMMITTED, where the trigger's SQL sees the
  -- newly committed active scope and rejects the write.
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));

  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.actor_user_id = v_actor
    and scope.operation_id = v_receipt.operation_id;
  if v_scope.id is not null then
    if v_scope.receipt_id is distinct from v_receipt.id
      or v_scope.preflight_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_OPERATION_REUSE_MISMATCH',
        'status', 409, 'message', 'Operation is already sealed differently'
      );
    end if;
    if v_scope.status in ('cancelled', 'failed') then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_SCOPE_TERMINAL_CONFLICT',
        'status', 409, 'scope_status', v_scope.status,
        'message', 'A cancelled or failed scope cannot be replayed as v2'
      );
    end if;
  else
    if exists (
      select 1
      from util.dataset_flow_identity_wrapper_invocations as invocation
      where invocation.actor_user_id = v_actor
        and array[
          v_request->>'execution_approval_request_sha256',
          v_request->>'execution_approval_text_sha256',
          v_request->>'execution_approval_identity_sha256'
        ] && array[
          invocation.approval_request_sha256,
          invocation.approval_text_sha256,
          invocation.approval_identity_sha256
        ]
    ) then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_APPROVAL_REUSE_MISMATCH',
        'status', 409,
        'message', 'Execution approval hash was already consumed'
      );
    end if;
    if v_receipt.expires_at <= clock_timestamp() then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_RECEIPT_EXPIRED', 'status', 409,
        'message', 'Capture receipt expired before first consumption'
      );
    end if;
    v_live := private.dataset_flow_identity_whole_scope_proof_v2(
      v_actor, v_receipt.id, null, true
    );
    if coalesce((v_live->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PREFLIGHT_LIVE_DRIFT', 'status', 409,
        'message', 'Receipt baselines drifted before scope sealing'
      );
    end if;

    select coalesce(jsonb_agg(guard.mapping order by guard.ordinal), '[]'::jsonb)
    into v_mappings
    from util.dataset_flow_identity_capture_mapping_guards as guard
    where guard.receipt_id = v_receipt.id;
    select coalesce(jsonb_agg(guard.guard order by guard.ordinal), '[]'::jsonb)
    into v_supports
    from util.dataset_flow_identity_capture_support_guards as guard
    where guard.receipt_id = v_receipt.id;
    select coalesce(jsonb_agg(intent.manifest order by intent.ordinal), '[]'::jsonb)
    into v_processes
    from util.dataset_flow_identity_capture_process_intents as intent
    where intent.receipt_id = v_receipt.id;

    v_internal := jsonb_build_object(
      'schema_version', 'dataset-flow-identity-scope-preflight.v1',
      'request_id', v_request->>'request_id',
      'environment', v_receipt.environment,
      'project_ref', v_receipt.project_ref,
      'actor', jsonb_build_object(
        'user_id', v_actor, 'email', v_actor_email
      ),
      'target_visibility', 'owner_draft',
      'operation_id', v_receipt.operation_id,
      'plan_sha256', v_request->>'plan_sha256',
      'freeze_sha256', v_request->>'freeze_sha256',
      'approval_identity_sha256',
        v_request->>'execution_approval_identity_sha256',
      'approval_text_sha256',
        v_request->>'execution_approval_text_sha256',
      'toolchain_evidence_sha256',
        v_request->>'toolchain_evidence_sha256',
      'compatibility_policy', v_receipt.compatibility_policy,
      'support_snapshot_set_sha256',
        v_receipt.support_snapshot_set_sha256,
      'support_snapshots', v_supports,
      'source_universe_sha256', v_receipt.source_universe_sha256,
      'source_universe_count', 305,
      'mapping_set_sha256', v_receipt.mapping_set_sha256,
      'process_manifest_sha256', v_receipt.process_manifest_sha256,
      'protected_closure_sha256', v_receipt.protected_closure_sha256,
      'mappings', v_mappings,
      'processes', v_processes,
      'protected_closure', v_receipt.protected_closure
    );
    perform set_config(
      'app.dataset_flow_identity_receipt_id', v_receipt.id::text, true
    );
    perform set_config(
      'app.dataset_flow_identity_receipt_proof_sha256',
      v_receipt.receipt_proof_sha256, true
    );
    perform set_config(
      'app.dataset_flow_identity_policy_approval_sha256',
      v_receipt.policy_approval_text_sha256, true
    );
    perform set_config(
      'app.dataset_flow_identity_execution_request_sha256',
      v_request->>'execution_approval_request_sha256', true
    );
    perform set_config(
      'app.dataset_flow_identity_preflight_request_sha256',
      v_request_sha256, true
    );
    v_core_result :=
      private.dataset_flow_identity_scope_preflight_core_v1(v_internal);
    if coalesce((v_core_result->>'ok')::boolean, false) is false then
      return v_core_result;
    end if;
    select scope.* into v_scope
    from util.dataset_flow_identity_scopes as scope
    where scope.id = (v_core_result->>'scope_id')::uuid
      and scope.actor_user_id = v_actor;
    if v_scope.id is null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PREFLIGHT_SCOPE_MISSING_AFTER_CORE';
    end if;
    v_scope_proof_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
        'schema_version', 'dataset-flow-identity-scope-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'actor_user_id', v_actor,
        'environment', v_scope.environment,
        'project_ref', v_scope.project_ref,
        'operation_id', v_scope.operation_id,
        'plan_sha256', v_scope.plan_sha256,
        'freeze_sha256', v_scope.freeze_sha256,
        'policy_approval_text_sha256',
          v_scope.policy_approval_text_sha256,
        'execution_approval_request_sha256',
          v_scope.execution_approval_request_sha256,
        'execution_approval_text_sha256', v_scope.approval_text_sha256,
        'execution_approval_identity_sha256',
          v_scope.approval_identity_sha256,
        'toolchain_evidence_sha256', v_scope.toolchain_evidence_sha256,
        'preflight_request_sha256', v_scope.preflight_request_sha256,
        'capture_whole_scope_proof_sha256',
          v_receipt.whole_scope_proof_sha256
      ));
    update util.dataset_flow_identity_scopes
    set scope_proof_sha256 = v_scope_proof_sha256,
      updated_at = clock_timestamp()
    where id = v_scope.id;
    v_scope.scope_proof_sha256 := v_scope_proof_sha256;
  end if;

  select audit.id, audit.payload into v_audit_id, v_audit_payload
  from public.command_audit_log as audit
  where audit.command = v_command and audit.actor_user_id = v_actor
    and audit.target_table is null
    and audit.payload->>'scope_id' = v_scope.id::text
  order by audit.id desc limit 1;
  if v_audit_id is null then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PREFLIGHT_V2_AUDIT_MISSING';
  end if;
  v_expected_audit_payload := jsonb_build_object(
      'record_type', 'scope_seal',
      'schema_version', 'dataset-flow-identity-scope-preflight.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'scope_id', v_scope.id,
      'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'freeze_sha256', v_scope.freeze_sha256,
      'capture_request_sha256', v_receipt.capture_request_sha256,
      'capture_whole_scope_proof_sha256',
        v_receipt.whole_scope_proof_sha256,
      'source_guard_set_sha256', v_receipt.source_guard_set_sha256,
      'support_guard_set_sha256', v_receipt.support_snapshot_set_sha256,
      'target_guard_set_sha256', v_receipt.target_guard_set_sha256,
      'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
      'protected_closure_sha256', v_receipt.protected_closure_sha256,
      'policy_sha256', v_receipt.compatibility_policy->>'policy_sha256',
      'policy_approval_text_sha256',
        v_scope.policy_approval_text_sha256,
      'execution_approval_request_sha256',
        v_scope.execution_approval_request_sha256,
      'execution_approval_text_sha256', v_scope.approval_text_sha256,
      'execution_approval_identity_sha256',
        v_scope.approval_identity_sha256,
      'toolchain_evidence_sha256', v_scope.toolchain_evidence_sha256,
      'user_state_claim', v_scope.user_state_claim,
      'maximum_wrapper_invocations', 1,
      'maximum_process_posts', v_scope.process_count,
      'maximum_finalize_posts', 1,
      'maximum_cli_apply_spawns', 1,
      'approval_reusable', false,
      'automatic_retry', false,
      'preflight_request_sha256', v_scope.preflight_request_sha256,
      'scope_request_sha256', v_scope.scope_request_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'source_universe_count', 305,
      'mapping_count', v_scope.mapping_count,
      'process_count', v_scope.process_count,
      'rewrite_count', v_scope.rewrite_count,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
  if v_core_result is not null then
    update public.command_audit_log
    set payload = v_expected_audit_payload
    where id = v_audit_id and actor_user_id = v_actor
      and command = v_command and target_table is null
    returning payload into v_audit_payload;
  end if;
  -- Fresh execution promotes the core's v1 row first; replay never mutates it.
  -- In both cases an exact reread/compare precedes whole-scope proof so that
  -- audit_current cannot deterministically reject a just-created scope.
  if v_audit_payload is distinct from v_expected_audit_payload then
    if v_core_result is not null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PREFLIGHT_V2_AUDIT_PROMOTION_FAILED';
    end if;
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PREFLIGHT_V2_AUDIT_DRIFT', 'status', 409,
      'message', 'The authoritative v2 scope audit is missing or drifted'
    );
  end if;
  v_live := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, true
  );
  if coalesce((v_live->>'ok')::boolean, false) is false then
    if v_core_result is not null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PREFLIGHT_POST_SEAL_DRIFT';
    end if;
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
      'message', 'Sealed scope no longer matches live database state'
    );
  end if;
  if v_core_result is not null then
    v_permit_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
    insert into util.dataset_flow_identity_wrapper_invocations (
      scope_id, actor_user_id, approval_kind,
      approval_request_sha256, approval_text_sha256,
      approval_identity_sha256, admission_request_sha256,
      baseline_whole_scope_proof_sha256,
      token_sha256, maximum_process_posts, maximum_finalize_posts
    ) values (
      v_scope.id, v_actor, 'initial',
      v_scope.execution_approval_request_sha256,
      v_scope.approval_text_sha256,
      v_scope.approval_identity_sha256,
      v_scope.preflight_request_sha256,
      v_live->>'whole_scope_proof_sha256',
      private.dataset_flow_identity_permit_token_sha256_v1(v_permit_token),
      v_scope.process_count, 1
    ) returning id into v_invocation_id;
    v_execution_permit := jsonb_build_object(
      'schema_version', 'dataset-flow-identity-execution-permit.v1',
      'invocation_id', v_invocation_id,
      'generation', 0,
      'token', v_permit_token
    );
  end if;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-preflight-result.v2',
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'scope_id', v_scope.id,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_scope.status,
    'process_count', v_scope.process_count,
    'mapping_count', v_scope.mapping_count,
    'support_snapshot_count', v_receipt.support_count,
    'source_universe_count', 305,
    'rewrite_count', v_scope.rewrite_count,
    'next_ordinal', coalesce((
      select min(ledger.ordinal)
      from util.dataset_flow_identity_process_ledger as ledger
      where ledger.scope_id = v_scope.id and ledger.status = 'pending'
    ), v_scope.process_count + 1),
    'audit_id', v_audit_id::text,
    'replay', v_core_result is null,
    'execution_permit', v_execution_permit
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_PREFLIGHT_LOCK_BUSY', 'status', 409,
    'message', 'Scope seal could not acquire its deterministic fence'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_preflight_guarded(
  jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_preflight_guarded(
  jsonb
) to authenticated;

-- Read-only recovery for the narrow case where fresh preflight committed but
-- its HTTP response was lost.  Exact actor/artifact bindings locate the
-- already sealed scope; this surface never creates or returns a permit.
create or replace function public.cmd_dataset_flow_identity_scope_lookup(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '180s'
as $$
declare
  v_command constant text := 'cmd_dataset_flow_identity_scope_lookup';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_whole_result jsonb;
  v_audit_id bigint;
  v_next_ordinal integer;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if v_request is null or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'receipt_id', 'receipt_proof_sha256',
      'environment', 'project_ref', 'actor', 'target_visibility',
      'user_state_claim', 'operation_id', 'plan_sha256', 'freeze_sha256',
      'policy_approval_text_sha256', 'execution_approval_request_sha256',
      'execution_approval_text_sha256',
      'execution_approval_identity_sha256', 'toolchain_evidence_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-lookup.v1'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'receipt_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or v_request->>'user_state_claim'
      <> 'authenticated_actor_state_100_plus_own_state_0'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}')) is distinct from v_actor_email
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or exists (
      select 1 from unnest(array[
        'receipt_proof_sha256', 'plan_sha256', 'freeze_sha256',
        'policy_approval_text_sha256', 'execution_approval_request_sha256',
        'execution_approval_text_sha256',
        'execution_approval_identity_sha256', 'toolchain_evidence_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_INVALID_REQUEST', 'status', 400,
      'message', 'Scope lookup request schema mismatch'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.actor_user_id = v_actor
    and scope.request_id = (v_request->>'request_id')::uuid
    and scope.receipt_id = (v_request->>'receipt_id')::uuid
    and scope.receipt_proof_sha256 = v_request->>'receipt_proof_sha256'
    and scope.environment = v_request->>'environment'
    and scope.project_ref = v_request->>'project_ref'
    and scope.target_visibility = v_request->>'target_visibility'
    and scope.user_state_claim = v_request->>'user_state_claim'
    and scope.operation_id = v_request->>'operation_id'
    and scope.plan_sha256 = v_request->>'plan_sha256'
    and scope.freeze_sha256 = v_request->>'freeze_sha256'
    and scope.policy_approval_text_sha256 =
      v_request->>'policy_approval_text_sha256'
    and scope.execution_approval_request_sha256 =
      v_request->>'execution_approval_request_sha256'
    and scope.approval_text_sha256 =
      v_request->>'execution_approval_text_sha256'
    and scope.approval_identity_sha256 =
      v_request->>'execution_approval_identity_sha256'
    and scope.toolchain_evidence_sha256 =
      v_request->>'toolchain_evidence_sha256';
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_NOT_FOUND', 'status', 404,
      'message', 'No exactly bound actor-owned Step 3 scope exists'
    );
  end if;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor
    and receipt.receipt_proof_sha256 = v_scope.receipt_proof_sha256;
  if v_receipt.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, false
  );
  if coalesce((v_whole_result->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
      'message', 'Located scope no longer matches live database state'
    );
  end if;
  select audit.id into v_audit_id
  from public.command_audit_log as audit
  where audit.command = 'cmd_dataset_flow_identity_scope_preflight_guarded'
    and audit.actor_user_id = v_actor and audit.target_table is null
    and audit.payload->>'scope_id' = v_scope.id::text;
  if v_audit_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_LOOKUP_AUDIT_DRIFT', 'status', 409,
      'message', 'Authoritative scope-seal audit is missing'
    );
  end if;
  select coalesce(min(ledger.ordinal) filter (
    where ledger.status = 'pending'
  ), v_scope.process_count + 1)
  into v_next_ordinal
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-lookup-result.v1',
    'read_only', true,
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_scope.status,
    'process_count', v_scope.process_count,
    'mapping_count', v_scope.mapping_count,
    'support_snapshot_count', v_receipt.support_count,
    'source_universe_count', 305,
    'rewrite_count', v_scope.rewrite_count,
    'next_ordinal', v_next_ordinal,
    'audit_id', v_audit_id::text,
    'whole_scope_proof_sha256',
      v_whole_result->>'whole_scope_proof_sha256',
    'execution_permit', null
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_lookup(jsonb)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_lookup(jsonb)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_lookup(jsonb)
  to authenticated;

create or replace function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  p_scope_id uuid,
  p_request jsonb,
  p_authorization jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '90s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_process_rewrite_guarded';
  v_actor uuid := auth.uid();
  v_request jsonb;
  v_request_sha256 text;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_ledger util.dataset_flow_identity_process_ledger%rowtype;
  v_internal jsonb;
  v_internal_sha256 text;
  v_core_result jsonb;
  v_live jsonb;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
  v_completed_process_count integer;
  v_next_ordinal integer;
  v_primary_complete boolean;
  v_invocation_id uuid;
  v_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_execution_permit jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if p_scope_id is null or v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'scope_proof_sha256', 'ordinal',
      'process_intent_proof_sha256', 'process_request_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-process-rewrite.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or v_request->>'process_intent_proof_sha256' !~ '^[a-f0-9]{64}$'
    or v_request->>'process_request_sha256' !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(v_request->'ordinal') <> 'number'
    or (v_request->>'ordinal')::numeric <= 0
    or (v_request->>'ordinal')::numeric > 2147483647 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 thin process request schema mismatch'
    );
  end if;
  v_request_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    v_request - 'process_request_sha256'
  );
  if v_request_sha256 is distinct from
    v_request->>'process_request_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_REQUEST_HASH_MISMATCH',
      'status', 409, 'message', 'Thin process request hash mismatch'
    );
  end if;

  -- Active execution is O(1): the owner fence precedes the scope fence.
  -- The full public target/support activation set is acquired only once by
  -- fresh preflight; repeating it for every ordinal would be O(P^2).
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_SCOPE_BUSY', 'status', 409,
      'message', 'Another process transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null
    or v_scope.scope_proof_sha256
      is distinct from v_request->>'scope_proof_sha256'
    or v_scope.status in ('failed', 'cancelled') then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Scope proof is invalid or scope is not executable'
    );
  end if;
  v_invocation_id := private.dataset_flow_identity_validate_wrapper_permit_v1(
    v_actor, p_scope_id, p_authorization, 'process'
  );
  if v_invocation_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_WRAPPER_PERMIT_REQUIRED', 'status', 409,
      'message', 'A fresh one-wrapper execution permit is required'
    );
  end if;
  select invocation.* into strict v_invocation
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = v_invocation_id
    and invocation.scope_id = v_scope.id
    and invocation.actor_user_id = v_actor
  for update;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_scope.receipt_proof_sha256 then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  select ledger.* into v_ledger
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id
    and ledger.ordinal = (v_request->>'ordinal')::integer
  for update;
  if v_ledger.scope_id is null
    or v_ledger.process_intent_proof_sha256
      is distinct from v_request->>'process_intent_proof_sha256' then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_INTENT_PROOF_MISMATCH', 'status', 409,
      'message', 'Thin request does not bind the receipt process intent'
    );
  end if;
  if v_ledger.status = 'completed' then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_ALREADY_COMPLETED_READ_SCOPE',
      'status', 409,
      'message', 'Completed process proof is available only through scope read'
    );
  end if;
  -- Full-scope reproof is reserved for ambiguous replay/recovery.  A normal
  -- pending ordinal is protected by the active row fences and is revalidated
  -- against its exact process/mapping/support guards inside the private core.
  if v_ledger.status = 'completed' then
    v_live := private.dataset_flow_identity_whole_scope_proof_v2(
      v_actor, v_scope.receipt_id, v_scope.id, true
    );
    if coalesce((v_live->>'ok')::boolean, false) is false then
      perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
        v_invocation_id
      );
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
        'message', 'Whole-scope replay proof drifted'
      );
    end if;
  end if;

  v_internal := jsonb_build_object(
    'schema_version', 'dataset-flow-identity-process-rewrite.v1',
    'request_id', v_request->>'request_id',
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'ordinal', v_ledger.ordinal,
    'process', v_ledger.manifest,
    'rewrites', v_ledger.manifest->'rewrites',
    'collision_ledger', v_ledger.manifest->'collision_ledger',
    'collision_ledger_sha256',
      v_ledger.manifest->>'collision_ledger_sha256',
    'process_request_sha256', ''
  );
  v_internal_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    v_internal - 'process_request_sha256'
  );
  v_internal := jsonb_set(
    v_internal, '{process_request_sha256}', to_jsonb(v_internal_sha256), false
  );
  perform set_config(
    'app.dataset_flow_identity_v2_process_request_sha256',
    v_request_sha256, true
  );
  v_core_result := private.dataset_flow_identity_process_rewrite_core_v1(
    p_scope_id, v_internal
  );
  if coalesce((v_core_result->>'ok')::boolean, false) is false then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return v_core_result;
  end if;
  update util.dataset_flow_identity_process_ledger as ledger
  set wrapper_invocation_id = v_invocation.id,
    permit_generation_before = v_invocation.generation
  where ledger.scope_id = v_scope.id
    and ledger.ordinal = (v_request->>'ordinal')::integer
    and ledger.wrapper_invocation_id is null
    and ledger.permit_generation_before is null
  returning ledger.* into v_ledger;
  if not found then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PROCESS_INVOCATION_BINDING_FAILED';
  end if;
  if v_ledger.status <> 'completed'
    or v_ledger.process_request_sha256 is distinct from v_request_sha256
    or v_ledger.after_payload_sha256
      is distinct from v_ledger.manifest->>'desired_payload_sha256'
    or v_ledger.after_exchange_set_sha256
      is distinct from v_ledger.manifest->>'desired_exchange_set_sha256'
    or v_ledger.audit_id is null or v_ledger.derivative_batch_id is null then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PROCESS_POST_CORE_PROOF_MISMATCH';
  end if;
  select audit.payload into v_audit_payload
  from public.command_audit_log as audit
  where audit.id = v_ledger.audit_id and audit.actor_user_id = v_actor
    and audit.command = v_command and audit.target_table = 'processes'
    and audit.target_id = v_ledger.process_id
    and audit.target_version = v_ledger.process_version;
  if v_audit_payload is null then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_PROCESS_V2_AUDIT_MISSING';
  end if;
  v_expected_audit_payload := jsonb_build_object(
      'record_type', 'process_rewrite',
      'schema_version', 'dataset-flow-identity-process-rewrite.v2',
      'proof_domain', 'dataset-flow-identity-db-proof.v2',
      'scope_id', v_scope.id,
      'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
      'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'wrapper_invocation_id', v_invocation.id,
      'wrapper_approval_kind', v_invocation.approval_kind,
      'wrapper_approval_identity_sha256',
        v_invocation.approval_identity_sha256,
      'wrapper_admission_request_sha256',
        v_invocation.admission_request_sha256,
      'permit_generation_before', v_invocation.generation,
      'ordinal', v_ledger.ordinal,
      'process_intent_proof_sha256',
        v_ledger.process_intent_proof_sha256,
      'process_request_sha256', v_ledger.process_request_sha256,
      'process_template_sha256', v_ledger.process_template_sha256,
      'rewrite_set_sha256', v_ledger.manifest->>'rewrite_set_sha256',
      'collision_ledger_sha256',
        v_ledger.manifest->>'collision_ledger_sha256',
      'before_payload_sha256', v_ledger.before_payload_sha256,
      'before_exchange_set_sha256',
        v_ledger.manifest->>'before_exchange_set_sha256',
      'desired_payload_sha256',
        v_ledger.manifest->>'desired_payload_sha256',
      'desired_exchange_set_sha256',
        v_ledger.manifest->>'desired_exchange_set_sha256',
      'after_payload_sha256', v_ledger.after_payload_sha256,
      'after_exchange_set_sha256', v_ledger.after_exchange_set_sha256,
      'rewrite_count', v_ledger.rewrite_count,
      'derivative_batch_id', v_ledger.derivative_batch_id,
      'derivative_reason_code', 'FLOW_IDENTITY_SCOPE:'
        || v_scope.id::text || ':' || v_ledger.ordinal::text,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
  if coalesce((v_core_result->>'replay')::boolean, false) is false then
    update public.command_audit_log
    set payload = v_expected_audit_payload
    where id = v_ledger.audit_id and actor_user_id = v_actor
      and command = v_command and target_table = 'processes'
    returning payload into v_audit_payload;
    if v_audit_payload is distinct from v_expected_audit_payload then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_PROCESS_V2_AUDIT_PROMOTION_FAILED';
    end if;
  elsif v_audit_payload is distinct from v_expected_audit_payload then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PROCESS_V2_AUDIT_DRIFT', 'status', 409,
      'message', 'The authoritative v2 process audit drifted'
    );
  end if;
  if coalesce((v_core_result->>'replay')::boolean, false) then
    select
      count(*) filter (where ledger.status = 'completed')::integer,
      min(ledger.ordinal) filter (where ledger.status = 'pending')::integer
    into v_completed_process_count, v_next_ordinal
    from util.dataset_flow_identity_process_ledger as ledger
    where ledger.scope_id = v_scope.id;
  else
    v_completed_process_count := v_ledger.ordinal;
    v_next_ordinal := case when v_ledger.ordinal < v_scope.process_count
      then v_ledger.ordinal + 1 else null end;
  end if;
  v_primary_complete := v_next_ordinal is null
    and v_completed_process_count = v_scope.process_count;
  v_execution_permit :=
    private.dataset_flow_identity_rotate_wrapper_permit_v1(
      v_invocation_id, 'process', false
    );
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-process-rewrite-result.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_scope.receipt_id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'invocation_id', v_invocation.id,
    'permit_generation_before', v_invocation.generation,
    'ordinal', v_ledger.ordinal,
    'process_id', v_ledger.process_id,
    'process_version', v_ledger.process_version,
    'process_request_sha256', v_ledger.process_request_sha256,
    'process_intent_proof_sha256',
      v_ledger.process_intent_proof_sha256,
    'before_payload_sha256', v_ledger.before_payload_sha256,
    'before_exchange_set_sha256',
      v_ledger.manifest->>'before_exchange_set_sha256',
    'desired_payload_sha256',
      v_ledger.manifest->>'desired_payload_sha256',
    'desired_exchange_set_sha256',
      v_ledger.manifest->>'desired_exchange_set_sha256',
    'after_payload_sha256', v_ledger.after_payload_sha256,
    'after_exchange_set_sha256', v_ledger.after_exchange_set_sha256,
    'rewrite_count', v_ledger.rewrite_count,
    'audit_id', v_ledger.audit_id::text,
    'derivative_batch_id', v_ledger.derivative_batch_id,
    'completed_process_count', v_completed_process_count,
    'next_ordinal', v_next_ordinal,
    'primary_complete', v_primary_complete,
    'status', v_ledger.status,
    'replay', coalesce((v_core_result->>'replay')::boolean, false),
    'execution_permit', v_execution_permit
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_PROCESS_LOCK_BUSY', 'status', 409,
    'message', 'Process transaction could not acquire its deterministic fence'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb, jsonb
) owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_process_rewrite_guarded(
  uuid, jsonb, jsonb
) to authenticated;

create or replace function public.cmd_dataset_flow_identity_scope_read(
  p_scope_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = ''
set statement_timeout = '90s'
as $$
declare
  v_command constant text := 'cmd_dataset_flow_identity_scope_read';
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_core jsonb;
  v_whole_result jsonb;
  v_whole jsonb;
  v_processes jsonb;
  v_status text;
  v_live_drift boolean;
  v_terminal_conflict boolean;
  v_compensation_required boolean;
  v_result jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_READ_INVALID_REQUEST', 'status', 400,
      'message', 'Scope ID is required'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_scope.receipt_proof_sha256 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  v_core := private.dataset_flow_identity_scope_read_core_v1(p_scope_id);
  if v_core->>'code' in ('AUTH_REQUIRED', 'FLOW_IDENTITY_SCOPE_NOT_FOUND') then
    return v_core;
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, false
  );
  v_whole := v_whole_result->'whole_scope_proof';
  if v_whole is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT', 'status', 409,
      'message', 'Whole-scope proof could not be constructed'
    );
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'ordinal', ledger.ordinal,
    'id', ledger.process_id,
    'version', ledger.process_version,
    'status', ledger.status,
    'process_request_sha256', ledger.process_request_sha256,
    'process_intent_proof_sha256', ledger.process_intent_proof_sha256,
    'rewrite_count', ledger.rewrite_count,
    'audit_id', case when ledger.audit_id is null
      then null else ledger.audit_id::text end,
    'before_payload_sha256', ledger.before_payload_sha256,
    'before_exchange_set_sha256',
      ledger.manifest->>'before_exchange_set_sha256',
    'desired_payload_sha256', ledger.manifest->>'desired_payload_sha256',
    'desired_exchange_set_sha256',
      ledger.manifest->>'desired_exchange_set_sha256',
    'after_payload_sha256', ledger.after_payload_sha256,
    'after_exchange_set_sha256', ledger.after_exchange_set_sha256,
    'derivative_batch_id', ledger.derivative_batch_id,
    'derivative_request_id', child.id,
    'derivative_status', case when ledger.derivative_batch_id is null
      then null else coalesce(child.status, 'missing') end,
    'causal_terminal_proof', false,
    'completed_at', ledger.completed_at,
    'last_error', coalesce(ledger.last_error, child.last_error)
  ) order by ledger.ordinal), '[]'::jsonb)
  into v_processes
  from util.dataset_flow_identity_process_ledger as ledger
  left join util.dataset_derivative_rebuild_requests as child
    on child.actor_user_id = v_actor
    and child.batch_id = ledger.derivative_batch_id
    and child.target_table = 'processes'
    and child.target_id = ledger.process_id
    and child.target_version = ledger.process_version
  where ledger.scope_id = p_scope_id;
  v_terminal_conflict := v_scope.status in ('cancelled', 'failed');
  v_live_drift := v_terminal_conflict
    or coalesce((v_whole_result->>'ok')::boolean, false) is false;
  v_status := case when v_live_drift then 'live_drift'
    else v_core->>'status' end;
  v_compensation_required := not v_live_drift
    and coalesce((v_core->>'derivative_failed_count')::integer, 0) > 0;
  v_result := jsonb_build_object(
    'ok', v_status not in ('failed', 'live_drift')
      and not v_compensation_required,
    'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-status.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_status,
    'process_count', (v_core->>'process_count')::integer,
    'completed_process_count',
      (v_core->>'completed_process_count')::integer,
    'pending_process_count', (v_core->>'pending_process_count')::integer,
    'failed_process_count', (v_core->>'failed_process_count')::integer,
    'next_ordinal', (v_core->>'next_ordinal')::integer,
    'rewrite_count', (v_core->>'rewrite_count')::integer,
    'completed_rewrite_count',
      (v_core->>'completed_rewrite_count')::integer,
    'primary_complete', (v_core->>'primary_complete')::boolean,
    'cancellable', (v_core->>'cancellable')::boolean,
    'strict_continuation_required',
      (v_core->>'strict_continuation_required')::boolean,
    'primary_current', (v_whole->>'primary_current')::boolean,
    'live_guard_current',
      (v_whole->>'audit_current')::boolean
      and (v_whole->>'source_guards_current')::boolean
      and (v_whole->>'support_guards_current')::boolean
      and (v_whole->>'target_guards_current')::boolean
      and (v_whole->>'protected_closure_current')::boolean
      and (v_whole->>'occurrence_closure_current')::boolean,
    'derivatives_current', case when v_live_drift then false
      else (v_whole->>'derivatives_current')::boolean end,
    'derivative_pending_count',
      (v_core->>'derivative_pending_count')::integer,
    'derivative_failed_count',
      (v_core->>'derivative_failed_count')::integer,
    'derivative_set_proof', v_whole_result->'derivative_set_proof',
    'derivative_proof_set_sha256',
      v_whole_result #>> '{derivative_set_proof,proof_sha256}',
    'compensation_required', v_compensation_required,
    'automatic_retry', false,
    'compensation_targets', case when v_compensation_required
      then v_core->'compensation_targets' else '[]'::jsonb end,
    'protected_closure_current',
      (v_whole->>'protected_closure_current')::boolean,
    'protected_closure_proof', v_core->'protected_closure_proof',
    'processes', v_processes,
    'terminal_proof_sha256', case when v_status = 'completed'
      then v_scope.terminal_proof_sha256 else null end,
    'completed_at', case when v_status = 'completed'
      then v_scope.completed_at else null end,
    'whole_scope_proof', v_whole,
    'whole_scope_proof_sha256',
      v_whole_result->>'whole_scope_proof_sha256'
  );
  if v_live_drift then
    v_result := v_result || jsonb_build_object(
      'code', case when v_terminal_conflict
        then 'FLOW_IDENTITY_SCOPE_TERMINAL_CONFLICT'
        else 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT' end
    );
  elsif v_compensation_required then
    v_result := v_result || jsonb_build_object(
      'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
    );
  end if;
  return v_result;
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_read(uuid)
  owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_read(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_read(uuid)
  to authenticated;

create or replace function public.cmd_dataset_flow_identity_scope_recover_guarded(
  p_scope_id uuid,
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '180s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_recover_guarded';
  v_actor uuid := auth.uid();
  v_actor_email text := lower(btrim(auth.email()));
  v_request jsonb;
  v_wire_request_sha256 text;
  v_approved_at timestamp with time zone;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_existing util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_invocation_id uuid;
  v_whole_result jsonb;
  v_completed_count integer;
  v_next_ordinal integer;
  v_remaining_count integer;
  v_mode text;
  v_token text;
  v_audit_id bigint;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if p_scope_id is null or v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'approved_at_utc',
      'environment', 'project_ref', 'actor', 'target_visibility',
      'user_state_claim', 'operation_id', 'plan_sha256', 'freeze_sha256',
      'original_execution_approval_identity_sha256', 'scope_proof_sha256',
      'observed_scope_status', 'observed_completed_process_count',
      'observed_next_ordinal', 'observed_whole_scope_proof_sha256',
      'recovery_mode', 'recovery_reason', 'toolchain_evidence_sha256',
      'maximum_wrapper_invocations', 'maximum_process_posts',
      'maximum_finalize_posts', 'maximum_cli_apply_spawns',
      'approval_reusable', 'automatic_retry',
      'recovery_approval_request_sha256',
      'recovery_approval_text_sha256',
      'recovery_approval_identity_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-recovery.v1'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'target_visibility' <> 'owner_draft'
    or v_request->>'user_state_claim'
      <> 'authenticated_actor_state_100_plus_own_state_0'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'actor', array['user_id', 'email']
    )
    or v_request #>> '{actor,user_id}' is distinct from v_actor::text
    or lower(btrim(v_request #>> '{actor,email}')) is distinct from v_actor_email
    or v_request->>'observed_scope_status' not in (
      'sealed', 'running', 'primary_complete', 'derivatives_pending'
    )
    or v_request->>'recovery_mode' not in (
      'resume_and_finalize', 'finalize_only'
    )
    or v_request->>'recovery_reason' not in (
      'wrapper_exited_without_permit', 'process_response_ambiguous',
      'process_domain_rejected', 'finalize_response_ambiguous',
      'derivatives_became_ready_after_wrapper_exit'
    )
    or jsonb_typeof(v_request->'observed_completed_process_count') <> 'number'
    or (v_request->>'observed_completed_process_count')::numeric < 0
    or (v_request->>'observed_completed_process_count')::numeric > 2147483647
    or jsonb_typeof(v_request->'observed_next_ordinal') <> 'number'
    or (v_request->>'observed_next_ordinal')::numeric < 1
    or (v_request->>'observed_next_ordinal')::numeric > 2147483647
    or (v_request->>'observed_next_ordinal')::numeric <>
      trunc((v_request->>'observed_next_ordinal')::numeric)
    or jsonb_typeof(v_request->'maximum_wrapper_invocations') <> 'number'
    or (v_request->>'maximum_wrapper_invocations')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_process_posts') <> 'number'
    or (v_request->>'maximum_process_posts')::numeric < 0
    or (v_request->>'maximum_process_posts')::numeric > 2147483647
    or jsonb_typeof(v_request->'maximum_finalize_posts') <> 'number'
    or (v_request->>'maximum_finalize_posts')::numeric <> 1
    or jsonb_typeof(v_request->'maximum_cli_apply_spawns') <> 'number'
    or (v_request->>'maximum_cli_apply_spawns')::numeric <> 1
    or v_request->'approval_reusable' is distinct from 'false'::jsonb
    or v_request->'automatic_retry' is distinct from 'false'::jsonb
    or exists (
      select 1 from unnest(array[
        'plan_sha256', 'freeze_sha256',
        'original_execution_approval_identity_sha256', 'scope_proof_sha256',
        'observed_whole_scope_proof_sha256', 'toolchain_evidence_sha256',
        'recovery_approval_request_sha256',
        'recovery_approval_text_sha256',
        'recovery_approval_identity_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_INVALID_REQUEST', 'status', 400,
      'message', 'Recovery approval request schema mismatch'
    );
  end if;
  begin
    v_approved_at := (v_request->>'approved_at_utc')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_INVALID_APPROVAL_TIME', 'status', 400,
      'message', 'Recovery approval timestamp is invalid'
    );
  end;
  v_wire_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);
  if cardinality(array[
      v_request->>'recovery_approval_request_sha256',
      v_request->>'recovery_approval_text_sha256',
      v_request->>'recovery_approval_identity_sha256'
    ]) <> (
      select count(distinct value)
      from unnest(array[
        v_request->>'recovery_approval_request_sha256',
        v_request->>'recovery_approval_text_sha256',
        v_request->>'recovery_approval_identity_sha256'
      ]) as approval(value)
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_APPROVAL_HASH_MISMATCH', 'status', 409,
      'message', 'Recovery approval artifact hashes must be distinct'
    );
  end if;

  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  if v_scope.status in ('completed', 'failed', 'cancelled')
    or v_scope.scope_proof_sha256 is distinct from
      v_request->>'scope_proof_sha256'
    or v_scope.environment is distinct from v_request->>'environment'
    or v_scope.project_ref is distinct from v_request->>'project_ref'
    or v_scope.target_visibility is distinct from
      v_request->>'target_visibility'
    or v_scope.user_state_claim is distinct from
      v_request->>'user_state_claim'
    or v_scope.operation_id is distinct from v_request->>'operation_id'
    or v_scope.plan_sha256 is distinct from v_request->>'plan_sha256'
    or v_scope.freeze_sha256 is distinct from v_request->>'freeze_sha256'
    or v_scope.approval_identity_sha256 is distinct from
      v_request->>'original_execution_approval_identity_sha256'
    or v_approved_at < v_scope.sealed_at
    or exists (
      select 1 from unnest(array[
        v_request->>'recovery_approval_request_sha256',
        v_request->>'recovery_approval_text_sha256',
        v_request->>'recovery_approval_identity_sha256'
      ]) as recovery(value)
      where recovery.value = any(array[
        v_scope.policy_approval_text_sha256,
        v_scope.execution_approval_request_sha256,
        v_scope.approval_text_sha256,
        v_scope.approval_identity_sha256
      ])
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_SCOPE_BINDING_MISMATCH', 'status', 409,
      'message', 'Recovery approval does not bind this active scope'
    );
  end if;

  select invocation.* into v_existing
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.actor_user_id = v_actor
    and array[
      v_request->>'recovery_approval_request_sha256',
      v_request->>'recovery_approval_text_sha256',
      v_request->>'recovery_approval_identity_sha256'
    ] && array[
      invocation.approval_request_sha256,
      invocation.approval_text_sha256,
      invocation.approval_identity_sha256
    ]
  order by invocation.admitted_at, invocation.id
  limit 1;
  if v_existing.id is not null then
    if v_existing.scope_id is distinct from v_scope.id
      or v_existing.approval_kind <> 'recovery'
      or v_existing.approval_request_sha256 is distinct from
        v_request->>'recovery_approval_request_sha256'
      or v_existing.approval_text_sha256 is distinct from
        v_request->>'recovery_approval_text_sha256'
      or v_existing.approval_identity_sha256 is distinct from
        v_request->>'recovery_approval_identity_sha256'
      or v_existing.admission_request_sha256 is distinct from
        v_wire_request_sha256
      or v_existing.baseline_whole_scope_proof_sha256 is distinct from
        v_request->>'observed_whole_scope_proof_sha256' then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_RECOVERY_APPROVAL_REUSE_MISMATCH',
        'status', 409,
        'message', 'Recovery approval was already consumed differently'
      );
    end if;
    select audit.id, audit.payload into v_audit_id, v_audit_payload
    from public.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
      and audit.payload->>'recovery_approval_identity_sha256'
        = v_existing.approval_identity_sha256;
    if v_audit_id is null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_RECOVERY_REPLAY_AUDIT_MISSING';
    end if;
    v_expected_audit_payload := jsonb_build_object(
      'record_type', 'scope_recovery_admission',
      'schema_version', 'dataset-flow-identity-scope-recovery.v1',
      'scope_id', v_scope.id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'operation_id', v_scope.operation_id,
      'plan_sha256', v_scope.plan_sha256,
      'freeze_sha256', v_scope.freeze_sha256,
      'original_execution_approval_identity_sha256',
        v_scope.approval_identity_sha256,
      'observed_whole_scope_proof_sha256',
        v_request->>'observed_whole_scope_proof_sha256',
      'observed_scope_status', v_request->>'observed_scope_status',
      'observed_completed_process_count',
        (v_request->>'observed_completed_process_count')::integer,
      'observed_next_ordinal',
        (v_request->>'observed_next_ordinal')::integer,
      'recovery_mode', v_request->>'recovery_mode',
      'recovery_reason', v_request->>'recovery_reason',
      'toolchain_evidence_sha256', v_request->>'toolchain_evidence_sha256',
      'recovery_approval_request_sha256',
        v_request->>'recovery_approval_request_sha256',
      'recovery_approval_text_sha256',
        v_request->>'recovery_approval_text_sha256',
      'recovery_approval_identity_sha256',
        v_request->>'recovery_approval_identity_sha256',
      'recovery_wire_request_sha256', v_wire_request_sha256,
      'invocation_id', v_existing.id,
      'maximum_wrapper_invocations', 1,
      'maximum_process_posts',
        (v_request->>'maximum_process_posts')::integer,
      'maximum_finalize_posts', 1,
      'maximum_cli_apply_spawns', 1,
      'approval_reusable', false,
      'automatic_retry', false,
      'hash_algorithm', 'restricted-safe-json-v2-sha256'
    );
    if v_audit_payload is distinct from v_expected_audit_payload then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_RECOVERY_AUDIT_DRIFT', 'status', 409,
        'message', 'Authoritative recovery admission audit drifted'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', 'dataset-flow-identity-scope-recovery-result.v1',
      'scope_id', v_scope.id,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'status', v_audit_payload->>'observed_scope_status',
      'completed_process_count',
        (v_audit_payload->>'observed_completed_process_count')::integer,
      'next_ordinal', (v_audit_payload->>'observed_next_ordinal')::integer,
      'whole_scope_proof_sha256',
        v_existing.baseline_whole_scope_proof_sha256,
      'recovery_wire_request_sha256',
        v_existing.admission_request_sha256,
      'recovery_approval_identity_sha256',
        v_existing.approval_identity_sha256,
      'invocation_id', v_existing.id,
      'audit_id', case when v_audit_id is null then null else v_audit_id::text end,
      'replay', true,
      'execution_permit', null
    );
  end if;

  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing'
    );
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, true
  );
  if coalesce((v_whole_result->>'ok')::boolean, false) is false
    or v_whole_result->>'whole_scope_proof_sha256' is distinct from
      v_request->>'observed_whole_scope_proof_sha256' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_LIVE_PROOF_MISMATCH', 'status', 409,
      'message', 'Current whole-scope proof does not match the approval'
    );
  end if;
  select count(*) filter (where ledger.status = 'completed')::integer,
    coalesce(
      min(ledger.ordinal) filter (where ledger.status = 'pending')::integer,
      v_scope.process_count + 1
    )
  into v_completed_count, v_next_ordinal
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  v_remaining_count := v_scope.process_count - v_completed_count;
  v_mode := case when v_remaining_count = 0
    then 'finalize_only' else 'resume_and_finalize' end;
  if v_scope.status is distinct from v_request->>'observed_scope_status'
    or v_completed_count is distinct from
      (v_request->>'observed_completed_process_count')::integer
    or (v_request->>'observed_next_ordinal')::integer <> v_next_ordinal
    or v_mode is distinct from v_request->>'recovery_mode'
    or v_remaining_count is distinct from
      (v_request->>'maximum_process_posts')::integer then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_RECOVERY_PROGRESS_MISMATCH', 'status', 409,
      'message', 'Recovery approval does not match current durable progress'
    );
  end if;

  update util.dataset_flow_identity_wrapper_invocations
  set status = 'superseded', generation = generation + 1,
    token_sha256 = private.dataset_flow_identity_permit_token_sha256_v1(
      pg_catalog.encode(extensions.gen_random_bytes(32), 'hex')
    ),
    updated_at = clock_timestamp(), closed_at = clock_timestamp()
  where scope_id = v_scope.id and status = 'active';
  v_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
  insert into util.dataset_flow_identity_wrapper_invocations (
    scope_id, actor_user_id, approval_kind,
    approval_request_sha256, approval_text_sha256,
    approval_identity_sha256, admission_request_sha256,
    baseline_whole_scope_proof_sha256,
    token_sha256, maximum_process_posts, maximum_finalize_posts
  ) values (
    v_scope.id, v_actor, 'recovery',
    v_request->>'recovery_approval_request_sha256',
    v_request->>'recovery_approval_text_sha256',
    v_request->>'recovery_approval_identity_sha256',
    v_wire_request_sha256,
    v_request->>'observed_whole_scope_proof_sha256',
    private.dataset_flow_identity_permit_token_sha256_v1(v_token),
    v_remaining_count, 1
  ) returning id into v_invocation_id;
  v_audit_payload := jsonb_build_object(
    'record_type', 'scope_recovery_admission',
    'schema_version', 'dataset-flow-identity-scope-recovery.v1',
    'scope_id', v_scope.id,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'freeze_sha256', v_scope.freeze_sha256,
    'original_execution_approval_identity_sha256',
      v_scope.approval_identity_sha256,
    'observed_whole_scope_proof_sha256',
      v_request->>'observed_whole_scope_proof_sha256',
    'observed_scope_status', v_scope.status,
    'observed_completed_process_count', v_completed_count,
    'observed_next_ordinal', v_next_ordinal,
    'recovery_mode', v_mode,
    'recovery_reason', v_request->>'recovery_reason',
    'toolchain_evidence_sha256', v_request->>'toolchain_evidence_sha256',
    'recovery_approval_request_sha256',
      v_request->>'recovery_approval_request_sha256',
    'recovery_approval_text_sha256',
      v_request->>'recovery_approval_text_sha256',
    'recovery_approval_identity_sha256',
      v_request->>'recovery_approval_identity_sha256',
    'recovery_wire_request_sha256', v_wire_request_sha256,
    'invocation_id', v_invocation_id,
    'maximum_wrapper_invocations', 1,
    'maximum_process_posts', v_remaining_count,
    'maximum_finalize_posts', 1,
    'maximum_cli_apply_spawns', 1,
    'approval_reusable', false,
    'automatic_retry', false,
    'hash_algorithm', 'restricted-safe-json-v2-sha256'
  );
  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null, v_audit_payload
  ) returning id into v_audit_id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-recovery-result.v1',
    'scope_id', v_scope.id,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_scope.status,
    'completed_process_count', v_completed_count,
    'next_ordinal', v_next_ordinal,
    'whole_scope_proof_sha256',
      v_request->>'observed_whole_scope_proof_sha256',
    'recovery_wire_request_sha256', v_wire_request_sha256,
    'recovery_approval_identity_sha256',
      v_request->>'recovery_approval_identity_sha256',
    'invocation_id', v_invocation_id,
    'audit_id', v_audit_id::text,
    'replay', false,
    'execution_permit', jsonb_build_object(
      'schema_version', 'dataset-flow-identity-execution-permit.v1',
      'invocation_id', v_invocation_id,
      'generation', 0,
      'token', v_token
    )
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_RECOVERY_LOCK_BUSY', 'status', 409,
    'message', 'Recovery could not acquire its deterministic fence'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_recover_guarded(
  uuid, jsonb
) owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_recover_guarded(
  uuid, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_recover_guarded(
  uuid, jsonb
) to authenticated;

create or replace function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  p_scope_id uuid,
  p_request jsonb,
  p_authorization jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '180s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_finalize_guarded';
  v_actor uuid := auth.uid();
  v_request jsonb;
  v_request_sha256 text;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_whole_result jsonb;
  v_whole jsonb;
  v_internal jsonb;
  v_core jsonb;
  v_derivative_targets jsonb;
  v_derivative_target_set_sha256 text;
  v_terminal_proof_sha256 text;
  v_final_audit_id bigint;
  v_final_audit_payload jsonb;
  v_expected_final_audit_payload jsonb;
  v_live_drift boolean;
  v_live_guard_current boolean;
  v_was_completed boolean;
  v_result jsonb;
  v_invocation_id uuid;
  v_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_execution_permit jsonb;
  v_permit_consumed boolean := false;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if p_scope_id is null or v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'scope_proof_sha256', 'expected'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-finalize.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'scope_proof_sha256' !~ '^[a-f0-9]{64}$'
    or not private.dataset_flow_identity_exact_keys(
      v_request->'expected', array[
        'process_count', 'rewrite_count', 'completed_process_count'
      ]
    )
    or exists (
      select 1 from unnest(array[
        'process_count', 'rewrite_count', 'completed_process_count'
      ]) as field(name)
      where jsonb_typeof(v_request->'expected'->field.name) <> 'number'
        or (v_request->'expected'->>field.name)::numeric <= 0
        or (v_request->'expected'->>field.name)::numeric > 2147483647
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 thin finalize request schema mismatch'
    );
  end if;
  v_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null
    or v_scope.scope_proof_sha256
      is distinct from v_request->>'scope_proof_sha256'
    or v_scope.status in ('failed', 'cancelled')
    or (v_request #>> '{expected,process_count}')::integer
      <> v_scope.process_count
    or (v_request #>> '{expected,rewrite_count}')::integer
      <> v_scope.rewrite_count
    or (v_request #>> '{expected,completed_process_count}')::integer
      <> v_scope.process_count then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Finalize request does not bind the actor scope and counts'
    );
  end if;
  v_invocation_id := private.dataset_flow_identity_validate_wrapper_permit_v1(
    v_actor, p_scope_id, p_authorization, 'finalize'
  );
  if v_invocation_id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_WRAPPER_PERMIT_REQUIRED', 'status', 409,
      'message', 'A fresh one-wrapper execution permit is required'
    );
  end if;
  select invocation.* into strict v_invocation
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = v_invocation_id
    and invocation.scope_id = v_scope.id
    and invocation.actor_user_id = v_actor
  for update;
  v_was_completed := v_scope.status = 'completed';
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.receipt_proof_sha256
      is distinct from v_scope.receipt_proof_sha256 then
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_FINALIZE_RECEIPT_DRIFT', 'status', 409,
      'message', 'Scope receipt relation is missing or drifted'
    );
  end if;
  v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
    v_actor, v_receipt.id, v_scope.id, true
  );
  v_whole := v_whole_result->'whole_scope_proof';
  v_live_drift := coalesce((v_whole_result->>'ok')::boolean, false) is false;
  v_live_guard_current := coalesce(
      (v_whole->>'audit_current')::boolean, false
    ) and coalesce((v_whole->>'source_guards_current')::boolean, false
    ) and coalesce((v_whole->>'support_guards_current')::boolean, false)
    and coalesce((v_whole->>'target_guards_current')::boolean, false)
    and coalesce((v_whole->>'protected_closure_current')::boolean, false)
    and coalesce((v_whole->>'occurrence_closure_current')::boolean, false);
  select coalesce(jsonb_agg(jsonb_build_object(
    'ordinal', ledger.ordinal,
    'id', ledger.process_id,
    'version', ledger.process_version,
    'desired_json_ordered_sha256',
      ledger.manifest->>'desired_payload_sha256',
    'baseline_snapshot_sha256',
      ledger.manifest->>'derivative_baseline_snapshot_sha256'
  ) order by ledger.ordinal), '[]'::jsonb)
  into v_derivative_targets
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  v_derivative_target_set_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_derivative_targets);
  if not v_live_drift then
    v_internal := jsonb_build_object(
      'schema_version', 'dataset-flow-identity-scope-finalize.v1',
      'request_id', v_request->>'request_id',
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'expected', jsonb_build_object(
        'process_count', v_scope.process_count,
        'rewrite_count', v_scope.rewrite_count,
        'completed_process_count', v_scope.process_count,
        'primary_closure_sha256', v_whole->>'primary_closure_sha256',
        'protected_closure_sha256', v_scope.protected_closure_sha256,
        'derivative_target_set_sha256', v_derivative_target_set_sha256
      )
    );
    perform set_config(
      'app.dataset_flow_identity_v2_finalize_request_sha256',
      v_request_sha256, true
    );
    v_core := private.dataset_flow_identity_scope_finalize_core_v1(
      p_scope_id, v_internal
    );
    if v_core->>'status' not in ('derivatives_pending', 'completed') then
      v_core := jsonb_build_object(
        'ok', false,
        'status', 'failed',
        'code', 'FLOW_IDENTITY_FINALIZE_FAILED',
        'compensation_required', false,
        'compensation_targets', '[]'::jsonb
      );
    end if;
    select scope.* into v_scope
    from util.dataset_flow_identity_scopes as scope
    where scope.id = p_scope_id and scope.actor_user_id = v_actor
    for update;
    -- A fresh completion is promoted using the already current pre-core proof.
    -- Re-entering whole-scope proof before the v2 terminal audit exists would
    -- deterministically mark audit_current false.  Pending and replay paths do
    -- not have that transitional state and are re-read immediately.
    if v_core->>'status' <> 'completed' or v_was_completed then
      v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
        v_actor, v_receipt.id, v_scope.id, true
      );
      v_whole := v_whole_result->'whole_scope_proof';
      v_live_drift :=
        coalesce((v_whole_result->>'ok')::boolean, false) is false;
      v_live_guard_current := coalesce(
          (v_whole->>'audit_current')::boolean, false
        ) and coalesce((v_whole->>'source_guards_current')::boolean, false
        ) and coalesce((v_whole->>'support_guards_current')::boolean, false)
        and coalesce((v_whole->>'target_guards_current')::boolean, false)
        and coalesce((v_whole->>'protected_closure_current')::boolean, false)
        and coalesce((v_whole->>'occurrence_closure_current')::boolean, false);
    end if;
  end if;
  -- Completed replay is dynamic, not a trust in the stored scope status.  A
  -- stale/failed derivative downgrades to pending/compensation and must never
  -- rewrite the terminal proof or terminal audit.
  if not v_live_drift and v_core->>'status' = 'completed'
    and (
      coalesce((v_whole->>'derivatives_current')::boolean, false) is false
      or coalesce((v_whole->>'causal_terminal_proof')::boolean, false) is false
    ) then
    v_core := jsonb_build_object(
      'ok', coalesce((v_whole_result #>>
        '{derivative_set_proof,failed_count}')::integer, 0) = 0,
      'status', 'derivatives_pending',
      'code', case when coalesce((v_whole_result #>>
          '{derivative_set_proof,failed_count}')::integer, 0) > 0
        then 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
        else 'FLOW_IDENTITY_DERIVATIVES_PENDING' end,
      'compensation_required', coalesce((v_whole_result #>>
        '{derivative_set_proof,failed_count}')::integer, 0) > 0,
      'compensation_targets', coalesce(
        v_whole_result #> '{derivative_set_proof,compensation_targets}',
        '[]'::jsonb
      )
    );
  end if;
  if not v_live_drift and v_core->>'status' = 'completed'
      and not v_was_completed then
    v_terminal_proof_sha256 :=
      util.dataset_flow_identity_restricted_sha256_v2(jsonb_build_object(
        'schema_version', 'dataset-flow-identity-terminal-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'plan_sha256', v_scope.plan_sha256,
        'final_request_sha256', v_request_sha256,
        'wrapper_invocation_id', v_invocation.id,
        'wrapper_approval_kind', v_invocation.approval_kind,
        'wrapper_approval_identity_sha256',
          v_invocation.approval_identity_sha256,
        'wrapper_admission_request_sha256',
          v_invocation.admission_request_sha256,
        'permit_generation_before', v_invocation.generation,
        'whole_scope_proof_sha256',
          v_whole_result->>'whole_scope_proof_sha256',
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'derivative_target_set_sha256', v_derivative_target_set_sha256,
        'derivative_proof_set_sha256',
          v_whole->>'derivative_proof_set_sha256'
      ));
    update util.dataset_flow_identity_scopes
    set terminal_proof_sha256 = v_terminal_proof_sha256,
      final_wrapper_invocation_id = v_invocation.id,
      final_permit_generation_before = v_invocation.generation,
      updated_at = clock_timestamp()
    where id = v_scope.id;
    v_scope.terminal_proof_sha256 := v_terminal_proof_sha256;
    select audit.id, audit.payload
    into v_final_audit_id, v_final_audit_payload
    from public.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
    order by audit.id desc limit 1;
    if v_final_audit_id is null then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_FINALIZE_V2_AUDIT_MISSING';
    end if;
    v_expected_final_audit_payload := jsonb_build_object(
        'record_type', 'scope_terminal',
        'schema_version', 'dataset-flow-identity-scope-finalize.v2',
        'proof_domain', 'dataset-flow-identity-db-proof.v2',
        'scope_id', v_scope.id,
        'receipt_id', v_receipt.id,
        'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
        'operation_id', v_scope.operation_id,
        'plan_sha256', v_scope.plan_sha256,
        'scope_proof_sha256', v_scope.scope_proof_sha256,
        'final_request_sha256', v_request_sha256,
        'wrapper_invocation_id', v_invocation.id,
        'wrapper_approval_kind', v_invocation.approval_kind,
        'wrapper_approval_identity_sha256',
          v_invocation.approval_identity_sha256,
        'wrapper_admission_request_sha256',
          v_invocation.admission_request_sha256,
        'permit_generation_before', v_invocation.generation,
        'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
        'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
        'protected_closure_sha256', v_receipt.protected_closure_sha256,
        'primary_closure_sha256', v_whole->>'primary_closure_sha256',
        'derivative_target_set_sha256', v_derivative_target_set_sha256,
        'derivative_proof_set_sha256',
          v_whole->>'derivative_proof_set_sha256',
        'whole_scope_proof_sha256',
          v_whole_result->>'whole_scope_proof_sha256',
        'terminal_proof_sha256', v_terminal_proof_sha256,
        'process_count', v_scope.process_count,
        'rewrite_count', v_scope.rewrite_count,
        'hash_algorithm', 'restricted-safe-json-v2-sha256'
      );
    update public.command_audit_log
    set payload = v_expected_final_audit_payload
    where id = v_final_audit_id and actor_user_id = v_actor
      and command = v_command and target_table is null
    returning payload into v_final_audit_payload;
    if v_final_audit_payload
        is distinct from v_expected_final_audit_payload then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_FINALIZE_V2_AUDIT_PROMOTION_FAILED';
    end if;
    -- Terminal consumption is part of the same transaction and precedes the
    -- durable post-promotion proof.  The proof therefore requires the exact
    -- N -> N+1 terminal rotation and one successful finalize post.
    v_execution_permit :=
      private.dataset_flow_identity_rotate_wrapper_permit_v1(
        v_invocation_id, 'finalize', true
      );
    v_permit_consumed := true;
    v_whole_result := private.dataset_flow_identity_whole_scope_proof_v2(
      v_actor, v_receipt.id, v_scope.id, true
    );
    v_whole := v_whole_result->'whole_scope_proof';
    v_live_drift :=
      coalesce((v_whole_result->>'ok')::boolean, false) is false;
    v_live_guard_current := coalesce(
        (v_whole->>'audit_current')::boolean, false
      ) and coalesce((v_whole->>'source_guards_current')::boolean, false
      ) and coalesce((v_whole->>'support_guards_current')::boolean, false)
      and coalesce((v_whole->>'target_guards_current')::boolean, false)
      and coalesce((v_whole->>'protected_closure_current')::boolean, false)
      and coalesce((v_whole->>'occurrence_closure_current')::boolean, false);
    if v_live_drift then
      raise exception using errcode = 'P0001',
        message = 'FLOW_IDENTITY_FINALIZE_POST_PROMOTION_DRIFT';
    end if;
  else
    select audit.id into v_final_audit_id
    from public.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
    order by audit.id desc limit 1;
  end if;
  v_result := jsonb_build_object(
    'ok', not v_live_drift
      and coalesce(v_core->>'status', '') <> 'failed'
      and coalesce((v_core->>'compensation_required')::boolean, false) is false,
    'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-finalize-result.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'mapping_guard_set_sha256', v_receipt.mapping_guard_set_sha256,
    'process_intent_set_sha256', v_receipt.process_intent_set_sha256,
    'invocation_id', v_invocation.id,
    'permit_generation_before', v_invocation.generation,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', case when v_live_drift then 'live_drift'
      else v_core->>'status' end,
    'process_count', v_scope.process_count,
    'completed_process_count',
      (v_request #>> '{expected,completed_process_count}')::integer,
    'rewrite_count', v_scope.rewrite_count,
    'primary_closure_sha256', v_whole->>'primary_closure_sha256',
    'protected_closure_sha256', v_receipt.protected_closure_sha256,
    'derivative_target_set_sha256', v_derivative_target_set_sha256,
    'derivative_proof_set_sha256',
      v_whole->>'derivative_proof_set_sha256',
    'primary_current', (v_whole->>'primary_current')::boolean,
    'live_guard_current', v_live_guard_current,
    'derivatives_current', case when v_live_drift then false
      else (v_whole->>'derivatives_current')::boolean end,
    'terminal_proof_sha256', case
      when not v_live_drift and v_core->>'status' = 'completed'
      then v_scope.terminal_proof_sha256 else null end,
    'whole_scope_proof', v_whole,
    'whole_scope_proof_sha256',
      v_whole_result->>'whole_scope_proof_sha256',
    'audit_id', case when v_final_audit_id is null
      then null else v_final_audit_id::text end,
    'replay', v_was_completed
  );
  if coalesce((v_result->>'ok')::boolean, false) then
    if not v_permit_consumed then
      v_execution_permit :=
        private.dataset_flow_identity_rotate_wrapper_permit_v1(
          v_invocation_id, 'finalize', false
        );
    end if;
    v_result := v_result || jsonb_build_object(
      'execution_permit', v_execution_permit
    );
  else
    perform private.dataset_flow_identity_invalidate_wrapper_permit_v1(
      v_invocation_id
    );
  end if;
  if v_live_drift then
    return v_result || jsonb_build_object(
      'code', 'FLOW_IDENTITY_PRIMARY_OR_GUARD_DRIFT',
      'compensation_required', false,
      'automatic_retry', false,
      'compensation_targets', '[]'::jsonb
    );
  elsif v_core->>'status' = 'derivatives_pending' then
    return v_result || jsonb_build_object(
      'code', v_core->>'code',
      'compensation_required',
        coalesce((v_core->>'compensation_required')::boolean, false),
      'automatic_retry', false,
      'compensation_targets',
        coalesce(v_core->'compensation_targets', '[]'::jsonb)
    );
  elsif v_core->>'status' = 'failed' then
    return v_result || jsonb_build_object(
      'code', 'FLOW_IDENTITY_FINALIZE_FAILED',
      'compensation_required', false,
      'automatic_retry', false,
      'compensation_targets', '[]'::jsonb
    );
  end if;
  return v_result;
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_FINALIZE_LOCK_BUSY', 'status', 409,
    'message', 'Finalization could not acquire its deterministic fence'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb, jsonb
) owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_finalize_guarded(
  uuid, jsonb, jsonb
) to authenticated;

create or replace function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  p_scope_id uuid,
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
set statement_timeout = '30s'
as $$
declare
  v_command constant text :=
    'cmd_dataset_flow_identity_scope_cancel_guarded';
  v_actor uuid := auth.uid();
  v_request jsonb;
  v_request_sha256 text;
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_receipt util.dataset_flow_identity_capture_receipts%rowtype;
  v_ledger_count integer;
  v_completed_count integer;
  v_nonpending_count integer;
  v_inactive_count integer;
  v_primary_proof_count integer;
  v_process_audit_count integer;
  v_permit_count integer;
  v_audit_id bigint;
  v_audit_count integer;
  v_audit_payload jsonb;
  v_expected_audit_payload jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command, 'code', 'AUTH_REQUIRED',
      'status', 401, 'message', 'Authentication required'
    );
  end if;
  v_request := private.dataset_flow_identity_safe_json_v2(p_request);
  if p_scope_id is null or v_request is null
    or pg_column_size(v_request) > 65536
    or not private.dataset_flow_identity_exact_keys(v_request, array[
      'schema_version', 'request_id', 'receipt_id', 'receipt_proof_sha256',
      'operation_id', 'plan_sha256', 'scope_proof_sha256', 'reason',
      'evidence_sha256'
    ])
    or v_request->>'schema_version'
      <> 'dataset-flow-identity-scope-cancel.v2'
    or v_request->>'request_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or v_request->>'receipt_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or nullif(btrim(v_request->>'operation_id'), '') is null
    or octet_length(v_request->>'operation_id') > 512
    or nullif(btrim(v_request->>'reason'), '') is null
    or octet_length(v_request->>'reason') > 512
    or exists (
      select 1 from unnest(array[
        'receipt_proof_sha256', 'plan_sha256', 'scope_proof_sha256',
        'evidence_sha256'
      ]) as field(name)
      where v_request->>field.name !~ '^[a-f0-9]{64}$'
    ) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_INVALID_REQUEST', 'status', 400,
      'message', 'Step 3 v2 cancel request schema mismatch'
    );
  end if;
  v_request_sha256 :=
    util.dataset_flow_identity_restricted_sha256_v2(v_request);

  -- Match the v2 execution lock order.  The owner fence excludes direct
  -- owner-row writes while the scope lock excludes a concurrent ordinal.
  perform pg_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity-actor:' || v_actor::text, 0
  ));
  if not pg_try_advisory_xact_lock(hashtextextended(
    'dataset-flow-identity:' || p_scope_id::text, 0
  )) then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_BUSY', 'status', 409,
      'message', 'Another transaction owns this scope'
    );
  end if;
  select scope.* into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor
  for update;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;
  select receipt.* into v_receipt
  from util.dataset_flow_identity_capture_receipts as receipt
  where receipt.id = v_scope.receipt_id and receipt.actor_user_id = v_actor;
  if v_receipt.id is null
    or v_receipt.id::text is distinct from v_request->>'receipt_id'
    or v_receipt.receipt_proof_sha256
      is distinct from v_request->>'receipt_proof_sha256'
    or v_scope.receipt_proof_sha256
      is distinct from v_request->>'receipt_proof_sha256'
    or v_scope.operation_id is distinct from v_request->>'operation_id'
    or v_scope.plan_sha256 is distinct from v_request->>'plan_sha256'
    or v_scope.scope_proof_sha256
      is distinct from v_request->>'scope_proof_sha256'
    or v_scope.target_visibility <> 'owner_draft' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_SCOPE_PROOF_MISMATCH', 'status', 409,
      'message', 'Cancel request does not bind the actor scope and receipt'
    );
  end if;

  v_expected_audit_payload := jsonb_build_object(
    'record_type', 'scope_cancel',
    'schema_version', 'dataset-flow-identity-scope-cancel.v2',
    'proof_domain', 'dataset-flow-identity-db-proof.v2',
    'scope_id', v_scope.id,
    'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'cancel_request_sha256', v_request_sha256,
    'reason', btrim(v_request->>'reason'),
    'evidence_sha256', v_request->>'evidence_sha256',
    'completed_process_count', 0,
    'process_count', v_scope.process_count,
    'rewrite_count', v_scope.rewrite_count,
    'hash_algorithm', 'restricted-safe-json-v2-sha256'
  );
  if v_scope.status = 'cancelled' then
    if v_scope.cancel_request_sha256 is distinct from v_request_sha256 then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CANCEL_REPLAY_MISMATCH', 'status', 409,
        'message', 'Cancelled scope is bound to a different request'
      );
    end if;
    select count(*)::integer,
      jsonb_agg(audit.payload order by audit.id)->0,
      min(audit.id)
    into v_audit_count, v_audit_payload, v_audit_id
    from public.command_audit_log as audit
    where audit.command = v_command and audit.actor_user_id = v_actor
      and audit.target_table is null
      and audit.payload->>'scope_id' = v_scope.id::text
      and audit.payload->>'cancel_request_sha256' = v_request_sha256;
    if v_audit_count <> 1
      or v_audit_payload is distinct from v_expected_audit_payload then
      return jsonb_build_object(
        'ok', false, 'command', v_command,
        'code', 'FLOW_IDENTITY_CANCEL_AUDIT_DRIFT', 'status', 409,
        'message', 'The authoritative v2 cancel audit is missing or drifted'
      );
    end if;
    return jsonb_build_object(
      'ok', true, 'command', v_command,
      'schema_version', 'dataset-flow-identity-scope-cancel-result.v2',
      'scope_id', v_scope.id, 'receipt_id', v_receipt.id,
      'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
      'operation_id', v_scope.operation_id, 'plan_sha256', v_scope.plan_sha256,
      'scope_proof_sha256', v_scope.scope_proof_sha256,
      'cancel_request_sha256', v_request_sha256,
      'status', 'cancelled', 'completed_process_count', 0,
      'audit_id', v_audit_id::text, 'replay', true
    );
  end if;
  if v_scope.status <> 'sealed' then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_REQUIRES_ZERO_WRITE_SEAL', 'status', 409,
      'message', 'Only an untouched sealed scope can be cancelled',
      'scope_status', v_scope.status, 'automatic_retry', false
    );
  end if;

  select count(*)::integer,
    count(*) filter (where ledger.status = 'completed')::integer,
    count(*) filter (where ledger.status <> 'pending')::integer,
    count(*) filter (where not ledger.active)::integer,
    count(*) filter (
      where ledger.audit_id is not null
        or ledger.after_payload_sha256 is not null
        or ledger.after_exchange_set_sha256 is not null
        or ledger.derivative_batch_id is not null
        or ledger.process_request_sha256 is not null
        or ledger.completed_at is not null
    )::integer
  into v_ledger_count, v_completed_count, v_nonpending_count,
    v_inactive_count, v_primary_proof_count
  from util.dataset_flow_identity_process_ledger as ledger
  where ledger.scope_id = v_scope.id;
  select count(*)::integer into v_process_audit_count
  from public.command_audit_log as audit
  where audit.command = 'cmd_dataset_flow_identity_process_rewrite_guarded'
    and audit.actor_user_id = v_actor
    and audit.target_table = 'processes'
    and audit.payload->>'scope_id' = v_scope.id::text;
  select count(*)::integer into v_permit_count
  from util.dataset_flow_identity_mutation_permits as permit
  where permit.scope_id = v_scope.id;
  if v_ledger_count <> v_scope.process_count
    or v_completed_count <> 0 or v_nonpending_count <> 0
    or v_inactive_count <> 0 or v_primary_proof_count <> 0
    or v_process_audit_count <> 0 or v_permit_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'command', v_command,
      'code', 'FLOW_IDENTITY_CANCEL_PRIMARY_WRITE_PROOF_EXISTS', 'status', 409,
      'message', 'Scope ledger or audit proves a primary write attempt',
      'completed_process_count', v_completed_count,
      'automatic_retry', false
    );
  end if;

  insert into public.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    v_command, v_actor, null, null, null, v_expected_audit_payload
  ) returning id into v_audit_id;
  update util.dataset_flow_identity_process_ledger
  set active = false where scope_id = v_scope.id;
  update util.dataset_flow_identity_scopes
  set status = 'cancelled', cancel_request_sha256 = v_request_sha256,
    last_error = jsonb_build_object(
      'code', 'FLOW_IDENTITY_SCOPE_CANCELLED',
      'schema_version', 'dataset-flow-identity-scope-cancel.v2',
      'cancel_request_sha256', v_request_sha256,
      'reason', btrim(v_request->>'reason'),
      'evidence_sha256', v_request->>'evidence_sha256'
    ), updated_at = clock_timestamp()
  where id = v_scope.id;
  return jsonb_build_object(
    'ok', true, 'command', v_command,
    'schema_version', 'dataset-flow-identity-scope-cancel-result.v2',
    'scope_id', v_scope.id, 'receipt_id', v_receipt.id,
    'receipt_proof_sha256', v_receipt.receipt_proof_sha256,
    'operation_id', v_scope.operation_id, 'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'cancel_request_sha256', v_request_sha256,
    'status', 'cancelled', 'completed_process_count', 0,
    'audit_id', v_audit_id::text, 'replay', false
  );
exception when lock_not_available then
  return jsonb_build_object(
    'ok', false, 'command', v_command,
    'code', 'FLOW_IDENTITY_CANCEL_LOCK_BUSY', 'status', 409,
    'message', 'Cancellation could not acquire its deterministic fence'
  );
end;
$$;

alter function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) owner to postgres;
revoke all on function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) to authenticated;

comment on function public.cmd_dataset_flow_identity_scope_cancel_guarded(
  uuid, jsonb
) is
  'Actor-only v2 Step 3 abandon path. Exact replay is read-only; a fresh cancel releases active fences only for an untouched sealed scope whose ledger, audit, derivative, and mutation-permit proof all show zero primary writes.';
