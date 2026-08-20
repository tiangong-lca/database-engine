begin;

create extension if not exists pgtap with schema extensions;
create extension if not exists dblink with schema extensions;

select extensions.plan(17);

select extensions.ok(
  to_regprocedure('private.dataset_flow_identity_active_fence()') is not null
  and to_regprocedure('private.dataset_flow_identity_active_fence_v2()') is null,
  'active fence uses one canonical internal name without a versioned alias'
);

select extensions.ok(
  (
    select trigger.tgtype = 19
      and trigger.tgqual is not null
      and pg_get_triggerdef(trigger.oid, true) like '%extracted_md%'
      and pg_get_triggerdef(trigger.oid, true) like '%embedding_ft%'
      and pg_get_triggerdef(trigger.oid, true) like '%embedding_ft_at%'
      and pg_get_triggerdef(trigger.oid, true) like '%search_text%'
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.flowproperties'::regclass
      and trigger.tgname =
        'dataset_flow_identity_flowproperty_active_fence'
      and not trigger.tgisinternal
  ),
  'FlowProperty UPDATE fence excludes only the four derivative columns'
);

select extensions.ok(
  (
    select trigger.tgtype = 11
      and trigger.tgqual is null
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.flowproperties'::regclass
      and trigger.tgname =
        'dataset_flow_identity_flowproperty_delete_active_fence'
      and not trigger.tgisinternal
  ),
  'FlowProperty DELETE remains unconditionally actor-fenced'
);

select extensions.ok(
  (
    select trigger.tgtype = 19
      and trigger.tgqual is not null
      and pg_get_triggerdef(trigger.oid, true) like '%extracted_md%'
      and pg_get_triggerdef(trigger.oid, true) like '%embedding_ft%'
      and pg_get_triggerdef(trigger.oid, true) like '%embedding_ft_at%'
      and pg_get_triggerdef(trigger.oid, true) like '%search_text%'
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.unitgroups'::regclass
      and trigger.tgname =
        'dataset_flow_identity_unitgroup_active_fence'
      and not trigger.tgisinternal
  ),
  'UnitGroup UPDATE fence excludes only the four derivative columns'
);

select extensions.ok(
  (
    select trigger.tgtype = 11
      and trigger.tgqual is null
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.unitgroups'::regclass
      and trigger.tgname =
        'dataset_flow_identity_unitgroup_delete_active_fence'
      and not trigger.tgisinternal
  ),
  'UnitGroup DELETE remains unconditionally actor-fenced'
);

set local session_replication_role = replica;

insert into util.dataset_flow_identity_scopes (
  id,
  receipt_id,
  receipt_proof_sha256,
  actor_user_id,
  actor_email,
  request_id,
  environment,
  project_ref,
  target_visibility,
  operation_id,
  plan_sha256,
  freeze_sha256,
  approval_identity_sha256,
  approval_text_sha256,
  policy_approval_text_sha256,
  execution_approval_request_sha256,
  toolchain_evidence_sha256,
  compatibility_policy,
  support_snapshot_set_sha256,
  support_snapshots,
  source_universe_sha256,
  source_universe,
  source_universe_count,
  mapping_set_sha256,
  process_manifest_sha256,
  protected_closure_sha256,
  protected_closure,
  preflight_request_sha256,
  scope_request_sha256,
  scope_proof_sha256,
  status,
  mapping_count,
  process_count,
  rewrite_count
) values (
  'fa327000-0000-4000-8000-000000000004'::uuid,
  'fa327000-0000-4000-8000-000000000001'::uuid,
  repeat('1', 64),
  'fa327000-0000-4000-8000-000000000002'::uuid,
  'issue-327@example.invalid',
  'fa327000-0000-4000-8000-000000000003'::uuid,
  'local',
  'local',
  'owner_draft',
  'issue-327-support-derivative-fence',
  repeat('2', 64),
  repeat('3', 64),
  repeat('4', 64),
  repeat('5', 64),
  repeat('6', 64),
  repeat('7', 64),
  repeat('8', 64),
  '{}'::jsonb,
  repeat('9', 64),
  '[]'::jsonb,
  repeat('a', 64),
  '[]'::jsonb,
  305,
  repeat('b', 64),
  repeat('c', 64),
  repeat('d', 64),
  '[]'::jsonb,
  repeat('e', 64),
  repeat('f', 64),
  repeat('0', 64),
  'sealed',
  1,
  1,
  1
);

insert into util.dataset_flow_identity_capture_support_guards (
  receipt_id,
  ordinal,
  support_table,
  support_id,
  support_version,
  guard
) values
(
  'fa327000-0000-4000-8000-000000000001'::uuid,
  1,
  'flowproperties',
  'fa327000-0000-4000-8000-000000000005'::uuid,
  '01.00.000',
  '{}'::jsonb
),
(
  'fa327000-0000-4000-8000-000000000001'::uuid,
  2,
  'unitgroups',
  'fa327000-0000-4000-8000-000000000006'::uuid,
  '01.00.000',
  '{}'::jsonb
);

insert into public.flowproperties (
  id,
  version,
  user_id,
  state_code,
  json,
  json_ordered
) values (
  'fa327000-0000-4000-8000-000000000005'::uuid,
  '01.00.000',
  'fa327000-0000-4000-8000-000000000002'::uuid,
  100,
  '{}'::jsonb,
  '{}'::json
);

insert into public.unitgroups (
  id,
  version,
  user_id,
  state_code,
  json,
  json_ordered
) values (
  'fa327000-0000-4000-8000-000000000006'::uuid,
  '01.00.000',
  'fa327000-0000-4000-8000-000000000002'::uuid,
  100,
  '{}'::jsonb,
  '{}'::json
);

set local session_replication_role = origin;

select extensions.dblink_connect(
  'support_derivative_actor_lock',
  'host=db port=5432 dbname=' || current_database()
    || ' user=postgres password=postgres'
);
select extensions.dblink_exec('support_derivative_actor_lock', 'begin');
select extensions.dblink_exec(
  'support_derivative_actor_lock',
  $dblink$
    do $lock$
    begin
      perform pg_advisory_xact_lock(hashtextextended(
        'dataset-flow-identity-actor:fa327000-0000-4000-8000-000000000002',
        0
      ));
    end
    $lock$
  $dblink$
);

select extensions.lives_ok(
  $test$
    update public.flowproperties
    set extracted_md = '# derivative markdown'
    where id = 'fa327000-0000-4000-8000-000000000005'::uuid
  $test$,
  'extracted_md-only FlowProperty update bypasses a busy actor fence'
);

select extensions.lives_ok(
  $test$
    update public.flowproperties
    set embedding_ft = array_fill(0::real, array[1024])::extensions.vector(1024)
    where id = 'fa327000-0000-4000-8000-000000000005'::uuid
  $test$,
  'embedding_ft-only FlowProperty update bypasses a busy actor fence'
);

select extensions.lives_ok(
  $test$
    update public.flowproperties
    set embedding_ft_at = clock_timestamp()
    where id = 'fa327000-0000-4000-8000-000000000005'::uuid
  $test$,
  'embedding_ft_at-only FlowProperty update bypasses a busy actor fence'
);

select extensions.lives_ok(
  $test$
    update public.flowproperties
    set search_text = array['support lexical projection']::text[]
    where id = 'fa327000-0000-4000-8000-000000000005'::uuid
  $test$,
  'search_text-only FlowProperty update bypasses a busy actor fence'
);

select extensions.throws_ok(
  $test$
    update public.flowproperties
    set modified_at = modified_at + interval '1 second'
    where id = 'fa327000-0000-4000-8000-000000000005'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'FlowProperty guard timestamp mutation remains actor-fenced'
);

select extensions.throws_ok(
  $test$
    delete from public.flowproperties
    where id = 'fa327000-0000-4000-8000-000000000005'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'FlowProperty delete remains actor-fenced'
);

select extensions.lives_ok(
  $test$
    update public.unitgroups
    set extracted_md = '# derivative markdown'
    where id = 'fa327000-0000-4000-8000-000000000006'::uuid
  $test$,
  'extracted_md-only UnitGroup update bypasses a busy actor fence'
);

select extensions.lives_ok(
  $test$
    update public.unitgroups
    set embedding_ft = array_fill(0::real, array[1024])::extensions.vector(1024)
    where id = 'fa327000-0000-4000-8000-000000000006'::uuid
  $test$,
  'embedding_ft-only UnitGroup update bypasses a busy actor fence'
);

select extensions.lives_ok(
  $test$
    update public.unitgroups
    set embedding_ft_at = clock_timestamp()
    where id = 'fa327000-0000-4000-8000-000000000006'::uuid
  $test$,
  'embedding_ft_at-only UnitGroup update bypasses a busy actor fence'
);

select extensions.lives_ok(
  $test$
    update public.unitgroups
    set search_text = array['support lexical projection']::text[]
    where id = 'fa327000-0000-4000-8000-000000000006'::uuid
  $test$,
  'search_text-only UnitGroup update bypasses a busy actor fence'
);

select extensions.throws_ok(
  $test$
    update public.unitgroups
    set modified_at = modified_at + interval '1 second'
    where id = 'fa327000-0000-4000-8000-000000000006'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'UnitGroup guard timestamp mutation remains actor-fenced'
);

select extensions.throws_ok(
  $test$
    delete from public.unitgroups
    where id = 'fa327000-0000-4000-8000-000000000006'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'UnitGroup delete remains actor-fenced'
);

select extensions.dblink_exec('support_derivative_actor_lock', 'rollback');
select extensions.dblink_disconnect('support_derivative_actor_lock');

select * from extensions.finish();

rollback;
