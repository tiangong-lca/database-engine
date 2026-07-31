begin;

create extension if not exists pgtap with schema extensions;
create extension if not exists dblink with schema extensions;

select extensions.plan(8);

select extensions.ok(
  (
    select trigger.tgtype = 19
      and trigger.tgqual is not null
      and pg_get_triggerdef(trigger.oid, true) like '%extracted_md%'
      and pg_get_triggerdef(trigger.oid, true) like '%embedding_ft%'
      and pg_get_triggerdef(trigger.oid, true) like '%embedding_ft_at%'
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.flows'::regclass
      and trigger.tgname = 'dataset_flow_identity_flow_active_fence'
      and not trigger.tgisinternal
  ),
  'Flow UPDATE actor fence excludes only the three guard-neutral derivative columns'
);

select extensions.ok(
  (
    select trigger.tgtype = 15
      and trigger.tgqual is null
    from pg_trigger as trigger
    where trigger.tgrelid = 'public.flows'::regclass
      and trigger.tgname =
        'dataset_flow_identity_flow_insert_delete_active_fence'
      and not trigger.tgisinternal
  ),
  'Flow INSERT and DELETE remain unconditionally actor-fenced'
);

alter table public.flows
  disable trigger dataset_flow_identity_flow_insert_delete_active_fence;

insert into public.flows (
  id,
  version,
  user_id,
  state_code,
  json,
  json_ordered
) values (
  'fa324000-0000-4000-8000-000000000001'::uuid,
  '01.00.000',
  'fa324000-0000-4000-8000-000000000002'::uuid,
  100,
  '{}'::jsonb,
  '{}'::json
);

alter table public.flows
  enable trigger dataset_flow_identity_flow_insert_delete_active_fence;

select extensions.dblink_connect(
  'flow_derivative_actor_lock',
  'host=db port=5432 dbname=' || current_database()
    || ' user=postgres password=postgres'
);
select extensions.dblink_exec('flow_derivative_actor_lock', 'begin');
select extensions.dblink_exec(
  'flow_derivative_actor_lock',
  $dblink$
    do $lock$
    begin
      perform pg_advisory_xact_lock(hashtextextended(
        'dataset-flow-identity-actor:fa324000-0000-4000-8000-000000000002',
        0
      ));
    end
    $lock$
  $dblink$
);

select extensions.lives_ok(
  $test$
    update public.flows
    set extracted_md = '# derivative markdown'
    where id = 'fa324000-0000-4000-8000-000000000001'::uuid
  $test$,
  'extracted_md-only Flow update bypasses a busy owner actor fence'
);

select extensions.lives_ok(
  $test$
    update public.flows
    set embedding_ft =
          array_fill(0::real, array[1024])::extensions.vector(1024),
        embedding_ft_at = clock_timestamp()
    where id = 'fa324000-0000-4000-8000-000000000001'::uuid
  $test$,
  'embedding value/timestamp Flow update bypasses a busy owner actor fence'
);

select extensions.throws_ok(
  $test$
    update public.flows
    set json_ordered = '{"changed":true}'::json
    where id = 'fa324000-0000-4000-8000-000000000001'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'Flow payload mutation remains fail-closed behind the actor fence'
);

select extensions.throws_ok(
  $test$
    update public.flows
    set state_code = 0
    where id = 'fa324000-0000-4000-8000-000000000001'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'Flow state mutation remains fail-closed behind the actor fence'
);

select extensions.throws_ok(
  $test$
    update public.flows
    set user_id = 'fa324000-0000-4000-8000-000000000003'::uuid
    where id = 'fa324000-0000-4000-8000-000000000001'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'Flow ownership mutation remains fail-closed behind the actor fence'
);

select extensions.throws_ok(
  $test$
    update public.flows
    set modified_at = modified_at + interval '1 second'
    where id = 'fa324000-0000-4000-8000-000000000001'::uuid
  $test$,
  '55P03',
  'FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY',
  'Flow guard timestamp mutation remains fail-closed behind the actor fence'
);

select extensions.dblink_exec('flow_derivative_actor_lock', 'rollback');
select extensions.dblink_disconnect('flow_derivative_actor_lock');

select * from extensions.finish();

rollback;
