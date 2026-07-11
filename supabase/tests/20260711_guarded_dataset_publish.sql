begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.unitgroup_payload(p_name text)
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'unitGroupDataSet', jsonb_build_object(
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      ),
      'unitGroupInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:name', jsonb_build_array(
            jsonb_build_object('@xml:lang', 'en', '#text', p_name)
          )
        )
      )
    )
  );
$$;

create or replace function pg_temp.flowproperty_payload(p_name text)
returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'flowPropertyDataSet', jsonb_build_object(
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      ),
      'flowPropertiesInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:name', jsonb_build_array(
            jsonb_build_object('@xml:lang', 'en', '#text', p_name)
          )
        )
      )
    )
  );
$$;

select plan(37);

select ok(
  has_function_privilege(
    'authenticated',
    'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  )
  and has_function_privilege(
    'service_role',
    'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  ),
  'guarded publish is executable only by authenticated and service roles'
);

select ok(
  to_regprocedure('public.cmd_dataset_publish(text,uuid,text,jsonb)') is not null
  and to_regprocedure(
    'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)'
  ) is not null,
  'guarded publish is a new RPC and keeps the legacy publish signature intact'
);

select ok(
  strpos(
    lower(
      pg_get_functiondef(
        'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)'::regprocedure
      )
    ),
    'for update of t'
  ) > 0,
  'guarded publish locks the exact dataset row before checking preconditions'
);

select ok(
  strpos(
    lower(
      pg_get_functiondef(
        'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)'::regprocedure
      )
    ),
    'and t.user_id = $3'
  ) > 0
  and strpos(
    lower(
      pg_get_functiondef(
        'public.cmd_dataset_publish_guarded(text,uuid,text,timestamptz,jsonb,jsonb)'::regprocedure
      )
    ),
    'using p_id, p_version, v_actor'
  ) > 0,
  'guarded publish applies the current actor inside the security-definer row-lock predicate'
);

select ok(
  to_regclass('public.command_audit_log_guarded_publish_replay_idx') is not null
  and (
    select index_meta.indisvalid
      and index_meta.indisready
      and strpos(
        pg_get_expr(index_meta.indpred, index_meta.indrelid),
        'cmd_dataset_publish_guarded'
      ) > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'actor_user_id') > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'target_table') > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'target_id') > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'target_version') > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'plan_sha256') > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'operation_id') > 0
      and strpos(pg_get_indexdef(index_meta.indexrelid), 'action_id') > 0
    from pg_catalog.pg_index as index_meta
    where index_meta.indexrelid =
      'public.command_audit_log_guarded_publish_replay_idx'::regclass
  ),
  'guarded replay has a valid partial expression index for its narrowed audit correlation lookup'
);

select set_config('request.jwt.claim.role', 'authenticated', true);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    'b1000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'guarded-publish-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b1000000-0000-0000-0000-000000000001"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b1000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'guarded-publish-other@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b1000000-0000-0000-0000-000000000002"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.unitgroups (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values
  (
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    pg_temp.unitgroup_payload('publish success'),
    'b1000000-0000-0000-0000-000000000001',
    0,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000002',
    '01.00.000',
    pg_temp.unitgroup_payload('stale timestamp'),
    'b1000000-0000-0000-0000-000000000001',
    0,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000003',
    '01.00.000',
    pg_temp.unitgroup_payload('stale payload'),
    'b1000000-0000-0000-0000-000000000001',
    0,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000004',
    '01.00.000',
    pg_temp.unitgroup_payload('other owner'),
    'b1000000-0000-0000-0000-000000000002',
    0,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000005',
    '01.00.000',
    pg_temp.unitgroup_payload('under review'),
    'b1000000-0000-0000-0000-000000000001',
    20,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000006',
    '01.00.000',
    pg_temp.unitgroup_payload('independently published'),
    'b1000000-0000-0000-0000-000000000001',
    100,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000007',
    '01.00.000',
    pg_temp.unitgroup_payload('audit state conflict'),
    'b1000000-0000-0000-0000-000000000001',
    0,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000008',
    '01.00.000',
    pg_temp.unitgroup_payload('replay-bound payload'),
    'b1000000-0000-0000-0000-000000000001',
    0,
    true,
    '2026-07-11 00:00:00+00'
  ),
  (
    'b2000000-0000-0000-0000-000000000009',
    '01.00.000',
    pg_temp.unitgroup_payload('forced audit failure'),
    'b1000000-0000-0000-0000-000000000001',
    0,
    true,
    '2026-07-11 00:00:00+00'
  );

insert into public.flowproperties (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  'b3000000-0000-0000-0000-000000000001',
  '01.00.000',
  pg_temp.flowproperty_payload('flow property success'),
  'b1000000-0000-0000-0000-000000000001',
  0,
  true,
  '2026-07-11 00:00:00+00'
);

insert into public.command_audit_log (
  command,
  actor_user_id,
  target_table,
  target_id,
  target_version,
  payload
)
values (
  'cmd_dataset_publish_guarded',
  'b1000000-0000-0000-0000-000000000001',
  'unitgroups',
  'b2000000-0000-0000-0000-000000000007',
  '01.00.000',
  jsonb_build_object(
    'plan_sha256', repeat('c', 64),
    'operation_id', 'maintenance-audit-conflict',
    'action_id', 'publish-audit-conflict',
    'expected_modified_at', '2026-07-11 00:00:00+00'::timestamptz,
    'expected_json_ordered', pg_temp.unitgroup_payload('audit state conflict')
  )
);

alter table public.command_audit_log
  add constraint command_audit_log_test_force_guarded_publish_failure
  check (
    (payload ->> 'operation_id') is distinct from 'maintenance-force-audit-failure'
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '', true);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'AUTH_REQUIRED',
  'guarded publish requires an authenticated actor identity'
);

select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000001', true);

select is(
  public.cmd_dataset_publish_guarded(
    'flows',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    '{}'::jsonb,
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'INVALID_DATASET_TABLE',
  'guarded publish rejects tables outside unitgroups and flowproperties'
);

select is(
  public.cmd_dataset_publish_guarded(
    null::text,
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-null-table',
      'action_id', 'publish-null-table'
    )
  )->>'code',
  'INVALID_DATASET_TABLE',
  'guarded publish rejects a null table before constructing dynamic SQL'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    '{}'::jsonb
  )->>'code',
  'DATASET_PUBLISH_AUDIT_CORRELATION_REQUIRED',
  'guarded publish requires plan and action audit correlation'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    null,
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'DATASET_PUBLISH_EXPECTED_MODIFIED_AT_REQUIRED',
  'guarded publish requires the planned modified_at value'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    '[]'::jsonb,
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'DATASET_PUBLISH_EXPECTED_JSON_ORDERED_INVALID',
  'guarded publish requires an object-shaped expected payload'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    null,
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'DATASET_PUBLISH_EXPECTED_JSON_ORDERED_REQUIRED',
  'guarded publish requires the planned json_ordered payload'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000099',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('missing'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-missing',
      'action_id', 'publish-missing'
    )
  )->>'code',
  'DATASET_NOT_FOUND',
  'guarded publish rejects an unknown exact id and version'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000002',
    '01.00.000',
    '2026-07-11 00:00:01+00',
    pg_temp.unitgroup_payload('stale timestamp'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-stale-time',
      'action_id', 'publish-stale-time'
    )
  )->>'code',
  'DATASET_PUBLISH_PRECONDITION_FAILED',
  'guarded publish rejects a stale modified_at precondition'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000003',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('different payload'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-stale-payload',
      'action_id', 'publish-stale-payload'
    )
  )->>'code',
  'DATASET_PUBLISH_PRECONDITION_FAILED',
  'guarded publish rejects a stale json_ordered precondition'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000004',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('other owner'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-owner-check',
      'action_id', 'publish-other-owner'
    )
  )->>'code',
  'DATASET_NOT_FOUND',
  'guarded publish does not disclose an exact row owned by another actor'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000005',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('under review'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-state-check',
      'action_id', 'publish-under-review'
    )
  )->>'code',
  'DATASET_PUBLISH_REQUIRES_DRAFT',
  'guarded publish accepts only state_code=0 for a first commit'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000007',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('audit state conflict'),
    jsonb_build_object(
      'plan_sha256', repeat('c', 64),
      'operation_id', 'maintenance-audit-conflict',
      'action_id', 'publish-audit-conflict'
    )
  )->>'code',
  'DATASET_PUBLISH_AUDIT_STATE_CONFLICT',
  'guarded publish rejects a draft row with an impossible committed audit record'
);

select throws_ok(
  $$
    select public.cmd_dataset_publish_guarded(
      'unitgroups',
      'b2000000-0000-0000-0000-000000000009',
      '01.00.000',
      '2026-07-11 00:00:00+00',
      pg_temp.unitgroup_payload('forced audit failure'),
      jsonb_build_object(
        'plan_sha256', repeat('f', 64),
        'operation_id', 'maintenance-force-audit-failure',
        'action_id', 'publish-force-audit-failure'
      )
    )
  $$,
  '23514',
  null,
  'guarded publish surfaces a forced audit-insert failure'
);

reset role;

select ok(
  (
    select state_code = 0
    from public.unitgroups
    where id = 'b2000000-0000-0000-0000-000000000009'
      and version = '01.00.000'
  )
  and not exists (
    select 1
    from public.command_audit_log
    where command = 'cmd_dataset_publish_guarded'
      and target_id = 'b2000000-0000-0000-0000-000000000009'
      and target_version = '01.00.000'
  ),
  'forced audit-insert failure rolls back the state update and leaves no audit row'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000001', true);

select ok(
  (
    select result @> '{"ok":true,"idempotent_replay":false}'::jsonb
    from (
      select public.cmd_dataset_publish_guarded(
        'unitgroups',
        'b2000000-0000-0000-0000-000000000001',
        '01.00.000',
        '2026-07-11 00:00:00+00',
        pg_temp.unitgroup_payload('publish success'),
        jsonb_build_object(
          'plan_sha256', repeat('a', 64),
          'operation_id', 'maintenance-success',
          'action_id', 'publish-unitgroup',
          'reason_code', 'FPUG-001'
        )
      ) as result
    ) as committed
  ),
  'owner can publish an exact draft unit group through guarded publish'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'reason_code', 'FPUG-001'
    )
  )->>'idempotent_replay',
  'true',
  'same guarded action replays successfully after the first commit'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:01+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'DATASET_PUBLISH_REPLAY_UNPROVEN',
  'same correlation cannot prove replay with a different expected timestamp'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'different-action'
    )
  )->>'code',
  'DATASET_PUBLISH_REPLAY_UNPROVEN',
  'published row cannot be claimed by a different action correlation'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('wrong replay payload'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup'
    )
  )->>'code',
  'DATASET_PUBLISH_REPLAY_PAYLOAD_MISMATCH',
  'matching audit cannot replay against a different expected payload'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000006',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('independently published'),
    jsonb_build_object(
      'plan_sha256', repeat('b', 64),
      'operation_id', 'maintenance-independent',
      'action_id', 'publish-independent'
    )
  )->>'code',
  'DATASET_PUBLISH_REPLAY_UNPROVEN',
  'independently published row is rejected without matching guarded audit proof'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000008',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('replay-bound payload'),
    jsonb_build_object(
      'plan_sha256', repeat('e', 64),
      'operation_id', 'maintenance-replay-bound',
      'action_id', 'publish-replay-bound'
    )
  )->>'ok',
  'true',
  'guarded publish commits the fixture used to verify replay precondition binding'
);

reset role;

update public.unitgroups
set json_ordered = pg_temp.unitgroup_payload('privileged payload change')
where id = 'b2000000-0000-0000-0000-000000000008'
  and version = '01.00.000';

set local role authenticated;
select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000001', true);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000008',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('privileged payload change'),
    jsonb_build_object(
      'plan_sha256', repeat('e', 64),
      'operation_id', 'maintenance-replay-bound',
      'action_id', 'publish-replay-bound'
    )
  )->>'code',
  'DATASET_PUBLISH_REPLAY_UNPROVEN',
  'same correlation cannot prove replay after a privileged payload change'
);

select is(
  public.cmd_dataset_publish_guarded(
    'flowproperties',
    'b3000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.flowproperty_payload('flow property success'),
    jsonb_build_object(
      'plan_sha256', repeat('d', 64),
      'operation_id', 'maintenance-flowproperty',
      'action_id', 'publish-flowproperty',
      'reason_code', 'FPUG-001'
    )
  )->>'ok',
  'true',
  'owner can publish an exact draft flow property through guarded publish'
);

reset role;

select is(
  (
    select state_code::text
    from public.unitgroups
    where id = 'b2000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '100',
  'guarded unit group publish commits state_code=100'
);

select is(
  (
    select json_ordered::jsonb
    from public.unitgroups
    where id = 'b2000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  pg_temp.unitgroup_payload('publish success'),
  'guarded publish preserves json_ordered exactly at the semantic JSON level'
);

select ok(
  (
    select modified_at > '2026-07-11 00:00:00+00'::timestamptz
    from public.unitgroups
    where id = 'b2000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  'guarded publish advances modified_at'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_publish_guarded'
      and actor_user_id = 'b1000000-0000-0000-0000-000000000001'
      and target_table = 'unitgroups'
      and target_id = 'b2000000-0000-0000-0000-000000000001'
      and target_version = '01.00.000'
      and payload->>'plan_sha256' = repeat('a', 64)
      and payload->>'operation_id' = 'maintenance-success'
      and payload->>'action_id' = 'publish-unitgroup'
      and payload->>'reason_code' = 'FPUG-001'
      and payload->'expected_modified_at' = to_jsonb('2026-07-11 00:00:00+00'::timestamptz)
      and payload->'expected_json_ordered' = pg_temp.unitgroup_payload('publish success')
  ),
  '1',
  'first commit writes one exact plan/action audit record and replay does not duplicate it'
);

select is(
  (
    select state_code::text
    from public.flowproperties
    where id = 'b3000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '100',
  'guarded flow property publish commits state_code=100'
);

select is(
  (
    select string_agg(id::text || ':' || state_code::text, ',' order by id)
    from public.unitgroups
    where id in (
      'b2000000-0000-0000-0000-000000000002',
      'b2000000-0000-0000-0000-000000000003',
      'b2000000-0000-0000-0000-000000000004',
      'b2000000-0000-0000-0000-000000000005',
      'b2000000-0000-0000-0000-000000000007'
    )
  ),
  'b2000000-0000-0000-0000-000000000002:0,'
    || 'b2000000-0000-0000-0000-000000000003:0,'
    || 'b2000000-0000-0000-0000-000000000004:0,'
    || 'b2000000-0000-0000-0000-000000000005:20,'
    || 'b2000000-0000-0000-0000-000000000007:0',
  'all rejected guarded publish attempts leave dataset states unchanged'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_publish_guarded'
      and target_id in (
        'b2000000-0000-0000-0000-000000000002',
        'b2000000-0000-0000-0000-000000000003',
        'b2000000-0000-0000-0000-000000000004',
        'b2000000-0000-0000-0000-000000000005'
      )
  ),
  '0',
  'rejected guarded publish attempts do not write success audit records'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_publish_guarded'
      and target_id = 'b3000000-0000-0000-0000-000000000001'
      and payload->>'plan_sha256' = repeat('d', 64)
      and payload->>'operation_id' = 'maintenance-flowproperty'
      and payload->>'action_id' = 'publish-flowproperty'
  ),
  '1',
  'guarded flow property publish writes its own exact audit correlation'
);

select * from finish();
rollback;
