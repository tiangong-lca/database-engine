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

select plan(63);

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
  has_function_privilege(
    'authenticated',
    'public.cmd_dataset_support_approve_guarded(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  )
  and has_function_privilege(
    'service_role',
    'public.cmd_dataset_support_approve_guarded(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.cmd_dataset_support_approve_guarded(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  ),
  'support approval is executable only by authenticated and service roles'
);

select ok(
  has_function_privilege(
    'authenticated',
    'public.qry_dataset_publish_guarded_proof(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.qry_dataset_publish_guarded_proof(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'public.qry_dataset_publish_guarded_proof(text,uuid,text,timestamptz,jsonb,jsonb)',
    'execute'
  ),
  'guarded publication proof is an authenticated owner-only query surface'
);

select ok(
  to_regprocedure('public.cmd_dataset_publish(text,uuid,text,jsonb)') is not null
  and to_regprocedure(
    'public.cmd_dataset_support_approve_guarded(text,uuid,text,timestamptz,jsonb,jsonb)'
  ) is not null
  and to_regprocedure(
    'public.qry_dataset_publish_guarded_proof(text,uuid,text,timestamptz,jsonb,jsonb)'
  ) is not null
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

select ok(
  to_regclass('public.command_audit_log_support_approval_replay_idx') is not null
  and (
    select index_meta.indisvalid
      and index_meta.indisready
      and index_meta.indisunique
      and strpos(
        pg_get_expr(index_meta.indpred, index_meta.indrelid),
        'cmd_dataset_support_approve_guarded'
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
      'public.command_audit_log_support_approval_replay_idx'::regclass
  ),
  'support approval replay has a valid narrowed partial expression index'
);

select ok(
  not has_table_privilege('authenticated', 'public.command_audit_log', 'insert')
  and not has_table_privilege('authenticated', 'public.command_audit_log', 'update')
  and not has_table_privilege('authenticated', 'public.command_audit_log', 'delete'),
  'dataset owners cannot forge or alter approval audit rows through raw table writes'
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
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b1000000-0000-0000-0000-000000000003',
    'authenticated',
    'authenticated',
    'guarded-publish-reviewer@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b1000000-0000-0000-0000-000000000003","email":"guarded-publish-reviewer@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  (
    'b1000000-0000-0000-0000-000000000001',
    '{"email":"guarded-publish-owner@example.com"}'::jsonb,
    null
  ),
  (
    'b1000000-0000-0000-0000-000000000002',
    '{"email":"guarded-publish-other@example.com"}'::jsonb,
    null
  ),
  (
    'b1000000-0000-0000-0000-000000000003',
    '{"email":"guarded-publish-reviewer@example.com"}'::jsonb,
    null
  )
on conflict (id) do nothing;

insert into public.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System Team"}'::jsonb,
  0,
  false
)
on conflict (id) do nothing;

insert into public.roles (user_id, team_id, role)
values
  (
    'b1000000-0000-0000-0000-000000000001',
    '00000000-0000-0000-0000-000000000000',
    'review-admin'
  ),
  (
    'b1000000-0000-0000-0000-000000000003',
    '00000000-0000-0000-0000-000000000000',
    'review-admin'
  );

create temporary table approval_results (
  action_key text primary key,
  response jsonb not null
);

grant select, insert, update, delete on table pg_temp.approval_results to authenticated;

create or replace function pg_temp.approval_id(p_action_key text)
returns text
language sql
stable
as $$
  select response->'data'->>'approval_audit_id'
  from pg_temp.approval_results
  where action_key = p_action_key
$$;

grant execute on function pg_temp.approval_id(text) to authenticated;

create or replace function pg_temp.approval_reviewer_user_id()
returns text
language sql
immutable
as $$
  select 'b1000000-0000-0000-0000-000000000003'::text
$$;

create or replace function pg_temp.approval_reviewer_email()
returns text
language sql
immutable
as $$
  select 'guarded-publish-reviewer@example.com'::text
$$;

grant execute on function pg_temp.approval_reviewer_user_id() to authenticated;
grant execute on function pg_temp.approval_reviewer_email() to authenticated;

create or replace function pg_temp.publish_audit_id(p_action_id text)
returns text
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select audit_log.id::text
  from public.command_audit_log as audit_log
  where audit_log.command = 'cmd_dataset_publish_guarded'
    and audit_log.payload->>'action_id' = p_action_id
  order by audit_log.id desc
  limit 1
$$;

grant execute on function pg_temp.publish_audit_id(text) to authenticated;

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
  ),
  (
    'b2000000-0000-0000-0000-000000000001',
    '01.00.001',
    jsonb_set(
      pg_temp.unitgroup_payload('wrong version fixture'),
      '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}',
      '"01.00.001"'::jsonb
    ),
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

alter table public.command_audit_log
  add constraint command_audit_log_test_force_guarded_publish_failure
  check (
    command is distinct from 'cmd_dataset_publish_guarded'
    or (payload ->> 'operation_id') is distinct from 'maintenance-force-audit-failure'
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

select is(
  public.cmd_dataset_support_approve_guarded(
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
  'support approval requires an authenticated reviewer identity'
);

select is(
  public.qry_dataset_publish_guarded_proof(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', '1',
      'publish_audit_id', '2'
    )
  )->>'code',
  'AUTH_REQUIRED',
  'guarded publication proof requires the authenticated dataset owner'
);

select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000002', true);

select is(
  public.cmd_dataset_support_approve_guarded(
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
  'REVIEW_ADMIN_REQUIRED',
  'an ordinary authenticated user cannot approve support publication'
);

select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000001', true);

select is(
  public.cmd_dataset_support_approve_guarded(
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
  'INDEPENDENT_REVIEWER_REQUIRED',
  'a review admin cannot approve a support dataset they own'
);

select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000003', true);

select is(
  public.cmd_dataset_support_approve_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000002',
    '01.00.000',
    '2026-07-11 00:00:01+00',
    pg_temp.unitgroup_payload('stale timestamp'),
    jsonb_build_object(
      'plan_sha256', repeat('2', 64),
      'operation_id', 'approval-stale-time',
      'action_id', 'approve-stale-time'
    )
  )->>'code',
  'DATASET_SUPPORT_APPROVAL_PRECONDITION_FAILED',
  'reviewer approval is bound to the exact planned modified_at snapshot'
);

select is(
  public.cmd_dataset_support_approve_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000005',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('under review'),
    jsonb_build_object(
      'plan_sha256', repeat('5', 64),
      'operation_id', 'approval-non-draft',
      'action_id', 'approve-non-draft'
    )
  )->>'code',
  'DATASET_SUPPORT_APPROVAL_REQUIRES_DRAFT',
  'reviewer approval applies only to an exact state_code=0 support dataset'
);

insert into pg_temp.approval_results (action_key, response)
values
  (
    'success',
    public.cmd_dataset_support_approve_guarded(
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
    )
  ),
  (
    'audit-conflict',
    public.cmd_dataset_support_approve_guarded(
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
    )
  ),
  (
    'force-failure',
    public.cmd_dataset_support_approve_guarded(
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
  ),
  (
    'replay-bound',
    public.cmd_dataset_support_approve_guarded(
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
    )
  ),
  (
    'flowproperty',
    public.cmd_dataset_support_approve_guarded(
      'flowproperties',
      'b3000000-0000-0000-0000-000000000001',
      '01.00.000',
      '2026-07-11 00:00:00+00',
      pg_temp.flowproperty_payload('flow property success'),
      jsonb_build_object(
        'plan_sha256', repeat('d', 64),
        'operation_id', 'maintenance-flowproperty',
        'action_id', 'publish-flowproperty'
      )
    )
  );

select ok(
  (
    select count(*) = 5
      and bool_and(response @> '{"ok":true,"idempotent_replay":false}'::jsonb)
      and bool_and(jsonb_typeof(response->'audit_id') = 'string')
      and bool_and(jsonb_typeof(response->'data'->'approval_audit_id') = 'string')
      and bool_and((response->'data'->>'reviewer_user_id')::uuid =
        'b1000000-0000-0000-0000-000000000003'::uuid)
      and bool_and(response->'data'->>'reviewer_email' =
        'guarded-publish-reviewer@example.com')
    from pg_temp.approval_results
  ),
  'independent review-admin approvals are recorded for every exact frozen action with lossless text ids'
);

select ok(
  (
    select replay @> '{"ok":true,"idempotent_replay":true}'::jsonb
      and replay->'data'->>'approval_audit_id' = original.response->'data'->>'approval_audit_id'
    from pg_temp.approval_results as original
    cross join lateral (
      select public.cmd_dataset_support_approve_guarded(
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
      ) as replay
    ) as replay_call
    where original.action_key = 'success'
  ),
  'repeating the same reviewer decision returns the same durable approval audit id'
);

reset role;

select ok(
  (
    select count(*) = 1
    from public.command_audit_log
    where id = (
        select (response->'data'->>'approval_audit_id')::bigint
        from pg_temp.approval_results
        where action_key = 'success'
      )
      and command = 'cmd_dataset_support_approve_guarded'
      and actor_user_id = 'b1000000-0000-0000-0000-000000000003'
      and target_table = 'unitgroups'
      and target_id = 'b2000000-0000-0000-0000-000000000001'
      and target_version = '01.00.000'
      and payload->>'decision' = 'approved_for_publication'
      and payload->>'reviewer_role' = 'review-admin'
      and payload->>'reviewer_email' = 'guarded-publish-reviewer@example.com'
      and payload->>'target_owner_user_id' = 'b1000000-0000-0000-0000-000000000001'
      and payload->>'plan_sha256' = repeat('a', 64)
      and payload->>'operation_id' = 'maintenance-success'
      and payload->>'action_id' = 'publish-unitgroup'
      and payload->'expected_modified_at' = to_jsonb('2026-07-11 00:00:00+00'::timestamptz)
      and payload->'expected_json_ordered' = pg_temp.unitgroup_payload('publish success')
  ),
  'approval audit immutably binds reviewer role, explicit decision, owner, target, plan, action, and snapshot'
);

with inserted as (
  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    'cmd_dataset_support_approve_guarded',
    'b1000000-0000-0000-0000-000000000003',
    'unitgroups',
    'b2000000-0000-0000-0000-000000000006',
    '01.00.000',
    jsonb_build_object(
      'plan_sha256', repeat('b', 64),
      'operation_id', 'maintenance-independent',
      'action_id', 'publish-independent',
      'decision', 'approved_for_publication',
      'reviewer_role', 'review-admin',
      'reviewer_email', 'guarded-publish-reviewer@example.com',
      'target_owner_user_id', 'b1000000-0000-0000-0000-000000000001',
      'expected_modified_at', '2026-07-11 00:00:00+00'::timestamptz,
      'expected_json_ordered', pg_temp.unitgroup_payload('independently published')
    )
  )
  returning id
)
insert into pg_temp.approval_results (action_key, response)
select
  'independent',
  jsonb_build_object(
    'data', jsonb_build_object('approval_audit_id', id::text)
  )
from inserted;

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
    'approval_audit_id', (
      select response->'data'->>'approval_audit_id'
      from pg_temp.approval_results
      where action_key = 'audit-conflict'
    ),
    'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
    'approval_reviewer_email', pg_temp.approval_reviewer_email(),
    'expected_modified_at', '2026-07-11 00:00:00+00'::timestamptz,
    'expected_json_ordered', pg_temp.unitgroup_payload('audit state conflict')
  )
);

delete from public.roles
where user_id = 'b1000000-0000-0000-0000-000000000003'
  and team_id = '00000000-0000-0000-0000-000000000000'
  and role = 'review-admin';

set local role authenticated;
select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000001', true);

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
      'approval_audit_id', (
        select response->'data'->>'approval_audit_id'
        from pg_temp.approval_results
        where action_key = 'success'
      ),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'publication revalidates that the independent approver still holds the review-admin role'
);

reset role;

insert into public.roles (user_id, team_id, role)
values (
  'b1000000-0000-0000-0000-000000000003',
  '00000000-0000-0000-0000-000000000000',
  'review-admin'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', 'b1000000-0000-0000-0000-000000000001', true);

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
  'DATASET_PUBLISH_APPROVAL_REQUIRED',
  'dataset owner cannot publish a valid frozen draft without the independent approval id'
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
      'approval_audit_id', '999999999999999999',
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'dataset owner cannot forge an approval by supplying an arbitrary audit id'
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
      'approval_audit_id', 123,
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_REQUIRED',
  'guarded publish rejects a numeric JSON approval id before any mutation'
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
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', 'b1000000-0000-0000-0000-000000000002',
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'guarded publish rejects reviewer UUID tampering before publication'
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
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', 'tampered-reviewer@example.com'
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'guarded publish rejects reviewer email tampering before publication'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('9', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'a real approval id cannot authorize a different plan hash'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000002',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('stale timestamp'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'a real approval id cannot authorize a different target id'
);

select is(
  public.cmd_dataset_publish_guarded(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.001',
    '2026-07-11 00:00:00+00',
    jsonb_set(
      pg_temp.unitgroup_payload('wrong version fixture'),
      '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}',
      '"01.00.001"'::jsonb
    ),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'a real approval id cannot authorize a different target version'
);

select is(
  public.cmd_dataset_publish_guarded(
    'flowproperties',
    'b3000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.flowproperty_payload('flow property success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'a real approval id cannot authorize a different dataset table'
);

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
      'action_id', 'publish-audit-conflict',
      'approval_audit_id', pg_temp.approval_id('audit-conflict'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
        'action_id', 'publish-force-audit-failure',
        'approval_audit_id', pg_temp.approval_id('force-failure'),
        'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
        'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
      and jsonb_typeof(result->'audit_id') = 'string'
      and jsonb_typeof(result->'approval_audit_id') = 'string'
      and result->>'approval_audit_id' = pg_temp.approval_id('success')
      and result->>'approval_reviewer_user_id' =
        'b1000000-0000-0000-0000-000000000003'
      and result->>'approval_reviewer_email' =
        'guarded-publish-reviewer@example.com'
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
          'reason_code', 'FPUG-001',
          'approval_audit_id', pg_temp.approval_id('success'),
          'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
          'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
      'reason_code', 'FPUG-001',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'idempotent_replay',
  'true',
  'same guarded action replays successfully after the first commit'
);

select ok(
  public.qry_dataset_publish_guarded_proof(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'publish_audit_id', pg_temp.publish_audit_id('publish-unitgroup')
    )
  ) @> jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'proof_verified', true,
      'publish_audit_id', pg_temp.publish_audit_id('publish-unitgroup'),
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  ),
  'owner verify receives database-backed exact approval and publication audit proof'
);

select is(
  public.qry_dataset_publish_guarded_proof(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'publish_audit_id', '999999999999999998'
    )
  )->>'code',
  'DATASET_PUBLISH_PROOF_INVALID',
  'a forged local publication audit id cannot satisfy database-backed verify'
);

select is(
  public.qry_dataset_publish_guarded_proof(
    'unitgroups',
    'b2000000-0000-0000-0000-000000000001',
    '01.00.000',
    '2026-07-11 00:00:00+00',
    pg_temp.unitgroup_payload('publish success'),
    jsonb_build_object(
      'plan_sha256', repeat('a', 64),
      'operation_id', 'maintenance-success',
      'action_id', 'different-action',
      'approval_audit_id', pg_temp.approval_id('success'),
      'publish_audit_id', pg_temp.publish_audit_id('publish-unitgroup')
    )
  )->>'code',
  'DATASET_PUBLISH_PROOF_INVALID',
  'database-backed verify rejects a real audit id claimed by another action'
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
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'a timestamp-drifted replay cannot reuse approval for the original frozen snapshot'
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
      'action_id', 'different-action',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'published row cannot be claimed by an action that the reviewer did not approve'
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
      'action_id', 'publish-unitgroup',
      'approval_audit_id', pg_temp.approval_id('success'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
      'action_id', 'publish-independent',
      'approval_audit_id', pg_temp.approval_id('independent'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
      'action_id', 'publish-replay-bound',
      'approval_audit_id', pg_temp.approval_id('replay-bound'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
      'action_id', 'publish-replay-bound',
      'approval_audit_id', pg_temp.approval_id('replay-bound'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
    )
  )->>'code',
  'DATASET_PUBLISH_APPROVAL_INVALID',
  'same correlation cannot reuse reviewer approval after a privileged payload change'
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
      'reason_code', 'FPUG-001',
      'approval_audit_id', pg_temp.approval_id('flowproperty'),
      'approval_reviewer_user_id', pg_temp.approval_reviewer_user_id(),
      'approval_reviewer_email', pg_temp.approval_reviewer_email()
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
      and payload->>'approval_audit_id' = pg_temp.approval_id('success')
      and jsonb_typeof(payload->'approval_audit_id') = 'string'
      and payload->>'approval_reviewer_user_id' =
        'b1000000-0000-0000-0000-000000000003'
      and payload->>'approval_reviewer_email' =
        'guarded-publish-reviewer@example.com'
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
      and payload->>'approval_audit_id' = pg_temp.approval_id('flowproperty')
      and jsonb_typeof(payload->'approval_audit_id') = 'string'
      and payload->>'approval_reviewer_user_id' =
        'b1000000-0000-0000-0000-000000000003'
      and payload->>'approval_reviewer_email' =
        'guarded-publish-reviewer@example.com'
  ),
  '1',
  'guarded flow property publish writes its own exact audit correlation'
);

select * from finish();
rollback;
