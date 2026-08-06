begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(26);

select is(
  (
    select count(*)
    from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname = any(array[
        'svc_identity_event_claim', 'svc_identity_event_release',
        'svc_identity_desired_state_upsert', 'svc_identity_desired_state_read',
        'svc_identity_login_bind', 'svc_identity_managed_role_materialize'
      ])
      and routine.prosecdef
      and routine.proconfig = array['search_path=""']::text[]
  ),
  6::bigint,
  'all six Identity Center facades use fixed-path SECURITY DEFINER'
);

select ok(
  has_function_privilege('service_role', 'api.svc_identity_event_claim(text,text)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_identity_desired_state_upsert(text,text,text,text,jsonb)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_identity_login_bind(text,uuid)', 'EXECUTE')
    and has_function_privilege('service_role', 'api.svc_identity_managed_role_materialize(text,uuid)', 'EXECUTE'),
  'service role can execute Identity Center facades'
);

select ok(
  not has_function_privilege('anon', 'api.svc_identity_event_claim(text,text)', 'EXECUTE')
    and not has_function_privilege('authenticated', 'api.svc_identity_event_claim(text,text)', 'EXECUTE')
    and not has_function_privilege('anon', 'api.svc_identity_login_bind(text,uuid)', 'EXECUTE')
    and not has_function_privilege('authenticated', 'api.svc_identity_managed_role_materialize(text,uuid)', 'EXECUTE'),
  'browser roles cannot execute Identity Center facades'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password,
  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
  created_at, updated_at, is_sso_user, is_anonymous
) values
  ('00000000-0000-0000-0000-000000000000', '42240000-0000-4000-8000-000000000001', 'authenticated', 'authenticated', 'ic-one@example.com', 'test', now(), '{}', '{}', now(), now(), false, false),
  ('00000000-0000-0000-0000-000000000000', '42240000-0000-4000-8000-000000000002', 'authenticated', 'authenticated', 'ic-two@example.com', 'test', now(), '{}', '{}', now(), now(), false, false),
  ('00000000-0000-0000-0000-000000000000', '42240000-0000-4000-8000-000000000003', 'authenticated', 'authenticated', 'ic-owner@example.com', 'test', now(), '{}', '{}', now(), now(), false, false);

set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select ok(api.svc_identity_event_claim('event-1', 'user.updated'), 'first event claim succeeds');
select ok(not api.svc_identity_event_claim('event-1', 'changed'), 'duplicate event claim is idempotent');
select is(
  (select event_type from private.identity_center_processed_events where event_id = 'event-1'),
  'user.updated'::text,
  'duplicate claim preserves the first event payload identity'
);
select ok(api.svc_identity_event_release('event-1'), 'event release removes the claim');
select ok(not api.svc_identity_event_release('event-1'), 'event release is idempotent');
select ok(api.svc_identity_event_claim('event-1', 'retry'), 'released event can be claimed again');

select is(
  api.svc_identity_desired_state_upsert('subject-1', null, null, 'preserve', '{"source":"fixture"}') ->> 'status',
  'active'::text,
  'desired-state placeholder defaults to active'
);
select is(
  api.svc_identity_desired_state_upsert('subject-1', null, 'admin', 'set', null) ->> 'desired_role',
  'admin'::text,
  'desired-state set stores a managed role'
);
select is(
  api.svc_identity_desired_state_upsert('subject-1', 'disabled', 'review-admin', 'revoke', null) ->> 'desired_role',
  'admin'::text,
  'mismatched revoke preserves desired role while updating status'
);
select is(
  api.svc_identity_desired_state_upsert('subject-1', null, 'admin', 'revoke', null) ->> 'desired_role',
  null::text,
  'matching revoke clears desired role'
);
select is(
  api.svc_identity_desired_state_read('subject-1') ->> 'status',
  'disabled'::text,
  'desired-state read returns the durable state'
);
select is(
  api.svc_identity_desired_state_read('missing-subject'),
  null::jsonb,
  'desired-state read returns null for an unknown subject'
);

select is(
  api.svc_identity_login_bind('subject-1', '42240000-0000-4000-8000-000000000001') ->> 'user_id',
  '42240000-0000-4000-8000-000000000001'::text,
  'login bind attaches an unbound subject'
);
select is(
  api.svc_identity_login_bind('subject-1', '42240000-0000-4000-8000-000000000001') ->> 'user_id',
  '42240000-0000-4000-8000-000000000001'::text,
  'exact login bind replay is idempotent'
);
select throws_ok(
  $$select api.svc_identity_login_bind('subject-1', '42240000-0000-4000-8000-000000000002')$$,
  '23505', 'IDENTITY_SUBJECT_ALREADY_BOUND',
  'subject cannot be rebound to another user'
);
select throws_ok(
  $$select api.svc_identity_login_bind('subject-2', '42240000-0000-4000-8000-000000000001')$$,
  '23505', 'IDENTITY_USER_ALREADY_BOUND',
  'user cannot be claimed by another subject'
);

select api.svc_identity_desired_state_upsert('subject-1', null, 'review-admin', 'set', null);
select is(
  api.svc_identity_managed_role_materialize('subject-1', '42240000-0000-4000-8000-000000000001') ->> 'effective_role',
  'review-admin'::text,
  'managed role materialization creates the requested role'
);
select is(
  api.svc_identity_managed_role_materialize('subject-1', '42240000-0000-4000-8000-000000000001') ->> 'changed',
  'false'::text,
  'managed role replay reports no change'
);
select api.svc_identity_desired_state_upsert('subject-1', null, null, 'revoke', null);
select is(
  api.svc_identity_managed_role_materialize('subject-1', '42240000-0000-4000-8000-000000000001') ->> 'effective_role',
  'member'::text,
  'revoking a managed role demotes only that managed value to member'
);

select api.svc_identity_desired_state_upsert('subject-owner', null, 'admin', 'set', null);
select api.svc_identity_login_bind('subject-owner', '42240000-0000-4000-8000-000000000003');
insert into private.roles (user_id, team_id, role) values (
  '42240000-0000-4000-8000-000000000003',
  '00000000-0000-0000-0000-000000000000',
  'owner'
);
select is(
  api.svc_identity_managed_role_materialize('subject-owner', '42240000-0000-4000-8000-000000000003') ->> 'effective_role',
  'owner'::text,
  'Identity Center never overwrites an application-owned role'
);
select is(
  (select role::text from private.roles where user_id = '42240000-0000-4000-8000-000000000003'),
  'owner'::text,
  'non-managed role remains unchanged in storage'
);

select throws_ok(
  $$select api.svc_identity_managed_role_materialize('subject-1', '42240000-0000-4000-8000-000000000002')$$,
  'P0001', 'IDENTITY_BINDING_MISMATCH',
  'managed role materialization rejects subject/user mismatch'
);

reset role;

select is(
  (select count(*) from private.identity_center_users where keycloak_sub = 'subject-1'),
  1::bigint,
  'Identity Center retries keep one durable subject row'
);

select * from finish();

rollback;
