begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(13);

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
    '43200000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'system-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43200000-0000-0000-0000-000000000001","display_name":"System Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43200000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'system-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43200000-0000-0000-0000-000000000002","name":"System Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43200000-0000-0000-0000-000000000003',
    'authenticated',
    'authenticated',
    'system-member@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43200000-0000-0000-0000-000000000003"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43200000-0000-0000-0000-000000000004',
    'authenticated',
    'authenticated',
    null,
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43200000-0000-0000-0000-000000000004","email":"legacy-system-member@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43200000-0000-0000-0000-000000000005',
    'authenticated',
    'authenticated',
    'system-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43200000-0000-0000-0000-000000000005","display_name":"System Outsider"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data, contact)
values
  (
    '43200000-0000-0000-0000-000000000001',
    '{"sub":"43200000-0000-0000-0000-000000000001","display_name":"System Owner"}'::jsonb,
    null
  ),
  (
    '43200000-0000-0000-0000-000000000002',
    '{"sub":"43200000-0000-0000-0000-000000000002","name":"System Admin"}'::jsonb,
    null
  ),
  (
    '43200000-0000-0000-0000-000000000003',
    '{"sub":"43200000-0000-0000-0000-000000000003"}'::jsonb,
    null
  ),
  (
    '43200000-0000-0000-0000-000000000004',
    '{"sub":"43200000-0000-0000-0000-000000000004","email":"legacy-system-member@example.com"}'::jsonb,
    null
  ),
  (
    '43200000-0000-0000-0000-000000000005',
    '{"sub":"43200000-0000-0000-0000-000000000005","display_name":"System Outsider"}'::jsonb,
    null
  );

insert into private.roles (user_id, team_id, role, modified_at)
values
  (
    '43200000-0000-0000-0000-000000000001',
    '00000000-0000-0000-0000-000000000000',
    'owner',
    now()
  ),
  (
    '43200000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000000',
    'admin',
    now()
  ),
  (
    '43200000-0000-0000-0000-000000000003',
    '00000000-0000-0000-0000-000000000000',
    'member',
    now()
  ),
  (
    '43200000-0000-0000-0000-000000000004',
    '00000000-0000-0000-0000-000000000000',
    'member',
    now()
  );

select ok(
  has_function_privilege(
    'authenticated',
    'api.qry_system_get_member_list(integer,integer,text,text)',
    'EXECUTE'
  )
  and not has_function_privilege(
    'anon',
    'api.qry_system_get_member_list(integer,integer,text,text)',
    'EXECUTE'
  ),
  'system member list keeps its authenticated-only execute boundary'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '43200000-0000-0000-0000-000000000001', true);

select is(
  (select count(*)::text from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')),
  '4',
  'system owner can read the complete system member list'
);

select is(
  (
    select email
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000001'
  ),
  'system-owner@example.com',
  'system member email comes from the canonical auth identity when metadata omits it'
);

select is(
  (
    select display_name
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000001'
  ),
  'System Owner',
  'display_name metadata remains the preferred system member name'
);

select is(
  (
    select display_name
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000002'
  ),
  'System Admin',
  'name metadata is used when system member display_name is absent'
);

select is(
  (
    select display_name
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000003'
  ),
  'system-member@example.com',
  'canonical email is the system member name fallback when profile names are absent'
);

select is(
  (
    select email
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000004'
  ),
  'legacy-system-member@example.com',
  'metadata email remains a system member compatibility fallback when auth email is absent'
);

select is(
  (
    select display_name
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000004'
  ),
  'legacy-system-member@example.com',
  'metadata email is also the final system member name compatibility fallback'
);

select is(
  (select total_count::text from api.qry_system_get_member_list(1, 1, 'created_at', 'desc')),
  '4',
  'pagination retains the full system member count'
);

select is(
  (
    select role
    from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')
    where user_id = '43200000-0000-0000-0000-000000000003'
  ),
  'member',
  'system member roles remain unchanged'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '43200000-0000-0000-0000-000000000002', true);

select is(
  (select count(*)::text from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')),
  '4',
  'system admin retains system member list visibility'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '43200000-0000-0000-0000-000000000003', true);

select is(
  (select count(*)::text from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')),
  '4',
  'ordinary system members retain their existing system member list visibility'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '43200000-0000-0000-0000-000000000005', true);

select is(
  (select count(*)::text from api.qry_system_get_member_list(1, 20, 'created_at', 'desc')),
  '0',
  'users outside the system team cannot read the system member list'
);

select * from finish();
rollback;
