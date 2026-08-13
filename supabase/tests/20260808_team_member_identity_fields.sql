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
    '43100000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43100000-0000-0000-0000-000000000001","display_name":"Team Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43100000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43100000-0000-0000-0000-000000000002","name":"Team Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43100000-0000-0000-0000-000000000003',
    'authenticated',
    'authenticated',
    'member@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43100000-0000-0000-0000-000000000003"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43100000-0000-0000-0000-000000000004',
    'authenticated',
    'authenticated',
    null,
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43100000-0000-0000-0000-000000000004","email":"legacy-invitee@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43100000-0000-0000-0000-000000000005',
    'authenticated',
    'authenticated',
    'outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43100000-0000-0000-0000-000000000005","display_name":"Outsider"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data, contact)
values
  (
    '43100000-0000-0000-0000-000000000001',
    '{"sub":"43100000-0000-0000-0000-000000000001","display_name":"Team Owner"}'::jsonb,
    null
  ),
  (
    '43100000-0000-0000-0000-000000000002',
    '{"sub":"43100000-0000-0000-0000-000000000002","name":"Team Admin"}'::jsonb,
    null
  ),
  (
    '43100000-0000-0000-0000-000000000003',
    '{"sub":"43100000-0000-0000-0000-000000000003"}'::jsonb,
    null
  ),
  (
    '43100000-0000-0000-0000-000000000004',
    '{"sub":"43100000-0000-0000-0000-000000000004","email":"legacy-invitee@example.com"}'::jsonb,
    null
  ),
  (
    '43100000-0000-0000-0000-000000000005',
    '{"sub":"43100000-0000-0000-0000-000000000005","display_name":"Outsider"}'::jsonb,
    null
  );

insert into private.teams (id, json, rank, is_public, modified_at)
values (
  '43110000-0000-0000-0000-000000000001',
  '{"title":[{"@xml:lang":"en","#text":"Identity Team"}]}'::jsonb,
  1,
  false,
  now()
);

insert into private.roles (user_id, team_id, role, modified_at)
values
  (
    '43100000-0000-0000-0000-000000000001',
    '43110000-0000-0000-0000-000000000001',
    'owner',
    now()
  ),
  (
    '43100000-0000-0000-0000-000000000002',
    '43110000-0000-0000-0000-000000000001',
    'admin',
    now()
  ),
  (
    '43100000-0000-0000-0000-000000000003',
    '43110000-0000-0000-0000-000000000001',
    'member',
    now()
  ),
  (
    '43100000-0000-0000-0000-000000000004',
    '43110000-0000-0000-0000-000000000001',
    'is_invited',
    now()
  );

select ok(
  has_function_privilege(
    'authenticated',
    'api.qry_team_get_member_list(uuid,integer,integer,text,text)',
    'EXECUTE'
  )
  and not has_function_privilege(
    'anon',
    'api.qry_team_get_member_list(uuid,integer,integer,text,text)',
    'EXECUTE'
  ),
  'team member list keeps its authenticated-only execute boundary'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '43100000-0000-0000-0000-000000000001', true);

select is(
  (
    select count(*)::text
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
  ),
  '4',
  'team owner can read the complete member list'
);

select is(
  (
    select email
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000001'
  ),
  'owner@example.com',
  'team member email comes from the canonical auth identity when metadata omits it'
);

select is(
  (
    select display_name
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000001'
  ),
  'Team Owner',
  'display_name metadata remains the preferred member name'
);

select is(
  (
    select display_name
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000002'
  ),
  'Team Admin',
  'name metadata is used when display_name is absent'
);

select is(
  (
    select display_name
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000003'
  ),
  'member@example.com',
  'canonical email is the member name fallback when profile names are absent'
);

select is(
  (
    select email
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000004'
  ),
  'legacy-invitee@example.com',
  'metadata email remains a compatibility fallback when auth email is absent'
);

select is(
  (
    select display_name
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000004'
  ),
  'legacy-invitee@example.com',
  'metadata email is also the final member name compatibility fallback'
);

select is(
  (
    select total_count::text
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      1,
      'created_at',
      'desc'
    )
  ),
  '4',
  'pagination retains the full team member count'
);

select is(
  (
    select role
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
    where user_id = '43100000-0000-0000-0000-000000000003'
  ),
  'member',
  'member roles remain unchanged'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '43100000-0000-0000-0000-000000000002', true);

select is(
  (
    select count(*)::text
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
  ),
  '4',
  'team admin retains manager visibility'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '43100000-0000-0000-0000-000000000003', true);

select is(
  (
    select count(*)::text
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
  ),
  '0',
  'ordinary team members still cannot read the member list'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '43100000-0000-0000-0000-000000000005', true);

select is(
  (
    select count(*)::text
    from api.qry_team_get_member_list(
      '43110000-0000-0000-0000-000000000001',
      1,
      20,
      'created_at',
      'desc'
    )
  ),
  '0',
  'non-members cannot read the team member list'
);

select * from finish();
rollback;
