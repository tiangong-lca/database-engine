begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(12);

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
    '81000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'invite-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"invite-owner@example.com","display_name":"Invite Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'invite-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"invite-admin@example.com","display_name":"Invite Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000003',
    'authenticated',
    'authenticated',
    'invite-member@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"invite-member@example.com","display_name":"Invite Member"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000004',
    'authenticated',
    'authenticated',
    'fresh-invitee@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"fresh-invitee@example.com","display_name":"Fresh Invitee"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000005',
    'authenticated',
    'authenticated',
    'other-member@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"other-member@example.com","display_name":"Other Member"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000006',
    'authenticated',
    'authenticated',
    'other-invited@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"other-invited@example.com","display_name":"Other Invited"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000007',
    'authenticated',
    'authenticated',
    'other-rejected@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"other-rejected@example.com","display_name":"Other Rejected"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000008',
    'authenticated',
    'authenticated',
    'same-invited@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"same-invited@example.com","display_name":"Same Invited"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '81000000-0000-0000-0000-000000000009',
    'authenticated',
    'authenticated',
    'same-rejected@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"same-rejected@example.com","display_name":"Same Rejected"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.teams (id, json, rank, is_public, modified_at)
values
  ('82000000-0000-0000-0000-000000000001', '{"title":[{"@xml:lang":"en","#text":"Invite Team"}]}'::jsonb, 1, false, now()),
  ('82000000-0000-0000-0000-000000000002', '{"title":[{"@xml:lang":"en","#text":"Other Team"}]}'::jsonb, 2, false, now());

insert into public.roles (user_id, team_id, role, modified_at)
values
  ('81000000-0000-0000-0000-000000000001', '82000000-0000-0000-0000-000000000001', 'owner', now()),
  ('81000000-0000-0000-0000-000000000002', '82000000-0000-0000-0000-000000000001', 'admin', now()),
  ('81000000-0000-0000-0000-000000000003', '82000000-0000-0000-0000-000000000001', 'member', now()),
  ('81000000-0000-0000-0000-000000000005', '82000000-0000-0000-0000-000000000002', 'member', now()),
  ('81000000-0000-0000-0000-000000000006', '82000000-0000-0000-0000-000000000002', 'is_invited', now()),
  ('81000000-0000-0000-0000-000000000007', '82000000-0000-0000-0000-000000000002', 'rejected', now()),
  ('81000000-0000-0000-0000-000000000008', '82000000-0000-0000-0000-000000000001', 'is_invited', now()),
  ('81000000-0000-0000-0000-000000000009', '82000000-0000-0000-0000-000000000001', 'rejected', now());

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000001', true);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'fresh-invitee@example.com'
  ) #>> '{data,id}',
  '81000000-0000-0000-0000-000000000004',
  'team owner can resolve a fresh invitee by exact email'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    '  FRESH-INVITEE@EXAMPLE.COM  '
  )->>'ok',
  'true',
  'invitee lookup normalizes email case and whitespace'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000002', true);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'fresh-invitee@example.com'
  )->>'ok',
  'true',
  'team admin can resolve a fresh invitee'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000003', true);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'fresh-invitee@example.com'
  )->>'code',
  'FORBIDDEN',
  'non-manager team members cannot use the invite lookup'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'missing@example.com'
  )->>'code',
  'FORBIDDEN',
  'unauthorized actors cannot learn whether an email exists'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000001', true);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'missing@example.com'
  )->>'code',
  'USER_NOT_FOUND',
  'authorized team managers receive USER_NOT_FOUND for missing email'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'other-member@example.com'
  )->>'code',
  'USER_ALREADY_IN_TEAM',
  'authorized lookup reports users already active in another team'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'other-invited@example.com'
  )->>'code',
  'USER_ALREADY_INVITED_TO_TEAM',
  'authorized lookup reports users already invited to another team'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'other-rejected@example.com'
  )->>'ok',
  'true',
  'rejected membership in another team does not block a fresh invite'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'same-invited@example.com'
  )->>'code',
  'TEAM_MEMBER_ALREADY_EXISTS',
  'same-team pending invitations are reported as existing membership'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    'same-rejected@example.com'
  )->>'code',
  'REINVITE_REQUIRED',
  'same-team rejected members require the reinvite command'
);

select is(
  public.qry_team_find_invitable_user_by_email(
    '82000000-0000-0000-0000-000000000001',
    '  '
  )->>'code',
  'INVALID_PAYLOAD',
  'blank email is rejected as an invalid payload'
);

select * from finish();
rollback;
