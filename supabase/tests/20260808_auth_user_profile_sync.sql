begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(27);

select ok(
  to_regprocedure('private.sync_auth_users_to_private_users()') is not null,
  'the Auth profile synchronization helper uses the private destination name'
);

select ok(
  to_regprocedure('private.sync_auth_users_to_public_users()') is null,
  'the stale public-destination synchronization helper name is retired'
);

select is(
  (
    select count(*)
    from pg_trigger as trigger_record
    where trigger_record.tgrelid = 'auth.users'::regclass
      and trigger_record.tgfoid =
        'private.sync_auth_users_to_private_users()'::regprocedure
      and not trigger_record.tgisinternal
  ),
  1::bigint,
  'auth.users has exactly one governed application-profile synchronization trigger'
);

select ok(
  (
    select trigger_record.tgdeferrable and trigger_record.tginitdeferred
    from pg_trigger as trigger_record
    where trigger_record.tgrelid = 'auth.users'::regclass
      and trigger_record.tgfoid =
        'private.sync_auth_users_to_private_users()'::regprocedure
      and not trigger_record.tgisinternal
  ),
  'the synchronization trigger preserves explicit same-transaction profile writes'
);

select ok(
  not has_function_privilege(
    'anon',
    'private.sync_auth_users_to_private_users()',
    'EXECUTE'
  )
  and not has_function_privilege(
    'authenticated',
    'private.sync_auth_users_to_private_users()',
    'EXECUTE'
  )
  and not has_function_privilege(
    'service_role',
    'private.sync_auth_users_to_private_users()',
    'EXECUTE'
  )
  and not has_function_privilege(
    'api_internal_executor',
    'private.sync_auth_users_to_private_users()',
    'EXECUTE'
  ),
  'the trigger helper is not directly executable by application roles'
);

select ok(
  exists (
    select 1
    from pg_constraint
    where conrelid = 'private.users'::regclass
      and conname = 'users_organization_metadata_contract'
      and contype = 'c'
      and convalidated
  ),
  'private.users has a validated organization metadata contract'
);

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
    '43500000-0000-4000-8000-000000000001',
    'authenticated',
    'authenticated',
    'review-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43500000-0000-4000-8000-000000000001","display_name":"Review Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43500000-0000-4000-8000-000000000002',
    'authenticated',
    'authenticated',
    'candidate@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43500000-0000-4000-8000-000000000002","display_name":"Candidate"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43500000-0000-4000-8000-000000000003',
    'authenticated',
    'authenticated',
    'deleted-candidate@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"43500000-0000-4000-8000-000000000003","display_name":"Deleted Candidate"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data, contact)
values (
  '43500000-0000-4000-8000-000000000001',
  '{"sub":"43500000-0000-4000-8000-000000000001","display_name":"Review Admin"}'::jsonb,
  '{"kind":"preserved"}'::jsonb
);

insert into private.roles (user_id, team_id, role, modified_at)
values (
  '43500000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'review-admin',
  now()
);

set constraints trg_sync_auth_users_to_private_users immediate;

select is(
  (
    select contact ->> 'kind'
    from private.users
    where id = '43500000-0000-4000-8000-000000000001'
  ),
  'preserved',
  'Auth synchronization preserves application-owned contact data'
);

select is(
  (
    select raw_user_meta_data ->> 'display_name'
    from private.users
    where id = '43500000-0000-4000-8000-000000000002'
  ),
  'Candidate',
  'an inserted Auth identity is mirrored into private.users'
);

update auth.users
set raw_user_meta_data =
  raw_user_meta_data || jsonb_build_object(
    'display_name', 'Updated Candidate',
    'organization', 'Tsinghua University',
    'profile_note', 'preserved'
  )
where id = '43500000-0000-4000-8000-000000000002';

select is(
  (
    select raw_user_meta_data ->> 'display_name'
    from private.users
    where id = '43500000-0000-4000-8000-000000000002'
  ),
  'Updated Candidate',
  'Auth metadata updates are mirrored into private.users'
);

select is(
  (
    select raw_user_meta_data ->> 'organization'
    from private.users
    where id = '43500000-0000-4000-8000-000000000002'
  ),
  'Tsinghua University',
  'organization metadata updates are mirrored into private.users'
);

select is(
  (
    select raw_user_meta_data ->> 'profile_note'
    from private.users
    where id = '43500000-0000-4000-8000-000000000002'
  ),
  'preserved',
  'organization metadata updates preserve unrelated user metadata'
);

update auth.users
set raw_user_meta_data =
  raw_user_meta_data || '{"organization":"TianGong Initiative"}'::jsonb
where id = '43500000-0000-4000-8000-000000000001';

select is(
  (
    select raw_user_meta_data ->> 'organization'
    from private.users
    where id = '43500000-0000-4000-8000-000000000001'
  ),
  'TianGong Initiative',
  'organization metadata is mirrored for an existing private profile'
);

select is(
  (
    select contact ->> 'kind'
    from private.users
    where id = '43500000-0000-4000-8000-000000000001'
  ),
  'preserved',
  'organization metadata updates preserve application-owned contact data'
);

update auth.users
set raw_user_meta_data =
  raw_user_meta_data || '{"organization":""}'::jsonb
where id = '43500000-0000-4000-8000-000000000002';

select is(
  (
    select raw_user_meta_data ->> 'organization'
    from private.users
    where id = '43500000-0000-4000-8000-000000000002'
  ),
  '',
  'an empty organization string remains a valid clear value'
);

select throws_ok(
  $$
    update auth.users
    set raw_user_meta_data = raw_user_meta_data || '{"organization":42}'::jsonb
    where id = '43500000-0000-4000-8000-000000000002'
  $$,
  '23514',
  null,
  'numeric organization metadata is rejected'
);

select throws_ok(
  $$
    update auth.users
    set raw_user_meta_data =
      raw_user_meta_data || '{"organization":{"name":"Example"}}'::jsonb
    where id = '43500000-0000-4000-8000-000000000002'
  $$,
  '23514',
  null,
  'object organization metadata is rejected'
);

select throws_ok(
  $$
    update auth.users
    set raw_user_meta_data =
      raw_user_meta_data || '{"organization":["Example"]}'::jsonb
    where id = '43500000-0000-4000-8000-000000000002'
  $$,
  '23514',
  null,
  'array organization metadata is rejected'
);

select throws_ok(
  $$
    update auth.users
    set raw_user_meta_data =
      raw_user_meta_data || '{"organization":null}'::jsonb
    where id = '43500000-0000-4000-8000-000000000002'
  $$,
  '23514',
  null,
  'JSON null organization metadata is rejected'
);

select throws_ok(
  $$
    update auth.users
    set raw_user_meta_data =
      raw_user_meta_data || '{"organization":" Example University "}'::jsonb
    where id = '43500000-0000-4000-8000-000000000002'
  $$,
  '23514',
  null,
  'organization metadata with surrounding whitespace is rejected'
);

select throws_ok(
  $$
    update auth.users
    set raw_user_meta_data = raw_user_meta_data || jsonb_build_object(
      'organization',
      repeat('x', 201)
    )
    where id = '43500000-0000-4000-8000-000000000002'
  $$,
  '23514',
  null,
  'organization metadata longer than 200 characters is rejected'
);

delete from auth.users
where id = '43500000-0000-4000-8000-000000000003';

select ok(
  not exists (
    select 1
    from private.users
    where id = '43500000-0000-4000-8000-000000000003'
  ),
  'deleting an Auth identity removes its private profile mirror'
);

-- Reproduce the reported hosted drift: the registered user remains in Auth,
-- but its application profile mirror is absent.
delete from private.users
where id = '43500000-0000-4000-8000-000000000002';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '43500000-0000-4000-8000-000000000001',
  true
);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  (
    select id
    from api.qry_review_find_member_candidate_by_email(
      ' CANDIDATE@example.com '
    )
    limit 1
  ),
  '43500000-0000-4000-8000-000000000002'::uuid,
  'review-admin can find an Auth-only member candidate by normalized email'
);

select is(
  (
    select email
    from api.qry_review_find_member_candidate_by_email('candidate@example.com')
    limit 1
  ),
  'candidate@example.com',
  'the Auth-only candidate returns the canonical Auth email'
);

select is(
  (
    select display_name
    from api.qry_review_find_member_candidate_by_email('candidate@example.com')
    limit 1
  ),
  'Updated Candidate',
  'the Auth-only candidate returns display metadata from Auth'
);

select is(
  (
    select contact
    from api.qry_review_find_member_candidate_by_email('candidate@example.com')
    limit 1
  ),
  null::jsonb,
  'the Auth-only candidate has a null application contact until its mirror is repaired'
);

reset role;
set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '43500000-0000-4000-8000-000000000002',
  true
);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select throws_ok(
  $$select * from api.qry_review_find_member_candidate_by_email('candidate@example.com')$$,
  '42501',
  'REVIEW_ADMIN_REQUIRED',
  'ordinary authenticated users cannot use the review candidate lookup'
);

reset role;

select ok(
  has_function_privilege(
    'authenticated',
    'api.qry_review_find_member_candidate_by_email(text)',
    'EXECUTE'
  )
  and not has_function_privilege(
    'anon',
    'api.qry_review_find_member_candidate_by_email(text)',
    'EXECUTE'
  ),
  'the review candidate RPC keeps its authenticated-only execute boundary'
);

select * from finish();
rollback;
