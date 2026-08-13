begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(7);

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
    'team-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"81000000-0000-0000-0000-000000000001","email":"team-owner@example.com","display_name":"Team Owner"}'::jsonb,
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
    'team-member@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"81000000-0000-0000-0000-000000000002","email":"team-member@example.com","display_name":"Team Member"}'::jsonb,
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
    'review-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"81000000-0000-0000-0000-000000000003","email":"review-admin@example.com","display_name":"Review Admin"}'::jsonb,
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
    'reviewer@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"81000000-0000-0000-0000-000000000004","email":"reviewer@example.com","display_name":"Reviewer"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data, contact)
values
  (
    '81000000-0000-0000-0000-000000000001',
    '{"email":"team-owner@example.com","display_name":"Team Owner"}'::jsonb,
    null
  ),
  (
    '81000000-0000-0000-0000-000000000002',
    '{"email":"team-member@example.com","display_name":"Team Member"}'::jsonb,
    null
  ),
  (
    '81000000-0000-0000-0000-000000000003',
    '{"email":"review-admin@example.com","display_name":"Review Admin"}'::jsonb,
    null
  ),
  (
    '81000000-0000-0000-0000-000000000004',
    '{"email":"reviewer@example.com","display_name":"Reviewer"}'::jsonb,
    null
  );

insert into private.teams (id, json, rank, is_public, modified_at)
values (
  '82000000-0000-0000-0000-000000000001',
  '{"title":[{"@xml:lang":"en","#text":"Owned Team"}]}'::jsonb,
  1,
  false,
  now()
);

insert into private.roles (user_id, team_id, role, modified_at)
values
  ('81000000-0000-0000-0000-000000000001', '82000000-0000-0000-0000-000000000001', 'owner', now()),
  ('81000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000000', 'review-admin', now()),
  ('81000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000000', 'review-member', now());

insert into private.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json,
  created_at,
  modified_at
)
values (
  '83000000-0000-0000-0000-000000000001',
  '84000000-0000-0000-0000-000000000001',
  '01.00.000',
  0,
  '["81000000-0000-0000-0000-000000000004"]'::jsonb,
  '{"data":{"id":"84000000-0000-0000-0000-000000000001","version":"01.00.000"}}'::jsonb,
  now(),
  now()
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000001', true);

do $$
begin
  begin
    update private.teams
    set rank = 99
    where id = '82000000-0000-0000-0000-000000000001';
  exception
    when others then
      null;
  end;
end;
$$;

reset role;

select is(
  (select rank::text from private.teams where id = '82000000-0000-0000-0000-000000000001'),
  '1',
  'team owner cannot directly update teams row after raw write revoke'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000001', true);

do $$
begin
  begin
    insert into private.roles (user_id, team_id, role, modified_at)
    values (
      '81000000-0000-0000-0000-000000000002',
      '82000000-0000-0000-0000-000000000001',
      'member',
      now()
    );
  exception
    when others then
      null;
  end;
end;
$$;

reset role;

select is(
  (
    select count(*)::text
    from private.roles
    where user_id = '81000000-0000-0000-0000-000000000002'
      and team_id = '82000000-0000-0000-0000-000000000001'
  ),
  '0',
  'authenticated user cannot directly insert roles row after raw write revoke'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000002', true);

do $$
begin
  begin
    update private.users
    set contact = '{"phone":"123"}'::jsonb
    where id = '81000000-0000-0000-0000-000000000002';
  exception
    when others then
      null;
  end;
end;
$$;

reset role;

select is(
  (
    select coalesce(contact::text, 'null')
    from private.users
    where id = '81000000-0000-0000-0000-000000000002'
  ),
  'null',
  'authenticated user cannot directly update users row after raw write revoke'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000003', true);

do $$
begin
  begin
    update private.reviews
    set state_code = 2
    where id = '83000000-0000-0000-0000-000000000001';
  exception
    when others then
      null;
  end;
end;
$$;

reset role;

select is(
  (select state_code::text from private.reviews where id = '83000000-0000-0000-0000-000000000001'),
  '0',
  'review admin cannot directly update reviews row after raw write revoke'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '81000000-0000-0000-0000-000000000004', true);

do $$
begin
  begin
    insert into private.comments (
      review_id,
      reviewer_id,
      json,
      state_code,
      created_at,
      modified_at
    )
    values (
      '83000000-0000-0000-0000-000000000001',
      '81000000-0000-0000-0000-000000000004',
      '{"note":"draft"}'::jsonb,
      0,
      now(),
      now()
    );
  exception
    when others then
      null;
  end;
end;
$$;

reset role;

select is(
  (
    select count(*)::text
    from private.comments
    where review_id = '83000000-0000-0000-0000-000000000001'
  ),
  '0',
  'reviewer cannot directly insert comments row after raw write revoke'
);

select is(
  (
    select count(*)::text
    from (
      values
        ('contacts'),
        ('sources'),
        ('unitgroups'),
        ('flowproperties'),
        ('flows'),
        ('processes'),
        ('lifecyclemodels')
    ) as dataset_tables(table_name)
    where not has_table_privilege('authenticated', format('public.%I', table_name), 'INSERT')
      and not has_table_privilege('authenticated', format('public.%I', table_name), 'UPDATE')
      and not has_table_privilege('authenticated', format('public.%I', table_name), 'DELETE')
  ),
  '7',
  'authenticated role has no raw insert, update, or delete grants on dataset tables after revoke'
);

select is(
  (
    select count(*)::text
    from (
      values
        ('contacts'),
        ('sources'),
        ('unitgroups'),
        ('flowproperties'),
        ('flows'),
        ('processes'),
        ('lifecyclemodels')
    ) as dataset_tables(table_name)
    where not has_table_privilege('anon', format('public.%I', table_name), 'INSERT')
      and not has_table_privilege('anon', format('public.%I', table_name), 'UPDATE')
      and not has_table_privilege('anon', format('public.%I', table_name), 'DELETE')
  ),
  '7',
  'anon role has no raw insert, update, or delete grants on dataset tables after revoke'
);

select * from finish();
rollback;
