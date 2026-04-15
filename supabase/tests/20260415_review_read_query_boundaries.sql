begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(22);

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
    '15000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'review-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000001","email":"review-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'assigned-reviewer@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000002","email":"assigned-reviewer@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000003',
    'authenticated',
    'authenticated',
    'peer-reviewer@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000003","email":"peer-reviewer@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000004',
    'authenticated',
    'authenticated',
    'unassigned-reviewer@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000004","email":"unassigned-reviewer@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000005',
    'authenticated',
    'authenticated',
    'review-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000005","email":"review-admin@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000006',
    'authenticated',
    'authenticated',
    'outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000006","email":"outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000007',
    'authenticated',
    'authenticated',
    'team-review-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000007","email":"team-review-admin@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000008',
    'authenticated',
    'authenticated',
    'former-reviewer@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000008","email":"former-reviewer@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data)
values
  ('15000000-0000-0000-0000-000000000001', '{"email":"review-owner@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000002', '{"email":"assigned-reviewer@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000003', '{"email":"peer-reviewer@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000004', '{"email":"unassigned-reviewer@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000005', '{"email":"review-admin@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000006', '{"email":"outsider@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000007', '{"email":"team-review-admin@example.com"}'::jsonb),
  ('15000000-0000-0000-0000-000000000008', '{"email":"former-reviewer@example.com"}'::jsonb);

insert into public.teams (id, json, rank, is_public)
values (
  '25000000-0000-0000-0000-000000000001',
  '{"title":[{"@xml:lang":"en","#text":"Review Scope Team"}]}'::jsonb,
  1,
  false
);

insert into public.roles (user_id, team_id, role)
values
  ('15000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000000', 'review-member'),
  ('15000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000000', 'review-member'),
  ('15000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000000', 'review-member'),
  ('15000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000000', 'review-admin'),
  ('15000000-0000-0000-0000-000000000007', '25000000-0000-0000-0000-000000000001', 'review-admin'),
  ('15000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000000', 'review-member');

insert into public.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json,
  created_at,
  modified_at
)
values
  (
    '55000000-0000-0000-0000-000000000001',
    '65000000-0000-0000-0000-000000000001',
    '01.00.000',
    1,
    '["15000000-0000-0000-0000-000000000002","15000000-0000-0000-0000-000000000003"]'::jsonb,
    '{
      "user": { "id": "15000000-0000-0000-0000-000000000001" },
      "data": { "id": "65000000-0000-0000-0000-000000000001", "version": "01.00.000" }
    }'::jsonb,
    now() - interval '3 days',
    now() - interval '1 day'
  ),
  (
    '55000000-0000-0000-0000-000000000002',
    '65000000-0000-0000-0000-000000000002',
    '01.00.000',
    2,
    '["15000000-0000-0000-0000-000000000002"]'::jsonb,
    '{
      "user": { "id": "15000000-0000-0000-0000-000000000001" },
      "data": { "id": "65000000-0000-0000-0000-000000000002", "version": "01.00.000" }
    }'::jsonb,
    now() - interval '4 days',
    now() - interval '2 days'
  ),
  (
    '55000000-0000-0000-0000-000000000003',
    '65000000-0000-0000-0000-000000000003',
    '01.00.000',
    -1,
    '["15000000-0000-0000-0000-000000000002"]'::jsonb,
    '{
      "user": { "id": "15000000-0000-0000-0000-000000000001" },
      "data": { "id": "65000000-0000-0000-0000-000000000003", "version": "01.00.000" }
    }'::jsonb,
    now() - interval '5 days',
    now() - interval '3 days'
  ),
  (
    '55000000-0000-0000-0000-000000000004',
    '65000000-0000-0000-0000-000000000004',
    '01.00.000',
    2,
    '[]'::jsonb,
    '{
      "user": { "id": "15000000-0000-0000-0000-000000000001" },
      "data": { "id": "65000000-0000-0000-0000-000000000004", "version": "01.00.000" }
    }'::jsonb,
    now() - interval '6 days',
    now() - interval '4 days'
  );

insert into public.comments (
  review_id,
  reviewer_id,
  json,
  state_code,
  created_at,
  modified_at
)
values
  (
    '55000000-0000-0000-0000-000000000001',
    '15000000-0000-0000-0000-000000000002',
    '{"comment":"assigned reviewer draft"}'::json,
    0,
    now() - interval '2 days',
    now() - interval '2 days'
  ),
  (
    '55000000-0000-0000-0000-000000000001',
    '15000000-0000-0000-0000-000000000003',
    '{"comment":"peer reviewer finished"}'::json,
    1,
    now() - interval '36 hours',
    now() - interval '36 hours'
  ),
  (
    '55000000-0000-0000-0000-000000000002',
    '15000000-0000-0000-0000-000000000002',
    '{"comment":"assigned reviewer approved"}'::json,
    1,
    now() - interval '3 days',
    now() - interval '3 days'
  ),
  (
    '55000000-0000-0000-0000-000000000003',
    '15000000-0000-0000-0000-000000000002',
    '{"comment":"assigned reviewer rejected"}'::json,
    -1,
    now() - interval '4 days',
    now() - interval '4 days'
  ),
  (
    '55000000-0000-0000-0000-000000000004',
    '15000000-0000-0000-0000-000000000008',
    '{"comment":"former reviewer history"}'::json,
    1,
    now() - interval '5 days',
    now() - interval '5 days'
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000006', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id in (
      '55000000-0000-0000-0000-000000000001',
      '55000000-0000-0000-0000-000000000002',
      '55000000-0000-0000-0000-000000000003'
    )
  ),
  '0',
  'outsider cannot read submitted, reviewed, or rejected review rows directly'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000007', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id = '55000000-0000-0000-0000-000000000001'
  ),
  '0',
  'team-scoped review-admin cannot use a non-system role to read review rows'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000001', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id in (
      '55000000-0000-0000-0000-000000000001',
      '55000000-0000-0000-0000-000000000002',
      '55000000-0000-0000-0000-000000000003',
      '55000000-0000-0000-0000-000000000004'
    )
  ),
  '4',
  'review owner can read all of their own review rows regardless of state'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000002', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id in (
      '55000000-0000-0000-0000-000000000001',
      '55000000-0000-0000-0000-000000000002',
      '55000000-0000-0000-0000-000000000003',
      '55000000-0000-0000-0000-000000000004'
    )
  ),
  '3',
  'assigned review-member can only read their assigned review rows'
);

select is(
  (
    select count(*)::text
    from public.comments
    where review_id = '55000000-0000-0000-0000-000000000001'
  ),
  '1',
  'review-member can only read their own comment rows on an assigned review'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_member_queue_items('pending', 1, 10, 'modified_at', 'desc')
  ),
  '1',
  'member pending queue RPC only returns the actor pending rows'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_member_queue_items('reviewed', 1, 10, 'modified_at', 'desc')
  ),
  '1',
  'member reviewed queue RPC only returns the actor reviewed rows'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_member_queue_items('reviewer-rejected', 1, 10, 'modified_at', 'desc')
  ),
  '1',
  'member rejected queue RPC only returns the actor rejected rows'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_comment_items('55000000-0000-0000-0000-000000000001', 'all')
  ),
  '1',
  'review-member comment query RPC still collapses all-scope requests down to the actor own rows'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000008', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id = '55000000-0000-0000-0000-000000000004'
  ),
  '1',
  'former review participant can still read review history via their own comment participation'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000004', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id = '55000000-0000-0000-0000-000000000001'
  ),
  '0',
  'unassigned review-member cannot read another review row'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000005', true);

select is(
  (
    select count(*)::text
    from public.reviews
    where id in (
      '55000000-0000-0000-0000-000000000001',
      '55000000-0000-0000-0000-000000000002',
      '55000000-0000-0000-0000-000000000003',
      '55000000-0000-0000-0000-000000000004'
    )
  ),
  '4',
  'system review-admin can read every review row'
);

select is(
  (
    select count(*)::text
    from public.comments
    where review_id = '55000000-0000-0000-0000-000000000001'
  ),
  '2',
  'system review-admin can read all comments for a review'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_admin_queue_items('assigned', 1, 10, 'modified_at', 'desc')
  ),
  '1',
  'admin queue RPC returns the assigned review set'
);

select is(
  (
    select coalesce(jsonb_agg(value order by value)::text, '[]')
    from jsonb_array_elements(
      (
        select comment_state_codes
        from public.qry_review_get_admin_queue_items('assigned', 1, 10, 'modified_at', 'desc')
        limit 1
      )
    ) as value
  ),
  '[0, 1]',
  'admin queue RPC includes aggregated reviewer state codes for progress rendering'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_comment_items('55000000-0000-0000-0000-000000000001', 'all')
  ),
  '2',
  'admin comment query RPC can read every comment row for the review'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000001', true);

select is(
  (
    select count(*)::text
    from public.comments
    where review_id = '55000000-0000-0000-0000-000000000001'
  ),
  '2',
  'review owner can read all comments for their own review'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_items(
      array['55000000-0000-0000-0000-000000000001'::uuid],
      null,
      null,
      null
    )
  ),
  '1',
  'generic review item RPC returns a directly addressed review for its owner'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_items(
      null,
      '65000000-0000-0000-0000-000000000004'::uuid,
      '01.00.000',
      null
    )
  ),
  '1',
  'generic review item RPC can resolve review history by data id and version for logs/detail views'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_comment_items('55000000-0000-0000-0000-000000000001', 'all')
  ),
  '2',
  'owner comment query RPC can read every comment row for the review'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000006', true);

select is(
  (
    select count(*)::text
    from public.comments
    where review_id = '55000000-0000-0000-0000-000000000001'
  ),
  '0',
  'outsider cannot read review comments'
);

select is(
  (
    select count(*)::text
    from public.qry_review_get_items(
      array['55000000-0000-0000-0000-000000000001'::uuid],
      null,
      null,
      null
    )
  ),
  '0',
  'generic review item RPC returns nothing for an unauthorized actor'
);

select * from finish();

rollback;
