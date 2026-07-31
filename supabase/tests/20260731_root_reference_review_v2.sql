begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(14);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password,
  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
  created_at, updated_at, is_sso_user, is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    '19000000-0000-0000-0000-000000000001',
    'authenticated', 'authenticated', 'root-owner@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"root-owner@example.com","display_name":"Root Owner"}'::jsonb,
    now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '19000000-0000-0000-0000-000000000002',
    'authenticated', 'authenticated', 'reviewer@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"reviewer@example.com","display_name":"Reviewer"}'::jsonb,
    now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '19000000-0000-0000-0000-000000000003',
    'authenticated', 'authenticated', 'review-admin@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"review-admin@example.com","display_name":"Review Admin"}'::jsonb,
    now(), now(), false, false
  );

insert into public.users (id, raw_user_meta_data)
values
  (
    '19000000-0000-0000-0000-000000000001',
    '{"email":"root-owner@example.com","display_name":"Root Owner"}'
  ),
  (
    '19000000-0000-0000-0000-000000000002',
    '{"email":"reviewer@example.com","display_name":"Reviewer"}'
  ),
  (
    '19000000-0000-0000-0000-000000000003',
    '{"email":"review-admin@example.com","display_name":"Review Admin"}'
  );

insert into public.teams (id, json, rank, is_public)
values
  (
    '29000000-0000-0000-0000-000000000001',
    '{"name":"Root Review Team"}',
    1,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '{"name":"System Team"}',
    0,
    false
  )
on conflict (id) do nothing;

insert into public.roles (user_id, team_id, role)
values
  (
    '19000000-0000-0000-0000-000000000001',
    '29000000-0000-0000-0000-000000000001',
    'owner'
  ),
  (
    '19000000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000000',
    'review-member'
  ),
  (
    '19000000-0000-0000-0000-000000000003',
    '00000000-0000-0000-0000-000000000000',
    'review-admin'
  );

alter table public.contacts disable trigger "contacts_json_sync_trigger";
alter table public.contacts disable trigger "contact_dataset_extraction_trigger_insert";
alter table public.contacts disable trigger "contact_dataset_extraction_trigger_update";
alter table public.contacts disable trigger "contact_embedding_ft_on_extract_md_update";

insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, team_id,
  rule_verification
)
values (
  '39000000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Root Contact"}]}}}}',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Root Contact"}]}}}}',
  '19000000-0000-0000-0000-000000000001',
  0,
  '29000000-0000-0000-0000-000000000001',
  true
);

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000001',
  true
);

create temporary table review_v2_result as
select public.cmd_review_submit_v2(
  'contacts',
  '39000000-0000-0000-0000-000000000001',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;

select ok((select (result->>'ok')::boolean from review_v2_result),
  'Contact uses the unified v2 submit command');
select is((select result #>> '{data,reviewKind}' from review_v2_result),
  'root', 'normal submission creates a Root Review');
select is((select state_code from public.contacts
  where id = '39000000-0000-0000-0000-000000000001'), 20,
  'submitted root becomes read-only state 20');
select is((select count(*)::integer from public.reviews
  where review_kind = 'root'), 1, 'one Root Review is created');
select is((select jsonb_array_length(scope_history->'snapshots')
  from public.reviews where review_kind = 'root'), 1,
  'Root Review receives its initial immutable scope snapshot');
select is((select cardinality(current_reference_review_ids)
  from public.reviews where review_kind = 'root'), 0,
  'a Contact with no references has an empty child review list');
select matches((select submitted_revision_checksum from public.reviews
  where review_kind = 'root'), '^[a-f0-9]{64}$',
  'submitted checksum matches the Gate checksum format');

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000003',
  true
);
select ok((
  public.cmd_review_assign_reviewers(
    (select id from public.reviews where review_kind = 'root'),
    '["19000000-0000-0000-0000-000000000002"]'::jsonb,
    now() + interval '7 days',
    '{}'::jsonb
  )->>'ok'
)::boolean, 'Review Admin assigns the simple Root Review');

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000002',
  true
);
select ok((
  public.cmd_simple_review_submit_decision(
    (select id from public.reviews where review_kind = 'root'),
    'approve',
    null,
    '{}'::jsonb
  )->>'ok'
)::boolean, 'Reviewer approval requires no opinion payload');
select is((select state_code from public.comments limit 1), 1,
  'Reviewer approval is stored as comment state 1');

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000003',
  true
);
select ok((
  public.cmd_review_finalize_approve(
    (select id from public.reviews where review_kind = 'root'),
    '{}'::jsonb
  )->>'ok'
)::boolean, 'Review Admin can finalize the completed Root Review');
select is((select state_code from public.contacts
  where id = '39000000-0000-0000-0000-000000000001'), 100,
  'final approval moves the exact dataset to Open Data state 100');
select is((select state_code from public.comments limit 1), 2,
  'final approval archives Reviewer comments as state 2');

select throws_ok(
  $$update public.contacts
    set rule_verification = false
    where id = '39000000-0000-0000-0000-000000000001'$$,
  '55000',
  'APPROVED_DATASET_IMMUTABLE',
  'published dataset business fields are immutable'
);

select * from finish();
rollback;
