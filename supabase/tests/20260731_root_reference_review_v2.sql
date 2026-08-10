begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(23);

select is((
  select count(*)::integer
  from pg_catalog.pg_indexes
  where schemaname = 'public'
    and tablename = 'notifications'
    and indexname = 'notifications_review_event_recipient_uidx'
), 0, 'latest-event notifications do not retain the superseded per-event unique index');

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

insert into private.users (id, raw_user_meta_data)
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

insert into private.teams (id, json, rank, is_public)
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

insert into private.roles (user_id, team_id, role)
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
select api.cmd_review_submit_v2(
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
select is((select count(*)::integer from private.reviews
  where review_kind = 'root'), 1, 'one Root Review is created');
select ok(
  not exists (
    select 1 from information_schema.columns
    where table_schema = 'private' and table_name = 'reviews'
      and column_name = 'scope_history'
  ),
  'Root Reviews do not persist an immutable relationship scope snapshot'
);
select is(
  (
    select count(*)::integer
    from private.review_derive_current_references_v1(array[
      (select id from private.reviews where review_kind = 'root')
    ])
  ),
  0,
  'a Contact with no current JSON/comment references has no derived child review'
);
select matches((select submitted_revision_checksum from private.reviews
  where review_kind = 'root'), '^[a-f0-9]{64}$',
  'submitted checksum matches the Gate checksum format');

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000003',
  true
);
select ok((
  api.cmd_review_assign_reviewers(
    (select id from private.reviews where review_kind = 'root'),
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
  api.cmd_simple_review_submit_decision(
    (select id from private.reviews where review_kind = 'root'),
    'approve',
    null,
    '{}'::jsonb
  )->>'ok'
)::boolean, 'Reviewer approval requires no opinion payload');
select is((select state_code from private.comments limit 1), 1,
  'Reviewer approval is stored as comment state 1');

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000003',
  true
);
select ok((
  api.cmd_review_finalize_approve(
    (select id from private.reviews where review_kind = 'root'),
    '{}'::jsonb
  )->>'ok'
)::boolean, 'Review Admin can finalize the completed Root Review');
select is((select state_code from public.contacts
  where id = '39000000-0000-0000-0000-000000000001'), 100,
  'final approval moves the exact dataset to Open Data state 100');
select is((select state_code from private.comments limit 1), 2,
  'final approval archives Reviewer comments as state 2');

select throws_ok(
  $$update public.contacts
    set rule_verification = false
    where id = '39000000-0000-0000-0000-000000000001'$$,
  '55000',
  'APPROVED_DATASET_IMMUTABLE',
  'published dataset business fields are immutable'
);

insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, team_id,
  rule_verification
)
values (
  '39000000-0000-0000-0000-000000000002',
  '01.00.000',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Rejected Contact"}]}}}}',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Rejected Contact"}]}}}}',
  '19000000-0000-0000-0000-000000000001',
  0,
  '29000000-0000-0000-0000-000000000001',
  true
);

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000001',
  true
);

select api.cmd_review_submit_v2(
  'contacts',
  '39000000-0000-0000-0000-000000000002',
  '01.00.000',
  null,
  '{}'::jsonb
);

select set_config(
  'request.jwt.claim.sub',
  '19000000-0000-0000-0000-000000000003',
  true
);

select private.review_notify_event_v1(
  'root_entered_review',
  (select id from private.reviews
    where review_kind = 'root'
      and data_id = '39000000-0000-0000-0000-000000000002'),
  '19000000-0000-0000-0000-000000000001',
  '19000000-0000-0000-0000-000000000003',
  'contacts',
  '39000000-0000-0000-0000-000000000002',
  '01.00.000',
  (select id from private.reviews
    where review_kind = 'root'
      and data_id = '39000000-0000-0000-0000-000000000002'),
  1,
  null
);

create temporary table review_v2_notification_before_reject as
select id
from private.notifications
where recipient_user_id = '19000000-0000-0000-0000-000000000001'
  and sender_user_id = '19000000-0000-0000-0000-000000000003'
  and type = 'review_event'
  and dataset_type = 'contacts'
  and dataset_id = '39000000-0000-0000-0000-000000000002'
  and dataset_version = '01.00.000';

select ok((
  api.cmd_review_finalize_reject(
    (select id from private.reviews
      where review_kind = 'root'
        and data_id = '39000000-0000-0000-0000-000000000002'),
    'notification identity regression',
    '{}'::jsonb
  )->>'ok'
)::boolean, 'Review Admin rejection succeeds after an earlier review event');

select is((
  select state_code
  from public.contacts
  where id = '39000000-0000-0000-0000-000000000002'
    and version = '01.00.000'
), 0, 'successful rejection atomically restores the dataset to editable state 0');

select is((
  select state_code
  from private.reviews
  where review_kind = 'root'
    and data_id = '39000000-0000-0000-0000-000000000002'
), -1, 'successful rejection keeps the review transition in the same transaction');

select is((
  select count(*)::integer
  from private.notifications
  where recipient_user_id = '19000000-0000-0000-0000-000000000001'
    and sender_user_id = '19000000-0000-0000-0000-000000000003'
    and type = 'review_event'
    and dataset_type = 'contacts'
    and dataset_id = '39000000-0000-0000-0000-000000000002'
    and dataset_version = '01.00.000'
), 1, 'review lifecycle notifications retain one latest-event row per existing identity');

select is((
  select id
  from private.notifications
  where recipient_user_id = '19000000-0000-0000-0000-000000000001'
    and sender_user_id = '19000000-0000-0000-0000-000000000003'
    and type = 'review_event'
    and dataset_type = 'contacts'
    and dataset_id = '39000000-0000-0000-0000-000000000002'
    and dataset_version = '01.00.000'
), (select id from review_v2_notification_before_reject),
  'review lifecycle notification updates preserve the existing row identity');

select is((
  select json->>'event_type'
  from private.notifications
  where recipient_user_id = '19000000-0000-0000-0000-000000000001'
    and sender_user_id = '19000000-0000-0000-0000-000000000003'
    and type = 'review_event'
    and dataset_type = 'contacts'
    and dataset_id = '39000000-0000-0000-0000-000000000002'
    and dataset_version = '01.00.000'
), 'root_rejected', 'latest-event notification exposes the rejection event');

select is((
  select json->>'reason_code'
  from private.notifications
  where recipient_user_id = '19000000-0000-0000-0000-000000000001'
    and sender_user_id = '19000000-0000-0000-0000-000000000003'
    and type = 'review_event'
    and dataset_type = 'contacts'
    and dataset_id = '39000000-0000-0000-0000-000000000002'
    and dataset_version = '01.00.000'
), 'ADMIN_REJECTED', 'latest-event notification retains the rejection reason code');

select private.review_notify_event_v1(
  'root_rejected',
  (select id from private.reviews
    where review_kind = 'root'
      and data_id = '39000000-0000-0000-0000-000000000002'),
  '19000000-0000-0000-0000-000000000001',
  '19000000-0000-0000-0000-000000000003',
  'contacts',
  '39000000-0000-0000-0000-000000000002',
  '01.00.000',
  (select id from private.reviews
    where review_kind = 'root'
      and data_id = '39000000-0000-0000-0000-000000000002'),
  1,
  'ADMIN_REJECTED'
);

select is((
  select count(*)::integer
  from private.notifications
  where recipient_user_id = '19000000-0000-0000-0000-000000000001'
    and sender_user_id = '19000000-0000-0000-0000-000000000003'
    and type = 'review_event'
    and dataset_type = 'contacts'
    and dataset_id = '39000000-0000-0000-0000-000000000002'
    and dataset_version = '01.00.000'
), 1, 'replaying the latest review event remains idempotent');

select * from finish();
rollback;
