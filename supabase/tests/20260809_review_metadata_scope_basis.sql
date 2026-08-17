begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(22);

create or replace function pg_temp.disable_trigger_if_exists(
  p_table regclass,
  p_trigger name
)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_catalog.pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute pg_catalog.format(
      'alter table %s disable trigger %I',
      p_table,
      p_trigger
    );
  end if;
end;
$$;

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
values (
  '00000000-0000-0000-0000-000000000000',
  '19800000-0000-0000-0000-000000000001',
  'authenticated',
  'authenticated',
  'metadata-reviewer@example.com',
  'test-password-hash',
  now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"email":"metadata-reviewer@example.com","display_name":"Metadata Reviewer"}'::jsonb,
  now(),
  now(),
  false,
  false
);

insert into private.users (id, raw_user_meta_data)
values (
  '19800000-0000-0000-0000-000000000001',
  '{"email":"metadata-reviewer@example.com","display_name":"Metadata Reviewer"}'::jsonb
)
on conflict (id) do update
set raw_user_meta_data = excluded.raw_user_meta_data;

insert into private.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System Team"}',
  0,
  false
)
on conflict (id) do nothing;

insert into private.roles (user_id, team_id, role)
values (
  '19800000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'review-admin'
);

select pg_temp.disable_trigger_if_exists(
  'public.sources'::regclass,
  'sources_json_sync_trigger'
);
select pg_temp.disable_trigger_if_exists(
  'public.sources'::regclass,
  'source_dataset_extraction_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.sources'::regclass,
  'source_dataset_extraction_trigger_update'
);
select pg_temp.disable_trigger_if_exists(
  'public.contacts'::regclass,
  'contacts_json_sync_trigger'
);
select pg_temp.disable_trigger_if_exists(
  'public.contacts'::regclass,
  'contact_dataset_extraction_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.contacts'::regclass,
  'contact_dataset_extraction_trigger_update'
);

insert into public.sources (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  reviews
)
values (
  '39800000-0000-0000-0000-000000000001',
  '01.01.000',
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"zh","#text":"审核员-来源"}]}}}}'::jsonb,
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"zh","#text":"审核员-来源"}]}}}}'::json,
  null,
  100,
  null,
  true,
  '[]'::jsonb
), (
  '39800000-0000-0000-0000-000000000003',
  '01.01.000',
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"zh","#text":"审核草稿-待分配来源"}]}}}}'::jsonb,
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"zh","#text":"审核草稿-待分配来源"}]}}}}'::json,
  '19800000-0000-0000-0000-000000000001',
  20,
  null,
  true,
  '[]'::jsonb
);

insert into public.contacts (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  reviews
)
values (
  '39800000-0000-0000-0000-000000000002',
  '01.01.000',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"zh","#text":"审核员-联系人"}]}}}}'::jsonb,
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"zh","#text":"审核员-联系人"}]}}}}'::json,
  null,
  100,
  null,
  true,
  '[]'::jsonb
);

insert into private.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json,
  review_kind,
  target_table,
  submitted_revision_checksum,
  target_owner_id
)
values (
  '59800000-0000-0000-0000-000000000001',
  '49800000-0000-0000-0000-000000000001',
  '01.01.000',
  1,
  '["19800000-0000-0000-0000-000000000001"]'::jsonb,
  '{"logs":[]}'::jsonb,
  'root',
  'processes',
  pg_catalog.repeat('a', 64),
  '19800000-0000-0000-0000-000000000001'
), (
  '59800000-0000-0000-0000-000000000002',
  '49800000-0000-0000-0000-000000000002',
  '01.01.000',
  1,
  '["19800000-0000-0000-0000-000000000001"]'::jsonb,
  '{"logs":[]}'::jsonb,
  'root',
  'processes',
  pg_catalog.repeat('b', 64),
  '19800000-0000-0000-0000-000000000001'
);

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '19800000-0000-0000-0000-000000000001',
  true
);

create temporary table review_metadata_result as
select api.cmd_review_submit_comment(
  '59800000-0000-0000-0000-000000000001',
  '{
    "modellingAndValidation": {
      "complianceDeclarations": {
        "compliance": [{
          "common:referenceToComplianceSystem": {
            "@refObjectId": "39800000-0000-0000-0000-000000000001",
            "@type": "source data set",
            "@uri": "../sources/39800000-0000-0000-0000-000000000001.xml",
            "@version": "01.01.000"
          }
        }]
      },
      "validation": {
        "review": [{
          "common:referenceToNameOfReviewerAndInstitution": {
            "@refObjectId": "39800000-0000-0000-0000-000000000002",
            "@type": "contact data set",
            "@uri": "../contacts/39800000-0000-0000-0000-000000000002.xml",
            "@version": "01.01.000"
          }
        }]
      }
    }
  }'::jsonb,
  1,
  '{}'::jsonb
) as result;

select ok(
  (select (result->>'ok')::boolean from review_metadata_result),
  'Reviewer metadata comment submission succeeds when it introduces references'
);

create temporary table review_metadata_draft_result as
select api.cmd_review_save_comment_draft(
  '59800000-0000-0000-0000-000000000002',
  '{
    "modellingAndValidation": {
      "complianceDeclarations": {
        "compliance": [{
          "common:referenceToComplianceSystem": {
            "@refObjectId": "39800000-0000-0000-0000-000000000003",
            "@type": "source data set",
            "@uri": "../sources/39800000-0000-0000-0000-000000000003.xml",
            "@version": "01.01.000"
          }
        }]
      }
    }
  }'::jsonb,
  '{}'::jsonb
) as result;

select ok(
  (select (result->>'ok')::boolean from review_metadata_draft_result),
  'Reviewer metadata draft save succeeds when it introduces a reference'
);

select is(
  (
    select state_code
    from private.comments
    where review_id = '59800000-0000-0000-0000-000000000002'
      and reviewer_id = '19800000-0000-0000-0000-000000000001'
  ),
  0,
  'Reviewer metadata draft remains an editable state-zero Comment'
);

select is(
  (
    select count(*)::integer
    from private.reviews
    where review_kind = 'reference'
      and target_table = 'sources'
      and data_id = '39800000-0000-0000-0000-000000000003'
      and pg_catalog.btrim(data_version::text) = '01.01.000'
  ),
  0,
  'temporarily stored metadata creates no Reference Review'
);

select ok(
  not (
    select reviews @> '[{"id":"59800000-0000-0000-0000-000000000002"}]'
    from public.sources
    where id = '39800000-0000-0000-0000-000000000003'
      and version = '01.01.000'
  ),
  'temporarily stored metadata appends no Root candidate hint'
);

update private.roles
set role = 'review-member'
where user_id = '19800000-0000-0000-0000-000000000001'
  and team_id = '00000000-0000-0000-0000-000000000000';

select is(
  (
    select count(*)::integer
    from api.qry_review_get_member_root_queue_items_v2(
      'reviewed', 1, 10, 'modified_at', 'descend'
    )
  ),
  1,
  'reviewed Member queue ignores the unrelated state-zero draft Root'
);

update private.roles
set role = 'review-admin'
where user_id = '19800000-0000-0000-0000-000000000001'
  and team_id = '00000000-0000-0000-0000-000000000000';

create temporary table review_metadata_draft_submit_result as
select api.cmd_review_submit_comment(
  '59800000-0000-0000-0000-000000000002',
  '{
    "modellingAndValidation": {
      "complianceDeclarations": {
        "compliance": [{
          "common:referenceToComplianceSystem": {
            "@refObjectId": "39800000-0000-0000-0000-000000000003",
            "@type": "source data set",
            "@uri": "../sources/39800000-0000-0000-0000-000000000003.xml",
            "@version": "01.01.000"
          }
        }]
      }
    }
  }'::jsonb,
  1,
  '{}'::jsonb
) as result;

select ok(
  (select (result->>'ok')::boolean
   from review_metadata_draft_submit_result),
  'formally saving the metadata Comment succeeds'
);

select is(
  (
    select state_code
    from private.comments
    where review_id = '59800000-0000-0000-0000-000000000002'
      and reviewer_id = '19800000-0000-0000-0000-000000000001'
  ),
  1,
  'formally saved metadata Comment leaves the draft state'
);

select is(
  (
    select state_code
    from private.reviews
    where review_kind = 'reference'
      and target_table = 'sources'
      and data_id = '39800000-0000-0000-0000-000000000003'
      and pg_catalog.btrim(data_version::text) = '01.01.000'
  ),
  0,
  'formally saved metadata creates an unassigned Reference Review'
);

select ok(
  (
    select reviews @> '[{"id":"59800000-0000-0000-0000-000000000002"}]'
    from public.sources
    where id = '39800000-0000-0000-0000-000000000003'
      and version = '01.01.000'
  ),
  'formally saved metadata stores the parent Root candidate hint'
);

select is(
  (
    select id
    from api.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where id = '59800000-0000-0000-0000-000000000002'
  ),
  '59800000-0000-0000-0000-000000000002'::uuid,
  'formally saved unassigned Reference Review is grouped under its Root'
);

select is(
  (
    select root_matches_status
    from api.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where id = '59800000-0000-0000-0000-000000000002'
  ),
  false,
  'formally saved Root is included by its unassigned child task'
);

select ok(
  not exists (
    select 1 from information_schema.columns
    where table_schema = 'private' and table_name = 'reviews'
      and column_name = 'scope_history'
  ),
  'Reviewer metadata persists no relationship snapshot'
);

select is(
  (
    select count(*)::integer
    from private.review_derive_current_references_v1(array[
      '59800000-0000-0000-0000-000000000001'::uuid
    ])
  ),
  2,
  'current Comment JSON dynamically derives both metadata references'
);

select is(
  (
    select count(*)::integer
    from (
      select reviews from public.sources
      where id = '39800000-0000-0000-0000-000000000001'
      union all
      select reviews from public.contacts
      where id = '39800000-0000-0000-0000-000000000002'
    ) as target
    where target.reviews @> '[{"id":"59800000-0000-0000-0000-000000000001"}]'
  ),
  0,
  'approved metadata targets remain immutable without active candidate hints'
);

select ok(
  not exists (
    select 1
    from (
      select reviews from public.sources
      where id = '39800000-0000-0000-0000-000000000001'
      union all
      select reviews from public.contacts
      where id = '39800000-0000-0000-0000-000000000002'
    ) as target
    cross join lateral pg_catalog.jsonb_array_elements(target.reviews) as entry(value)
    where entry.value ? 'reference_review_id'
      or entry.value ? 'scope_history'
  ),
  'business candidate entries contain no Reference Review mapping data'
);

select is(
  (
    select count(*)::integer
    from private.reviews
    where review_kind = 'reference'
      and data_id in (
        '39800000-0000-0000-0000-000000000001',
        '39800000-0000-0000-0000-000000000002'
      )
  ),
  2,
  'Source and Contact each receive a reusable Reference Review'
);

select is(
  (
    select count(*)::integer
    from private.reviews
    where review_kind = 'reference'
      and data_id in (
        '39800000-0000-0000-0000-000000000001',
        '39800000-0000-0000-0000-000000000002'
      )
      and state_code = 2
      and target_owner_id is null
  ),
  2,
  'approved ownerless metadata references are represented by approved Reference Reviews'
);

select is(
  (
    select pg_catalog.jsonb_array_length(
      result #> '{data,affected_datasets}'
    )
    from review_metadata_result
  ),
  2,
  'command response reports both affected datasets'
);

select ok(
  not exists (
    select 1
    from private.command_audit_log as audit
    cross join lateral pg_catalog.jsonb_array_elements(
      coalesce(audit.payload->'affected_datasets', '[]'::jsonb)
    ) as affected(value)
    where audit.command = 'cmd_review_submit_comment'
      and audit.target_id = '59800000-0000-0000-0000-000000000001'
      and affected.value ? 'reference_review_id'
  ),
  'command audit preserves command facts without persisting Root/Reference mappings'
);

select is(
  (
    select state_code
    from private.comments
    where review_id = '59800000-0000-0000-0000-000000000001'
      and reviewer_id = '19800000-0000-0000-0000-000000000001'
  ),
  1,
  'Reviewer approval comment is stored after scope expansion'
);

update private.comments
set state_code = -2
where review_id = '59800000-0000-0000-0000-000000000001'
  and reviewer_id = '19800000-0000-0000-0000-000000000001';

select is(
  (
    select count(*)::integer
    from private.review_derive_current_references_v1(array[
      '59800000-0000-0000-0000-000000000001'::uuid
    ])
  ),
  0,
  'revoking the Comment removes both current relationships without cleanup writes'
);

select * from finish();
rollback;
