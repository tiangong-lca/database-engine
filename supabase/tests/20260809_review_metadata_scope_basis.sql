begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(8);

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
  '19800000-0000-0000-0000-000000000001',
  100,
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
  '19800000-0000-0000-0000-000000000001',
  100,
  null,
  true,
  '[]'::jsonb
);

with root_items as (
  select pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'item_kind', 'root',
      'target_table', 'processes',
      'data_id', '49800000-0000-0000-0000-000000000001',
      'data_version', '01.01.000',
      'submitted_revision_checksum', pg_catalog.repeat('a', 64),
      'target_owner_id', '19800000-0000-0000-0000-000000000001'
    )
  ) as items
),
root_history as (
  select pg_catalog.jsonb_build_object(
    'schema_version', 'review_scope.v1',
    'current_version', 1,
    'snapshots', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'version_no', 1,
        'scope_basis', 'submitted',
        'root_revision_checksum', pg_catalog.repeat('a', 64),
        'scope_checksum', private.review_scope_checksum_v1(root_items.items),
        'created_by', '19800000-0000-0000-0000-000000000001',
        'created_at', pg_catalog.to_jsonb(pg_catalog.now()),
        'items', root_items.items
      )
    )
  ) as scope_history
  from root_items
)
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
  target_owner_id,
  scope_schema_version,
  scope_history
)
select
  '59800000-0000-0000-0000-000000000001',
  '49800000-0000-0000-0000-000000000001',
  '01.01.000',
  1,
  '["19800000-0000-0000-0000-000000000001"]'::jsonb,
  '{"logs":[]}'::jsonb,
  'root',
  'processes',
  pg_catalog.repeat('a', 64),
  '19800000-0000-0000-0000-000000000001',
  'review_scope.v1',
  root_history.scope_history
from root_history;

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

select is(
  (
    select (scope_history->>'current_version')::integer
    from private.reviews
    where id = '59800000-0000-0000-0000-000000000001'
  ),
  2,
  'Reviewer metadata appends exactly one scope snapshot'
);

select is(
  (
    select private.review_scope_current_snapshot_v1(scope_history)->>'scope_basis'
    from private.reviews
    where id = '59800000-0000-0000-0000-000000000001'
  ),
  'review_metadata',
  'appended snapshot uses the canonical review_metadata scope basis'
);

select is(
  (
    select pg_catalog.jsonb_array_length(
      private.review_scope_current_snapshot_v1(scope_history)->'items'
    )
    from private.reviews
    where id = '59800000-0000-0000-0000-000000000001'
  ),
  3,
  'current scope retains the root and adds both Reviewer metadata references'
);

select is(
  (
    select count(*)::integer
    from private.reviews as root_review
    cross join lateral pg_catalog.jsonb_array_elements(
      private.review_scope_current_snapshot_v1(root_review.scope_history)->'items'
    ) as item(value)
    where root_review.id = '59800000-0000-0000-0000-000000000001'
      and item.value->>'item_kind' = 'reference'
      and item.value->>'relation_type' = 'reviewer_metadata'
      and item.value->>'introduced_by' = 'reviewer_metadata'
  ),
  2,
  'item-level provenance remains reviewer_metadata for both references'
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
    select pg_catalog.jsonb_array_length(
      result #> '{data,affected_datasets}'
    )
    from review_metadata_result
  ),
  2,
  'command response reports both affected datasets'
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

select * from finish();
rollback;
