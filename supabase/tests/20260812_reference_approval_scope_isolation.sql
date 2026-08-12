begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(7);

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
  '19812000-0000-0000-0000-000000000001',
  'authenticated',
  'authenticated',
  'reference-scope-admin@example.com',
  'test-password-hash',
  now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"email":"reference-scope-admin@example.com","display_name":"Reference Scope Admin"}'::jsonb,
  now(),
  now(),
  false,
  false
);

insert into private.users (id, raw_user_meta_data)
values (
  '19812000-0000-0000-0000-000000000001',
  '{"email":"reference-scope-admin@example.com","display_name":"Reference Scope Admin"}'::jsonb
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
  '19812000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'review-admin'
);

alter table public.sources disable trigger "sources_json_sync_trigger";
alter table public.sources disable trigger "source_dataset_extraction_trigger_insert";
alter table public.sources disable trigger "source_dataset_extraction_trigger_update";
alter table public.sources disable trigger "source_embedding_ft_on_extract_md_update";
alter table public.contacts disable trigger "contacts_json_sync_trigger";
alter table public.contacts disable trigger "contact_dataset_extraction_trigger_insert";
alter table public.contacts disable trigger "contact_dataset_extraction_trigger_update";
alter table public.contacts disable trigger "contact_embedding_ft_on_extract_md_update";

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
  '39812000-0000-0000-0000-000000000001',
  '01.01.000',
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Selected Reference"}]}}}}'::jsonb,
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Selected Reference"}]}}}}'::json,
  '19812000-0000-0000-0000-000000000001',
  20,
  null,
  true,
  '[{"id":"59812000-0000-0000-0000-000000000001"}]'::jsonb
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
  '39812000-0000-0000-0000-000000000002',
  '01.01.000',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Unrelated Missing Reference"}]}}}}'::jsonb,
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Unrelated Missing Reference"}]}}}}'::json,
  '19812000-0000-0000-0000-000000000001',
  20,
  null,
  true,
  '[{"id":"59812000-0000-0000-0000-000000000002"}]'::jsonb
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
values
  (
    '59812000-0000-0000-0000-000000000001',
    '49812000-0000-0000-0000-000000000001',
    '01.01.000',
    0,
    '[]'::jsonb,
    '{"logs":[]}'::jsonb,
    'root',
    'processes',
    pg_catalog.repeat('a', 64),
    '19812000-0000-0000-0000-000000000001'
  ),
  (
    '59812000-0000-0000-0000-000000000002',
    '49812000-0000-0000-0000-000000000002',
    '01.01.000',
    0,
    '[]'::jsonb,
    '{"logs":[]}'::jsonb,
    'root',
    'processes',
    pg_catalog.repeat('b', 64),
    '19812000-0000-0000-0000-000000000001'
  ),
  (
    '59812000-0000-0000-0000-000000000003',
    '39812000-0000-0000-0000-000000000001',
    '01.01.000',
    1,
    '["19812000-0000-0000-0000-000000000001"]'::jsonb,
    '{"logs":[]}'::jsonb,
    'reference',
    'sources',
    private.review_revision_fingerprint_v1(
      'sources',
      api.cmd_review_get_dataset_row(
        'sources',
        '39812000-0000-0000-0000-000000000001',
        '01.01.000',
        false
      )
    ),
    '19812000-0000-0000-0000-000000000001'
  );

insert into private.comments (review_id, reviewer_id, json, state_code)
values
  (
    '59812000-0000-0000-0000-000000000001',
    '19812000-0000-0000-0000-000000000001',
    '{"modellingAndValidation":{"complianceDeclarations":{"compliance":[{"common:referenceToComplianceSystem":{"@refObjectId":"39812000-0000-0000-0000-000000000001","@type":"source data set","@uri":"../sources/39812000-0000-0000-0000-000000000001.xml","@version":"01.01.000"}}]}}}'::json,
    1
  ),
  (
    '59812000-0000-0000-0000-000000000002',
    '19812000-0000-0000-0000-000000000001',
    '{"modellingAndValidation":{"validation":{"review":[{"common:referenceToNameOfReviewerAndInstitution":{"@refObjectId":"39812000-0000-0000-0000-000000000002","@type":"contact data set","@uri":"../contacts/39812000-0000-0000-0000-000000000002.xml","@version":"01.01.000"}}]}}}'::json,
    1
  ),
  (
    '59812000-0000-0000-0000-000000000003',
    '19812000-0000-0000-0000-000000000001',
    '{}'::json,
    1
  );

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '19812000-0000-0000-0000-000000000001',
  true
);

select throws_ok(
  $$select * from private.review_derive_current_references_v1(array[
    '59812000-0000-0000-0000-000000000001'::uuid,
    '59812000-0000-0000-0000-000000000002'::uuid
  ])$$,
  '55000',
  'MISSING_CURRENT_REFERENCE_REVIEW',
  'the fixture retains an unrelated active Root with a missing current Reference Review'
);

select lives_ok(
  $$select * from api.qry_reference_review_impacted_roots(
    '59812000-0000-0000-0000-000000000003',
    false
  )$$,
  'impacted-root lookup does not evaluate the unrelated incomplete Root'
);

select results_eq(
  $$select root_review_id
    from api.qry_reference_review_impacted_roots(
      '59812000-0000-0000-0000-000000000003',
      false
    )$$,
  $$values ('59812000-0000-0000-0000-000000000001'::uuid)$$,
  'impacted-root lookup returns only the Root currently matching the selected Reference checksum'
);

create temporary table reference_approval_result as
select api.cmd_review_finalize_approve(
  '59812000-0000-0000-0000-000000000003',
  '{}'::jsonb
) as result;

select ok(
  (select (result->>'ok')::boolean from reference_approval_result),
  'Reference approval succeeds despite an unrelated incomplete Root'
);

select is(
  (
    select state_code
    from private.reviews
    where id = '59812000-0000-0000-0000-000000000003'
  ),
  2,
  'successful Reference approval commits the Review transition'
);

select is(
  (
    select state_code
    from public.sources
    where id = '39812000-0000-0000-0000-000000000001'
      and version = '01.01.000'
  ),
  100,
  'successful Reference approval publishes the selected dataset'
);

select is(
  (
    select state_code
    from private.reviews
    where id = '59812000-0000-0000-0000-000000000002'
  ),
  0,
  'the unrelated incomplete Root remains unchanged'
);

select * from finish();
rollback;
