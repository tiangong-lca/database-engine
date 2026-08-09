begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(19);

select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'private.review_get_or_create_reference_v1(text,jsonb,text,uuid)'
        ::pg_catalog.regprocedure
    ),
    'REFERENCE_REVISION_REJECTED_UNCHANGED'
  ) = 0,
  'the shared Reference Review helper no longer rejects an unchanged rejected checksum'
);

select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'
        ::pg_catalog.regprocedure
    ),
    'REFERENCE_REVISION_REJECTED_UNCHANGED'
  ) = 0,
  'the unified submit command no longer exposes the unchanged-rejection gate'
);

create or replace function pg_temp.disable_trigger_if_exists(
  p_table pg_catalog.regclass,
  p_trigger pg_catalog.name
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
values
  (
    '00000000-0000-0000-0000-000000000000',
    '43900000-0000-4000-8000-000000000001',
    'authenticated',
    'authenticated',
    'issue439-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"issue439-owner@example.com","display_name":"Issue 439 Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '43900000-0000-4000-8000-000000000002',
    'authenticated',
    'authenticated',
    'issue439-admin@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"issue439-admin@example.com","display_name":"Issue 439 Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data)
values
  (
    '43900000-0000-4000-8000-000000000001',
    '{"email":"issue439-owner@example.com","display_name":"Issue 439 Owner"}'::jsonb
  ),
  (
    '43900000-0000-4000-8000-000000000002',
    '{"email":"issue439-admin@example.com","display_name":"Issue 439 Admin"}'::jsonb
  )
on conflict (id) do update
set raw_user_meta_data = excluded.raw_user_meta_data;

insert into private.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System Team"}'::jsonb,
  0,
  false
)
on conflict (id) do nothing;

insert into private.roles (user_id, team_id, role)
values (
  '43900000-0000-4000-8000-000000000002',
  '00000000-0000-0000-0000-000000000000',
  'review-admin'
)
on conflict do nothing;

select pg_temp.disable_trigger_if_exists(
  'public.flowproperties'::pg_catalog.regclass,
  'flowproperties_json_sync_trigger'
);
select pg_temp.disable_trigger_if_exists(
  'public.flowproperties'::pg_catalog.regclass,
  'flowproperty_dataset_extraction_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.flows'::pg_catalog.regclass,
  'flows_json_sync_trigger'
);
select pg_temp.disable_trigger_if_exists(
  'public.flows'::pg_catalog.regclass,
  'flow_dataset_extraction_trigger_insert'
);

insert into public.flowproperties (
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
values
  (
    '43910000-0000-4000-8000-000000000001',
    '01.00.000',
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner repair property"}]}}}}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner repair property"}]}}}}'::json,
    '43900000-0000-4000-8000-000000000001',
    0,
    null,
    true,
    '[]'::jsonb
  ),
  (
    '43910000-0000-4000-8000-000000000002',
    '01.00.000',
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Dependency retry property"}]}}}}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Dependency retry property"}]}}}}'::json,
    '43900000-0000-4000-8000-000000000001',
    0,
    null,
    true,
    '[]'::jsonb
  );

with flow_fixture(id, flow_name, flowproperty_id) as (
  values
    (
      '43920000-0000-4000-8000-000000000001'::uuid,
      'Owner repair root',
      '43910000-0000-4000-8000-000000000001'::uuid
    ),
    (
      '43920000-0000-4000-8000-000000000002'::uuid,
      'First dependency root',
      '43910000-0000-4000-8000-000000000002'::uuid
    ),
    (
      '43920000-0000-4000-8000-000000000003'::uuid,
      'Second dependency root',
      '43910000-0000-4000-8000-000000000002'::uuid
    ),
    (
      '43920000-0000-4000-8000-000000000004'::uuid,
      'Third dependency root',
      '43910000-0000-4000-8000-000000000002'::uuid
    )
),
flow_document as (
  select
    flow_fixture.id,
    pg_catalog.jsonb_build_object(
      'flowDataSet',
      pg_catalog.jsonb_build_object(
        'flowInformation',
        pg_catalog.jsonb_build_object(
          'dataSetInformation',
          pg_catalog.jsonb_build_object(
            'name',
            pg_catalog.jsonb_build_object(
              'baseName',
              pg_catalog.jsonb_build_array(
                pg_catalog.jsonb_build_object(
                  '@xml:lang', 'en',
                  '#text', flow_fixture.flow_name
                )
              )
            )
          )
        ),
        'flowProperties',
        pg_catalog.jsonb_build_object(
          'flowProperty',
          pg_catalog.jsonb_build_array(
            pg_catalog.jsonb_build_object(
              'referenceToFlowPropertyDataSet',
              pg_catalog.jsonb_build_object(
                '@type', 'flow property data set',
                '@refObjectId', flow_fixture.flowproperty_id,
                '@version', '01.00.000'
              )
            )
          )
        )
      )
    ) as document
  from flow_fixture
)
insert into public.flows (
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
select
  flow_document.id,
  '01.00.000',
  flow_document.document,
  flow_document.document::json,
  '43900000-0000-4000-8000-000000000001',
  0,
  null,
  true,
  '[]'::jsonb
from flow_document;

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '43900000-0000-4000-8000-000000000001',
  true
);

set local role authenticated;
create temporary table issue439_owner_root_result as
select api.cmd_review_submit_v2(
  'flows',
  '43920000-0000-4000-8000-000000000001',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_owner_root_result),
  'the owner-repair fixture enters its initial Root Review'
);

create temporary table issue439_owner_cycle as
select
  (root_result.result #>> '{data,reviewId}')::uuid as root_review_id,
  reference_review.id as rejected_reference_id,
  reference_review.submitted_revision_checksum
from issue439_owner_root_result as root_result
join private.reviews as reference_review
  on reference_review.review_kind = 'reference'
  and reference_review.target_table = 'flowproperties'
  and reference_review.data_id = '43910000-0000-4000-8000-000000000001';

grant select on issue439_owner_cycle to authenticated;

select set_config(
  'request.jwt.claim.sub',
  '43900000-0000-4000-8000-000000000002',
  true
);
set local role authenticated;
create temporary table issue439_owner_reject_result as
select api.cmd_review_finalize_reject(
  (select rejected_reference_id from issue439_owner_cycle),
  'Exercise unchanged owner resubmission',
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_owner_reject_result),
  'Review Admin rejects the first Reference Review'
);

select is(
  (
    select state_code
    from public.flowproperties
    where id = '43910000-0000-4000-8000-000000000001'
      and version = '01.00.000'
  ),
  0,
  'the rejected referenced dataset returns to Draft without changing content'
);

select set_config(
  'request.jwt.claim.sub',
  '43900000-0000-4000-8000-000000000001',
  true
);
set local role authenticated;
create temporary table issue439_owner_resubmit_result as
select api.cmd_review_submit_v2(
  'flowproperties',
  '43910000-0000-4000-8000-000000000001',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_owner_resubmit_result),
  'the referenced dataset owner can resubmit the unchanged rejected revision'
);

select is(
  (select result #>> '{data,submissionMode}' from issue439_owner_resubmit_result),
  'reference_repair',
  'unchanged owner resubmission uses the reference_repair path'
);

select isnt(
  (
    select (result #>> '{data,reviewId}')::uuid
    from issue439_owner_resubmit_result
  ),
  (select rejected_reference_id from issue439_owner_cycle),
  'unchanged owner resubmission creates a new Reference Review identity'
);

select is(
  (
    select pg_catalog.jsonb_agg(review_row.state_code order by review_row.state_code)
    from private.reviews as review_row
    where review_row.review_kind = 'reference'
      and review_row.target_table = 'flowproperties'
      and review_row.data_id = '43910000-0000-4000-8000-000000000001'
      and review_row.submitted_revision_checksum = (
        select submitted_revision_checksum from issue439_owner_cycle
      )
  ),
  '[-1, 0]'::jsonb,
  'the rejected history and new active review retain the same unchanged checksum'
);

select is(
  (
    select root_review.current_reference_review_ids[1]
    from private.reviews as root_review
    where root_review.id = (
      select root_review_id from issue439_owner_cycle
    )
  ),
  (
    select (result #>> '{data,reviewId}')::uuid
    from issue439_owner_resubmit_result
  ),
  'reference repair rebinds the impacted Root Review to the new review identity'
);

select is(
  (
    select (root_review.scope_history->>'current_version')::integer
    from private.reviews as root_review
    where root_review.id = (
      select root_review_id from issue439_owner_cycle
    )
  ),
  2,
  'unchanged reference repair appends a second scope snapshot'
);

set local role authenticated;
create temporary table issue439_dependency_root_one_result as
select api.cmd_review_submit_v2(
  'flows',
  '43920000-0000-4000-8000-000000000002',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_dependency_root_one_result),
  'the dependency fixture enters its initial Root Review'
);

create temporary table issue439_dependency_cycle as
select
  (root_result.result #>> '{data,reviewId}')::uuid as first_root_review_id,
  reference_review.id as rejected_reference_id,
  reference_review.submitted_revision_checksum
from issue439_dependency_root_one_result as root_result
join private.reviews as reference_review
  on reference_review.review_kind = 'reference'
  and reference_review.target_table = 'flowproperties'
  and reference_review.data_id = '43910000-0000-4000-8000-000000000002';

grant select on issue439_dependency_cycle to authenticated;

select set_config(
  'request.jwt.claim.sub',
  '43900000-0000-4000-8000-000000000002',
  true
);
set local role authenticated;
create temporary table issue439_dependency_reject_result as
select api.cmd_review_finalize_reject(
  (select rejected_reference_id from issue439_dependency_cycle),
  'Exercise unchanged dependency resubmission',
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_dependency_reject_result),
  'Review Admin rejects the shared dependency revision'
);

select set_config(
  'request.jwt.claim.sub',
  '43900000-0000-4000-8000-000000000001',
  true
);
set local role authenticated;
create temporary table issue439_dependency_root_two_result as
select api.cmd_review_submit_v2(
  'flows',
  '43920000-0000-4000-8000-000000000003',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_dependency_root_two_result),
  'a new Root Review can submit an unchanged previously rejected dependency'
);

select isnt(
  (
    select root_review.current_reference_review_ids[1]
    from private.reviews as root_review
    where root_review.id = (
      select (result #>> '{data,reviewId}')::uuid
      from issue439_dependency_root_two_result
    )
  ),
  (select rejected_reference_id from issue439_dependency_cycle),
  'the new Root Review receives a new Reference Review instead of the rejected row'
);

select is(
  (
    select root_review.current_reference_review_ids[1]
    from private.reviews as root_review
    where root_review.id = (
      select first_root_review_id from issue439_dependency_cycle
    )
  ),
  (select rejected_reference_id from issue439_dependency_cycle),
  'the earlier Root Review retains its immutable historical rejected relationship'
);

set local role authenticated;
create temporary table issue439_dependency_root_three_result as
select api.cmd_review_submit_v2(
  'flows',
  '43920000-0000-4000-8000-000000000004',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue439_dependency_root_three_result),
  'a later Root Review can reuse the new active Reference Review'
);

select is(
  (
    select root_review.current_reference_review_ids[1]
    from private.reviews as root_review
    where root_review.id = (
      select (result #>> '{data,reviewId}')::uuid
      from issue439_dependency_root_two_result
    )
  ),
  (
    select root_review.current_reference_review_ids[1]
    from private.reviews as root_review
    where root_review.id = (
      select (result #>> '{data,reviewId}')::uuid
      from issue439_dependency_root_three_result
    )
  ),
  'active-review deduplication remains unchanged after the rejected retry'
);

select is(
  (
    select pg_catalog.jsonb_agg(review_row.state_code order by review_row.state_code)
    from private.reviews as review_row
    where review_row.review_kind = 'reference'
      and review_row.target_table = 'flowproperties'
      and review_row.data_id = '43910000-0000-4000-8000-000000000002'
      and review_row.submitted_revision_checksum = (
        select submitted_revision_checksum from issue439_dependency_cycle
      )
  ),
  '[-1, 0]'::jsonb,
  'the dependency retry creates one new active row and preserves one rejected row'
);

select * from finish();
rollback;
