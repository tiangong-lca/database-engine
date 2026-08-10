begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(15);

select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'
        ::pg_catalog.regprocedure
    ),
    'perform private.review_rebind_active_roots_to_reference_v1('
  ) > 0,
  'root submission invokes the shared active-root rebind helper for dependencies'
);

select ok(
  not pg_catalog.has_function_privilege(
    'authenticated',
    'private.review_rebind_active_roots_to_reference_v1(text,jsonb,text,uuid,uuid)',
    'EXECUTE'
  ),
  'the internal root-rebind helper is not directly executable by authenticated users'
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

create or replace function pg_temp.issue446_scope_history(
  p_root_id uuid,
  p_root_checksum text,
  p_reference_review_id uuid,
  p_reference_checksum text,
  p_keep_reference_current boolean
)
returns jsonb
language sql
as $$
  with root_item as (
    select pg_catalog.jsonb_build_object(
      'item_kind', 'root',
      'target_table', 'processes',
      'data_id', p_root_id,
      'data_version', '01.00.000',
      'submitted_revision_checksum', p_root_checksum,
      'reference_review_id', null,
      'target_owner_id', '44600000-0000-4000-8000-000000000001',
      'target_team_id', null,
      'relation_type', 'root',
      'relation_path', '$',
      'introduced_by', 'submitted_data',
      'introduced_field_path', null
    ) as item
  ),
  reference_item as (
    select pg_catalog.jsonb_build_object(
      'item_kind', 'reference',
      'target_table', 'contacts',
      'data_id', '44610000-0000-4000-8000-000000000001',
      'data_version', '01.00.000',
      'submitted_revision_checksum', p_reference_checksum,
      'reference_review_id', p_reference_review_id,
      'target_owner_id', '44600000-0000-4000-8000-000000000001',
      'target_team_id', null,
      'relation_type', 'dependency',
      'relation_path', '$',
      'introduced_by', 'submitted_data',
      'introduced_field_path', null
    ) as item
  ),
  scope_items as (
    select
      pg_catalog.jsonb_build_array(
        root_item.item,
        reference_item.item
      ) as with_reference,
      pg_catalog.jsonb_build_array(root_item.item) as without_reference
    from root_item
    cross join reference_item
  ),
  first_snapshot as (
    select pg_catalog.jsonb_build_object(
      'version_no', 1,
      'scope_basis', 'submitted',
      'root_revision_checksum', p_root_checksum,
      'scope_checksum', private.review_scope_checksum_v1(
        scope_items.with_reference
      ),
      'created_by', '44600000-0000-4000-8000-000000000001',
      'created_at', pg_catalog.to_jsonb(pg_catalog.now()),
      'items', scope_items.with_reference
    ) as snapshot
    from scope_items
  ),
  second_snapshot as (
    select pg_catalog.jsonb_build_object(
      'version_no', 2,
      'scope_basis', 'review_metadata',
      'root_revision_checksum', p_root_checksum,
      'scope_checksum', private.review_scope_checksum_v1(
        scope_items.without_reference
      ),
      'created_by', '44600000-0000-4000-8000-000000000001',
      'created_at', pg_catalog.to_jsonb(pg_catalog.now()),
      'items', scope_items.without_reference
    ) as snapshot
    from scope_items
  )
  select pg_catalog.jsonb_build_object(
    'schema_version', 'review_scope.v1',
    'current_version', case when p_keep_reference_current then 1 else 2 end,
    'snapshots', case
      when p_keep_reference_current
        then pg_catalog.jsonb_build_array(first_snapshot.snapshot)
      else pg_catalog.jsonb_build_array(
        first_snapshot.snapshot,
        second_snapshot.snapshot
      )
    end
  )
  from first_snapshot
  cross join second_snapshot
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
  '44600000-0000-4000-8000-000000000001',
  'authenticated',
  'authenticated',
  'issue446-owner@example.com',
  'test-password-hash',
  pg_catalog.now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"email":"issue446-owner@example.com","display_name":"Issue 446 Owner"}'::jsonb,
  pg_catalog.now(),
  pg_catalog.now(),
  false,
  false
);

insert into private.users (id, raw_user_meta_data)
values (
  '44600000-0000-4000-8000-000000000001',
  '{"email":"issue446-owner@example.com","display_name":"Issue 446 Owner"}'::jsonb
)
on conflict (id) do update
set raw_user_meta_data = excluded.raw_user_meta_data;

select pg_temp.disable_trigger_if_exists(
  'public.contacts'::pg_catalog.regclass,
  'contacts_json_sync_trigger'
);
select pg_temp.disable_trigger_if_exists(
  'public.contacts'::pg_catalog.regclass,
  'contact_dataset_extraction_trigger_insert'
);
select pg_temp.disable_trigger_if_exists(
  'public.contacts'::pg_catalog.regclass,
  'contact_dataset_extraction_trigger_update'
);
select pg_temp.disable_trigger_if_exists(
  'public.contacts'::pg_catalog.regclass,
  'contact_embedding_ft_on_extract_md_update'
);

with contact_fixture(id, contact_name, reference_id) as (
  values
    (
      '44610000-0000-4000-8000-000000000001'::uuid,
      'Contact C',
      null::uuid
    ),
    (
      '44610000-0000-4000-8000-000000000002'::uuid,
      'Contact D',
      '44610000-0000-4000-8000-000000000001'::uuid
    ),
    (
      '44610000-0000-4000-8000-000000000003'::uuid,
      'Contact D2',
      '44610000-0000-4000-8000-000000000001'::uuid
    )
),
contact_document as (
  select
    contact_fixture.id,
    case
      when contact_fixture.reference_id is null then
        pg_catalog.jsonb_build_object(
          'contactDataSet',
          pg_catalog.jsonb_build_object(
            'contactInformation',
            pg_catalog.jsonb_build_object(
              'dataSetInformation',
              pg_catalog.jsonb_build_object(
                'common:shortName',
                pg_catalog.jsonb_build_array(
                  pg_catalog.jsonb_build_object(
                    '@xml:lang', 'en',
                    '#text', contact_fixture.contact_name
                  )
                )
              )
            )
          )
        )
      else
        pg_catalog.jsonb_build_object(
          'contactDataSet',
          pg_catalog.jsonb_build_object(
            'contactInformation',
            pg_catalog.jsonb_build_object(
              'dataSetInformation',
              pg_catalog.jsonb_build_object(
                'common:shortName',
                pg_catalog.jsonb_build_array(
                  pg_catalog.jsonb_build_object(
                    '@xml:lang', 'en',
                    '#text', contact_fixture.contact_name
                  )
                ),
                'referenceToContact',
                pg_catalog.jsonb_build_object(
                  '@type', 'contact data set',
                  '@refObjectId', contact_fixture.reference_id,
                  '@version', '01.00.000'
                )
              )
            )
          )
        )
    end as document
  from contact_fixture
)
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
select
  contact_document.id,
  '01.00.000',
  contact_document.document,
  contact_document.document::json,
  '44600000-0000-4000-8000-000000000001',
  0,
  null,
  true,
  '[]'::jsonb
from contact_document;

create temporary table issue446_rejected_reference as
select
  '44620000-0000-4000-8000-000000000001'::uuid as review_id,
  private.review_revision_fingerprint_v1(
    'contacts',
    api.cmd_review_get_dataset_row(
      'contacts',
      '44610000-0000-4000-8000-000000000001',
      '01.00.000',
      false
    )
  ) as checksum;

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
select
  issue446_rejected_reference.review_id,
  '44610000-0000-4000-8000-000000000001',
  '01.00.000',
  -1,
  '[]'::jsonb,
  '{"logs":[]}'::jsonb,
  'reference',
  'contacts',
  issue446_rejected_reference.checksum,
  '44600000-0000-4000-8000-000000000001'
from issue446_rejected_reference;

with root_fixture(
  review_id,
  process_id,
  state_code,
  root_checksum,
  keep_reference_current
) as (
  values
    (
      '44630000-0000-4000-8000-000000000001'::uuid,
      '44640000-0000-4000-8000-000000000001'::uuid,
      0,
      pg_catalog.repeat('a', 64),
      true
    ),
    (
      '44630000-0000-4000-8000-000000000002'::uuid,
      '44640000-0000-4000-8000-000000000002'::uuid,
      1,
      pg_catalog.repeat('b', 64),
      true
    ),
    (
      '44630000-0000-4000-8000-000000000003'::uuid,
      '44640000-0000-4000-8000-000000000003'::uuid,
      2,
      pg_catalog.repeat('c', 64),
      true
    ),
    (
      '44630000-0000-4000-8000-000000000004'::uuid,
      '44640000-0000-4000-8000-000000000004'::uuid,
      0,
      pg_catalog.repeat('d', 64),
      false
    )
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
  approved_revision_checksum,
  target_owner_id,
  scope_schema_version,
  scope_history
)
select
  root_fixture.review_id,
  root_fixture.process_id,
  '01.00.000',
  root_fixture.state_code,
  '[]'::jsonb,
  '{"logs":[]}'::jsonb,
  'root',
  'processes',
  root_fixture.root_checksum,
  case when root_fixture.state_code = 2 then root_fixture.root_checksum end,
  '44600000-0000-4000-8000-000000000001',
  'review_scope.v1',
  pg_temp.issue446_scope_history(
    root_fixture.process_id,
    root_fixture.root_checksum,
    issue446_rejected_reference.review_id,
    issue446_rejected_reference.checksum,
    root_fixture.keep_reference_current
  )
from root_fixture
cross join issue446_rejected_reference;

select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '44600000-0000-4000-8000-000000000001',
  true
);

set local role authenticated;
create temporary table issue446_contact_d_result as
select api.cmd_review_submit_v2(
  'contacts',
  '44610000-0000-4000-8000-000000000002',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue446_contact_d_result),
  'Contact D can enter review while referencing rejected Contact C'
);

create temporary table issue446_active_reference as
select reference_review.id
from private.reviews as reference_review
where reference_review.review_kind = 'reference'
  and reference_review.target_table = 'contacts'
  and reference_review.data_id = '44610000-0000-4000-8000-000000000001'
  and reference_review.state_code in (0, 1, 2);

select is(
  (
    select pg_catalog.jsonb_agg(
      reference_review.state_code
      order by reference_review.state_code
    )
    from private.reviews as reference_review
    where reference_review.review_kind = 'reference'
      and reference_review.target_table = 'contacts'
      and reference_review.data_id = '44610000-0000-4000-8000-000000000001'
  ),
  '[-1, 0]'::jsonb,
  'Contact C keeps the rejected review and receives one new active review'
);

select is(
  (
    select pg_catalog.count(distinct current_reference_review_ids[1])::integer
    from private.reviews
    where id in (
      '44630000-0000-4000-8000-000000000001',
      '44630000-0000-4000-8000-000000000002',
      (select (result #>> '{data,reviewId}')::uuid
       from issue446_contact_d_result)
    )
  ),
  1,
  'Process A, Process B, and Contact D share one current Contact C review identity'
);

select is(
  (
    select pg_catalog.count(*)::integer
    from private.reviews as root_review
    cross join issue446_active_reference
    where root_review.id in (
      '44630000-0000-4000-8000-000000000001',
      '44630000-0000-4000-8000-000000000002',
      (select (result #>> '{data,reviewId}')::uuid
       from issue446_contact_d_result)
    )
      and root_review.current_reference_review_ids
        @> array[issue446_active_reference.id]::uuid[]
  ),
  3,
  'all three eligible roots point at the new active Contact C review'
);

select is(
  (
    select pg_catalog.jsonb_agg(
      (scope_history->>'current_version')::integer
      order by id
    )
    from private.reviews
    where id in (
      '44630000-0000-4000-8000-000000000001',
      '44630000-0000-4000-8000-000000000002'
    )
  ),
  '[2, 2]'::jsonb,
  'both active Process roots append exactly one reference_repair snapshot'
);

select is(
  (
    select pg_catalog.count(*)::integer
    from private.reviews as root_review
    cross join lateral pg_catalog.jsonb_array_elements(
      root_review.scope_history->'snapshots'
    ) as snapshot(value)
    cross join lateral pg_catalog.jsonb_array_elements(
      snapshot.value->'items'
    ) as item(value)
    cross join issue446_rejected_reference
    where root_review.id in (
      '44630000-0000-4000-8000-000000000001',
      '44630000-0000-4000-8000-000000000002'
    )
      and (snapshot.value->>'version_no')::integer = 1
      and item.value->>'reference_review_id' =
        issue446_rejected_reference.review_id::text
  ),
  2,
  'both Process roots retain Contact C rejection in their historical snapshots'
);

select is(
  (
    select pg_catalog.jsonb_build_object(
      'scopeVersion', (scope_history->>'current_version')::integer,
      'referenceReviewId', current_reference_review_ids[1]
    )
    from private.reviews
    where id = '44630000-0000-4000-8000-000000000003'
  ),
  pg_catalog.jsonb_build_object(
    'scopeVersion', 1,
    'referenceReviewId',
      '44620000-0000-4000-8000-000000000001'::uuid
  ),
  'a finalized Process root remains bound to its historical rejected child'
);

select is(
  (
    select pg_catalog.jsonb_build_object(
      'scopeVersion', (scope_history->>'current_version')::integer,
      'referenceCount', cardinality(current_reference_review_ids)
    )
    from private.reviews
    where id = '44630000-0000-4000-8000-000000000004'
  ),
  '{"scopeVersion":2,"referenceCount":0}'::jsonb,
  'an active Process root that no longer references Contact C is not rebound'
);

set local role authenticated;
create temporary table issue446_contact_d2_result as
select api.cmd_review_submit_v2(
  'contacts',
  '44610000-0000-4000-8000-000000000003',
  '01.00.000',
  null,
  '{}'::jsonb
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from issue446_contact_d2_result),
  'a later Contact root can reuse the same active Contact C review'
);

select is(
  (
    select current_reference_review_ids[1]
    from private.reviews
    where id = (
      select (result #>> '{data,reviewId}')::uuid
      from issue446_contact_d2_result
    )
  ),
  (select id from issue446_active_reference),
  'the later root receives the existing active Contact C review identity'
);

select is(
  (
    select pg_catalog.jsonb_agg(
      (scope_history->>'current_version')::integer
      order by id
    )
    from private.reviews
    where id in (
      '44630000-0000-4000-8000-000000000001',
      '44630000-0000-4000-8000-000000000002'
    )
  ),
  '[2, 2]'::jsonb,
  'active-reference reuse does not append duplicate snapshots to Process A or B'
);

select is(
  (
    select pg_catalog.count(*)::integer
    from private.reviews as reference_review
    where reference_review.review_kind = 'reference'
      and reference_review.target_table = 'contacts'
      and reference_review.data_id = '44610000-0000-4000-8000-000000000001'
      and reference_review.state_code in (0, 1, 2)
  ),
  1,
  'repeated roots still produce exactly one active Contact C review'
);

select is(
  (
    select pg_catalog.count(*)::integer
    from private.reviews as root_review
    cross join issue446_rejected_reference
    where root_review.id in (
      '44630000-0000-4000-8000-000000000001',
      '44630000-0000-4000-8000-000000000002'
    )
      and root_review.all_reference_review_ids
        @> array[issue446_rejected_reference.review_id]::uuid[]
  ),
  2,
  'append-only root histories continue to index the rejected Contact C review'
);

select * from finish();
rollback;
