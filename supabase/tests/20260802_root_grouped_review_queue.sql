begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(27);

select is(
  public.cmd_review_get_dataset_name(
    'contacts',
    '{"json":{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Contact name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Contact name"}]}'::jsonb,
  'Contact review snapshots normalize common:shortName as baseName'
);
select is(
  public.cmd_review_get_dataset_name(
    'sources',
    '{"json":{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Source name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Source name"}]}'::jsonb,
  'Source review snapshots normalize common:shortName as baseName'
);
select is(
  public.cmd_review_get_dataset_name(
    'unitgroups',
    '{"json":{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Unit group name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Unit group name"}]}'::jsonb,
  'Unit Group review snapshots normalize common:name as baseName'
);
select is(
  public.cmd_review_get_dataset_name(
    'flowproperties',
    '{"json":{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Flow property name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Flow property name"}]}'::jsonb,
  'Flow Property review snapshots normalize common:name as baseName'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password,
  email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
  created_at, updated_at, is_sso_user, is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    '48200000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'queue-owner@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}',
    '{"display_name":"Queue Owner"}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '48200000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'reviewer-a@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}',
    '{"display_name":"Reviewer A"}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '48200000-0000-4000-8000-000000000003',
    'authenticated', 'authenticated', 'reviewer-b@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}',
    '{"display_name":"Reviewer B"}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '48200000-0000-4000-8000-000000000004',
    'authenticated', 'authenticated', 'queue-admin@example.com', 'test',
    now(), '{"provider":"email","providers":["email"]}',
    '{"display_name":"Queue Admin"}', now(), now(), false, false
  );

insert into public.users (id, raw_user_meta_data)
values
  ('48200000-0000-4000-8000-000000000001', '{"display_name":"Queue Owner"}'),
  ('48200000-0000-4000-8000-000000000002', '{"display_name":"Reviewer A"}'),
  ('48200000-0000-4000-8000-000000000003', '{"display_name":"Reviewer B"}'),
  ('48200000-0000-4000-8000-000000000004', '{"display_name":"Queue Admin"}');

insert into public.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System Team"}', 0, false
)
on conflict (id) do nothing;

insert into public.roles (user_id, team_id, role)
values
  (
    '48200000-0000-4000-8000-000000000002',
    '00000000-0000-0000-0000-000000000000',
    'review-member'
  ),
  (
    '48200000-0000-4000-8000-000000000003',
    '00000000-0000-0000-0000-000000000000',
    'review-member'
  ),
  (
    '48200000-0000-4000-8000-000000000004',
    '00000000-0000-0000-0000-000000000000',
    'review-admin'
  );

-- One unassigned shared Reference Review and two independently assigned
-- Reference Reviews provide mixed-status and access-isolation fixtures.
insert into public.reviews (
  id, data_id, data_version, state_code, reviewer_id, json,
  review_kind, target_table, submitted_revision_checksum, target_owner_id,
  created_at, modified_at
)
values
  (
    '48200000-0000-4000-8000-000000000101',
    '48200000-0000-4000-8000-000000000201', '01.00.000', 0, '[]',
    '{"review_kind":"reference","data":{"id":"48200000-0000-4000-8000-000000000201","version":"01.00.000","table":"flows","name":{"baseName":{"en":"Shared Flow"}}}}',
    'reference', 'flows', repeat('1', 64),
    '48200000-0000-4000-8000-000000000001', now(), now()
  ),
  (
    '48200000-0000-4000-8000-000000000102',
    '48200000-0000-4000-8000-000000000202', '01.00.000', 1,
    '["48200000-0000-4000-8000-000000000002"]',
    '{"review_kind":"reference","data":{"id":"48200000-0000-4000-8000-000000000202","version":"01.00.000","table":"sources","name":{"baseName":{"en":"Reviewer A Source"}}}}',
    'reference', 'sources', repeat('2', 64),
    '48200000-0000-4000-8000-000000000001', now(), now()
  ),
  (
    '48200000-0000-4000-8000-000000000103',
    '48200000-0000-4000-8000-000000000203', '01.00.000', 1,
    '["48200000-0000-4000-8000-000000000003"]',
    '{"review_kind":"reference","data":{"id":"48200000-0000-4000-8000-000000000203","version":"01.00.000","table":"contacts","name":{"baseName":{"en":"Reviewer B Contact"}}}}',
    'reference', 'contacts', repeat('3', 64),
    '48200000-0000-4000-8000-000000000001', now(), now()
  );

insert into public.comments (review_id, reviewer_id, json, state_code)
values
  (
    '48200000-0000-4000-8000-000000000102',
    '48200000-0000-4000-8000-000000000002', '{}', 0
  ),
  (
    '48200000-0000-4000-8000-000000000103',
    '48200000-0000-4000-8000-000000000003', '{}', 0
  );

create temporary table grouped_root_fixture (
  root_review_id uuid primary key,
  root_state_code integer not null,
  reference_ids uuid[] not null
) on commit drop;

insert into grouped_root_fixture (root_review_id, root_state_code, reference_ids)
values
  (
    '48200000-0000-4000-8000-000000000111', 1,
    array[
      '48200000-0000-4000-8000-000000000101'::uuid,
      '48200000-0000-4000-8000-000000000102'::uuid,
      '48200000-0000-4000-8000-000000000103'::uuid
    ]
  ),
  (
    '48200000-0000-4000-8000-000000000112', 0,
    array['48200000-0000-4000-8000-000000000101'::uuid]
  ),
  (
    '48200000-0000-4000-8000-000000000113', 1,
    array['48200000-0000-4000-8000-000000000102'::uuid]
  );

with root_items as (
  select
    fixture.root_review_id,
    fixture.root_state_code,
    pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'item_kind', 'root',
        'target_table', 'processes',
        'data_id', fixture.root_review_id,
        'data_version', '01.00.000',
        'submitted_revision_checksum', repeat('a', 64),
        'target_owner_id', '48200000-0000-4000-8000-000000000001'
      )
    ) || coalesce((
      select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'item_kind', 'reference',
          'target_table', reference_review.target_table,
          'data_id', reference_review.data_id,
          'data_version', pg_catalog.btrim(reference_review.data_version::text),
          'submitted_revision_checksum', reference_review.submitted_revision_checksum,
          'reference_review_id', reference_review.id,
          'target_owner_id', reference_review.target_owner_id
        )
        order by reference_review.id
      )
      from pg_catalog.unnest(fixture.reference_ids) as related(reference_review_id)
      join public.reviews as reference_review
        on reference_review.id = related.reference_review_id
    ), '[]'::jsonb) as items
  from grouped_root_fixture as fixture
),
root_histories as (
  select
    root_item.root_review_id,
    root_item.root_state_code,
    pg_catalog.jsonb_build_object(
      'schema_version', 'review_scope.v1',
      'current_version', 1,
      'snapshots', pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'version_no', 1,
          'scope_basis', 'submitted',
          'root_revision_checksum', repeat('a', 64),
          'scope_checksum', public.review_scope_checksum_v1(root_item.items),
          'created_by', '48200000-0000-4000-8000-000000000001',
          'created_at', pg_catalog.to_jsonb(now()),
          'items', root_item.items
        )
      )
    ) as scope_history
  from root_items as root_item
)
insert into public.reviews (
  id, data_id, data_version, state_code, reviewer_id, json,
  review_kind, target_table, submitted_revision_checksum, target_owner_id,
  scope_schema_version, scope_history, created_at, modified_at
)
select
  root_history.root_review_id,
  root_history.root_review_id,
  '01.00.000',
  root_history.root_state_code,
  '[]',
  pg_catalog.jsonb_build_object(
    'review_kind', 'root',
    'data', pg_catalog.jsonb_build_object(
      'id', root_history.root_review_id,
      'version', '01.00.000',
      'table', 'processes',
      'name', pg_catalog.jsonb_build_object(
        'baseName', pg_catalog.jsonb_build_object(
          'en', 'Root ' || root_history.root_review_id::text
        )
      )
    ),
    'user', pg_catalog.jsonb_build_object(
      'id', '48200000-0000-4000-8000-000000000001',
      'name', 'Queue Owner'
    )
  ),
  'root', 'processes', repeat('a', 64),
  '48200000-0000-4000-8000-000000000001',
  'review_scope.v1', root_history.scope_history, now(), now()
from root_histories as root_history;

select has_function(
  'public', 'qry_review_get_admin_root_queue_items_v2',
  array['text', 'integer', 'integer', 'text', 'text'],
  'grouped Review Admin queue function exists'
);
select has_function(
  'public', 'qry_review_get_member_root_queue_items_v2',
  array['text', 'integer', 'integer', 'text', 'text'],
  'grouped Review Member queue function exists'
);
select has_function(
  'public', 'qry_root_review_reference_progress_v2', array['uuid'],
  'versioned root child query function exists'
);
select has_function(
  'public', 'qry_root_review_reference_progress', array['uuid'],
  'legacy root child query remains available for compatibility'
);

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  '48200000-0000-4000-8000-000000000004',
  true
);
set local role authenticated;

select is(
  (
    select pg_catalog.count(*)::integer
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Admin unassigned queue includes roots matched directly or through a child'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where review_kind = 'root'
  ),
  2,
  'Admin main queue contains only Root Reviews'
);
select is(
  (
    select root_matches_status
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where id = '48200000-0000-4000-8000-000000000111'
  ),
  false,
  'an assigned root is marked as included by its unassigned child'
);
select is(
  (
    select root_matches_status
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where id = '48200000-0000-4000-8000-000000000112'
  ),
  true,
  'an unassigned root is marked as directly matching the tab'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Admin total_count counts grouped roots rather than individual reviews'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 1, 'modified_at', 'desc'
    )
  ),
  1,
  'Admin pagination is applied after root grouping'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from public.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 1, 'modified_at', 'desc'
    )
  ),
  2,
  'paginated Admin rows retain the grouped total'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from public.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  3,
  'Review Admin sees all current Reference Reviews under a root'
);
select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_function_result(
      'public.qry_root_review_reference_progress_v2(uuid)'::regprocedure
    ),
    'relation_paths'
  ) = 0,
  'the child-row contract does not expose relation paths'
);
select is(
  (
    select child_a.reference_review_id
    from public.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    ) as child_a
    where child_a.reference_review_id =
      '48200000-0000-4000-8000-000000000101'
  ),
  (
    select child_b.reference_review_id
    from public.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000112'
    ) as child_b
    where child_b.reference_review_id =
      '48200000-0000-4000-8000-000000000101'
  ),
  'a shared Reference Review keeps the same review id under both roots'
);

reset role;
select set_config(
  'request.jwt.claim.sub',
  '48200000-0000-4000-8000-000000000002',
  true
);
set local role authenticated;

select is(
  (
    select pg_catalog.count(*)::integer
    from public.qry_review_get_member_root_queue_items_v2(
      'pending', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'one assigned Reference Review places every related root in the member queue'
);
select ok(
  not exists (
    select 1
    from public.qry_review_get_member_root_queue_items_v2(
      'pending', 1, 10, 'modified_at', 'desc'
    )
    where root_matches_status or root_can_read
  ),
  'Reference-only member results do not expose root actions or full root access'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from public.qry_review_get_member_root_queue_items_v2(
      'pending', 1, 1, 'modified_at', 'desc'
    )
  ),
  2,
  'Review Member pagination also counts grouped roots'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from public.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  1,
  'Review Member child query hides unassigned and sibling reviewer tasks'
);
select is(
  (
    select reference_review_id
    from public.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  '48200000-0000-4000-8000-000000000102'::uuid,
  'Review Member sees only their assigned Reference Review id'
);
select is(
  (
    select actor_comment_state_code
    from public.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  0,
  'member child row includes the actor comment state needed for tab matching'
);

reset role;
select set_config('request.jwt.claim.sub', '', true);
set local role authenticated;
select throws_ok(
  $$select * from public.qry_root_review_reference_progress_v2(
    '48200000-0000-4000-8000-000000000111'
  )$$,
  '42501',
  'REVIEW_ROLE_REQUIRED',
  'child query rejects callers without a review role'
);

reset role;
select ok(
  not has_function_privilege(
    'anon',
    'public.qry_review_get_admin_root_queue_items_v2(text,integer,integer,text,text)',
    'EXECUTE'
  ),
  'anonymous callers cannot execute the grouped Admin queue'
);
select ok(
  not has_function_privilege(
    'anon',
    'public.qry_review_get_member_root_queue_items_v2(text,integer,integer,text,text)',
    'EXECUTE'
  ),
  'anonymous callers cannot execute the grouped Member queue'
);

select * from finish();
rollback;
