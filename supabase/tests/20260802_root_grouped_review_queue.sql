begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(54);

select is(
  api.cmd_review_get_dataset_name(
    'contacts',
    '{"json":{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Contact name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Contact name"}]}'::jsonb,
  'Contact review snapshots normalize common:shortName as baseName'
);
select is(
  api.cmd_review_get_dataset_name(
    'sources',
    '{"json":{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Source name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Source name"}]}'::jsonb,
  'Source review snapshots normalize common:shortName as baseName'
);
select is(
  api.cmd_review_get_dataset_name(
    'unitgroups',
    '{"json":{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Unit group name"}]}}}}}'::jsonb
  ),
  '{"baseName":[{"@xml:lang":"en","#text":"Unit group name"}]}'::jsonb,
  'Unit Group review snapshots normalize common:name as baseName'
);
select is(
  api.cmd_review_get_dataset_name(
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

insert into private.users (id, raw_user_meta_data)
values
  ('48200000-0000-4000-8000-000000000001', '{"display_name":"Queue Owner"}'),
  ('48200000-0000-4000-8000-000000000002', '{"display_name":"Reviewer A"}'),
  ('48200000-0000-4000-8000-000000000003', '{"display_name":"Reviewer B"}'),
  ('48200000-0000-4000-8000-000000000004', '{"display_name":"Queue Admin"}');

insert into private.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System Team"}', 0, false
)
on conflict (id) do nothing;

insert into private.roles (user_id, team_id, role)
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

-- Real business JSON is the relationship source. reviews[].id only narrows the
-- candidate roots and intentionally contains no Reference Review IDs.
set local session_replication_role = replica;
insert into public.flows (id, version, json, json_ordered, user_id, state_code, reviews)
values (
  '48200000-0000-4000-8000-000000000201', '01.00.000',
  '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Shared Flow"}]}}}}}',
  '{}', '48200000-0000-4000-8000-000000000001', 20,
  '[{"key":0,"id":"48200000-0000-4000-8000-000000000111"},{"key":0,"id":"48200000-0000-4000-8000-000000000112"}]'
);
insert into public.sources (id, version, json, json_ordered, user_id, state_code, reviews)
values (
  '48200000-0000-4000-8000-000000000202', '01.00.000',
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Reviewer A Source"}]}}}}',
  '{}', '48200000-0000-4000-8000-000000000001', 20,
  '[{"key":0,"id":"48200000-0000-4000-8000-000000000111"},{"key":0,"id":"48200000-0000-4000-8000-000000000113"}]'
);
insert into public.contacts (id, version, json, json_ordered, user_id, state_code, reviews)
values (
  '48200000-0000-4000-8000-000000000203', '01.00.000',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Reviewer B Contact"}]}}}}',
  '{}', '48200000-0000-4000-8000-000000000001', 20,
  '[{"key":0,"id":"48200000-0000-4000-8000-000000000111"}]'
);

insert into public.processes (id, version, json, json_ordered, user_id, state_code, reviews)
values
  (
    '48200000-0000-4000-8000-000000000111', '01.00.000', '{}',
    '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"48200000-0000-4000-8000-000000000201","@version":"01.00.000"}}},"fixtures":[{"@type":"source data set","@refObjectId":"48200000-0000-4000-8000-000000000202","@version":"01.00.000"},{"@type":"contact data set","@refObjectId":"48200000-0000-4000-8000-000000000203","@version":"01.00.000"}]}}',
    '48200000-0000-4000-8000-000000000001', 20,
    '[{"key":0,"id":"48200000-0000-4000-8000-000000000111"}]'
  ),
  (
    '48200000-0000-4000-8000-000000000112', '01.00.000', '{}',
    '{"processDataSet":{"exchanges":{"exchange":{"referenceToFlowDataSet":{"@type":"flow data set","@refObjectId":"48200000-0000-4000-8000-000000000201","@version":"01.00.000"}}}}}',
    '48200000-0000-4000-8000-000000000001', 20,
    '[{"key":0,"id":"48200000-0000-4000-8000-000000000112"}]'
  ),
  (
    '48200000-0000-4000-8000-000000000113', '01.00.000', '{}',
    '{"processDataSet":{"fixtures":[{"@type":"source data set","@refObjectId":"48200000-0000-4000-8000-000000000202","@version":"01.00.000"}]}}',
    '48200000-0000-4000-8000-000000000001', 20,
    '[{"key":0,"id":"48200000-0000-4000-8000-000000000113"}]'
  );
set local session_replication_role = origin;

insert into private.reviews (
  id, data_id, data_version, state_code, reviewer_id, json,
  review_kind, target_table, submitted_revision_checksum, target_owner_id,
  created_at, modified_at
)
select
  fixture.review_id, fixture.data_id, '01.00.000', fixture.state_code,
  fixture.reviewer_id, fixture.review_json, 'reference', fixture.target_table,
  private.review_revision_fingerprint_v1(
    fixture.target_table,
    api.cmd_review_get_dataset_row(
      fixture.target_table, fixture.data_id, '01.00.000', false
    )
  ),
  '48200000-0000-4000-8000-000000000001', now(), now()
from (values
  (
    '48200000-0000-4000-8000-000000000101'::uuid,
    '48200000-0000-4000-8000-000000000201'::uuid, 'flows', 0, '[]'::jsonb,
    '{"review_kind":"reference","data":{"table":"flows","name":{"baseName":{"en":"Shared Flow"}}}}'::jsonb
  ),
  (
    '48200000-0000-4000-8000-000000000102'::uuid,
    '48200000-0000-4000-8000-000000000202'::uuid, 'sources', 1,
    '["48200000-0000-4000-8000-000000000002"]'::jsonb,
    '{"review_kind":"reference","data":{"table":"sources","name":{"baseName":{"en":"Reviewer A Source"}}}}'::jsonb
  ),
  (
    '48200000-0000-4000-8000-000000000103'::uuid,
    '48200000-0000-4000-8000-000000000203'::uuid, 'contacts', 1,
    '["48200000-0000-4000-8000-000000000003"]'::jsonb,
    '{"review_kind":"reference","data":{"table":"contacts","name":{"baseName":{"en":"Reviewer B Contact"}}}}'::jsonb
  )
) as fixture(review_id, data_id, target_table, state_code, reviewer_id, review_json);

insert into private.comments (review_id, reviewer_id, json, state_code)
values
  (
    '48200000-0000-4000-8000-000000000102',
    '48200000-0000-4000-8000-000000000002', '{}', 0
  ),
  (
    '48200000-0000-4000-8000-000000000103',
    '48200000-0000-4000-8000-000000000003', '{}', 0
  );

insert into private.reviews (
  id, data_id, data_version, state_code, reviewer_id, json,
  review_kind, target_table, submitted_revision_checksum, target_owner_id,
  created_at, modified_at
)
select
  fixture.root_review_id, fixture.root_review_id, '01.00.000',
  fixture.root_state_code,
  '[]',
  pg_catalog.jsonb_build_object(
    'review_kind', 'root',
    'data', pg_catalog.jsonb_build_object(
      'id', fixture.root_review_id,
      'version', '01.00.000',
      'table', 'processes',
      'name', pg_catalog.jsonb_build_object(
        'baseName', pg_catalog.jsonb_build_object(
          'en', 'Root ' || fixture.root_review_id::text
        )
      )
    ),
    'user', pg_catalog.jsonb_build_object(
      'id', '48200000-0000-4000-8000-000000000001',
      'name', 'Queue Owner'
    )
  ),
  'root', 'processes',
  private.review_revision_fingerprint_v1(
    'processes',
    api.cmd_review_get_dataset_row(
      'processes', fixture.root_review_id, '01.00.000', false
    )
  ),
  '48200000-0000-4000-8000-000000000001', now(), now()
from (values
  ('48200000-0000-4000-8000-000000000111'::uuid, 1),
  ('48200000-0000-4000-8000-000000000112'::uuid, 0),
  ('48200000-0000-4000-8000-000000000113'::uuid, 1)
) as fixture(root_review_id, root_state_code);

select has_function(
  'api', 'qry_review_get_admin_root_queue_items_v2',
  array['text', 'integer', 'integer', 'text', 'text'],
  'grouped Review Admin queue function exists'
);
select has_function(
  'api', 'qry_review_get_member_root_queue_items_v2',
  array['text', 'integer', 'integer', 'text', 'text'],
  'grouped Review Member queue function exists'
);
select has_function(
  'api', 'qry_review_get_admin_queue_items_v3',
  array['text', 'integer', 'integer', 'text', 'text'],
  'flat Review Admin queue function exists'
);
select has_function(
  'api', 'qry_review_get_member_queue_items_v3',
  array['text', 'integer', 'integer', 'text', 'text'],
  'flat Review Member queue function exists'
);
select has_index(
  'private', 'reviews', 'reviews_review_queue_status_modified_idx',
  'flat Review queues have a status and modified-time index'
);
select has_index(
  'private', 'comments', 'comments_reviewer_queue_state_review_idx',
  'Member Review queues have a reviewer and state index'
);
select has_function(
  'api', 'qry_root_review_reference_progress_v2', array['uuid'],
  'versioned root child query function exists'
);
select has_function(
  'api', 'qry_root_review_reference_progress', array['uuid'],
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
    from api.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Admin unassigned queue includes roots matched directly or through a child'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_review_get_admin_root_queue_items_v2(
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
    from api.qry_review_get_admin_root_queue_items_v2(
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
    from api.qry_review_get_admin_root_queue_items_v2(
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
    from api.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Admin total_count counts grouped roots rather than individual reviews'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 1, 'modified_at', 'desc'
    )
  ),
  1,
  'Admin pagination is applied after root grouping'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from api.qry_review_get_admin_root_queue_items_v2(
      'unassigned', 1, 1, 'modified_at', 'desc'
    )
  ),
  2,
  'paginated Admin rows retain the grouped total'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'flat Admin queue lists the matching Root and Reference as independent rows'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where review_kind = 'root'
  ),
  1,
  'flat Admin queue retains the directly matching Root row'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'unassigned', 1, 10, 'modified_at', 'desc'
    )
    where review_kind = 'reference'
  ),
  1,
  'flat Admin queue retains the shared Reference only once'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'unassigned', 1, 1, 'modified_at', 'desc'
    )
  ),
  2,
  'flat Admin total_count counts independent Reviews before pagination'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  3,
  'Review Admin sees all current Reference Reviews under a root'
);
select ok(
  pg_catalog.strpos(
    pg_catalog.pg_get_function_result(
      'api.qry_root_review_reference_progress_v2(uuid)'::regprocedure
    ),
    'relation_paths'
  ) = 0,
  'the child-row contract does not expose relation paths'
);
select is(
  (
    select child_a.reference_review_id
    from api.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    ) as child_a
    where child_a.reference_review_id =
      '48200000-0000-4000-8000-000000000101'
  ),
  (
    select child_b.reference_review_id
    from api.qry_root_review_reference_progress_v2(
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
    from api.qry_review_get_member_root_queue_items_v2(
      'pending', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'one assigned Reference Review places every related root in the member queue'
);
select ok(
  not exists (
    select 1
    from api.qry_review_get_member_root_queue_items_v2(
      'pending', 1, 10, 'modified_at', 'desc'
    )
    where root_matches_status or root_can_read
  ),
  'Reference-only member results do not expose root actions or full root access'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from api.qry_review_get_member_root_queue_items_v2(
      'pending', 1, 1, 'modified_at', 'desc'
    )
  ),
  2,
  'Review Member pagination also counts grouped roots'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_review_get_member_queue_items_v3(
      'pending', 1, 10, 'modified_at', 'desc'
    )
  ),
  1,
  'flat Member queue lists the assigned Reference once instead of related Roots'
);
select is(
  (
    select id
    from api.qry_review_get_member_queue_items_v3(
      'pending', 1, 10, 'modified_at', 'desc'
    )
  ),
  '48200000-0000-4000-8000-000000000102'::uuid,
  'flat Member queue preserves the assigned Reference Review identity'
);
select is(
  (
    select pg_catalog.max(total_count)::integer
    from api.qry_review_get_member_queue_items_v3(
      'pending', 1, 1, 'modified_at', 'desc'
    )
  ),
  1,
  'flat Member total_count counts independent actor tasks'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from api.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  1,
  'Review Member child query hides unassigned and sibling reviewer tasks'
);
select is(
  (
    select reference_review_id
    from api.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  '48200000-0000-4000-8000-000000000102'::uuid,
  'Review Member sees only their assigned Reference Review id'
);
select is(
  (
    select actor_comment_state_code
    from api.qry_root_review_reference_progress_v2(
      '48200000-0000-4000-8000-000000000111'
    )
  ),
  0,
  'member child row includes the actor comment state needed for tab matching'
);

reset role;

update private.comments
set state_code = 1
where review_id = '48200000-0000-4000-8000-000000000102'
  and reviewer_id = '48200000-0000-4000-8000-000000000002';
set local role authenticated;
select is(
  (
    select count(*)::integer
    from api.qry_review_get_member_root_queue_items_v2(
      'reviewed', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Member reviewed tab includes every root that currently derives a reviewed child'
);
select is(
  (
    select count(*)::integer
    from api.qry_review_get_member_queue_items_v3(
      'reviewed', 1, 10, 'modified_at', 'desc'
    )
  ),
  1,
  'flat Member reviewed tab lists the reviewed Reference itself'
);
reset role;

update private.reviews
set state_code = -1
where id = '48200000-0000-4000-8000-000000000102';
update private.comments
set state_code = -1
where review_id = '48200000-0000-4000-8000-000000000102'
  and reviewer_id = '48200000-0000-4000-8000-000000000002';
set local role authenticated;
select is(
  (
    select count(*)::integer
    from api.qry_review_get_member_root_queue_items_v2(
      'reviewer-rejected', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Member rejected tab requires both rejected Review and rejected actor Comment'
);
select is(
  (
    select count(*)::integer
    from api.qry_review_get_member_queue_items_v3(
      'reviewer-rejected', 1, 10, 'modified_at', 'desc'
    )
  ),
  1,
  'flat Member rejected tab lists the rejected Reference itself'
);
reset role;

select set_config(
  'request.jwt.claim.sub',
  '48200000-0000-4000-8000-000000000004',
  true
);
set local role authenticated;
select is(
  (
    select count(*)::integer
    from api.qry_review_get_admin_root_queue_items_v2(
      'assigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'Admin assigned tab groups direct assigned roots and assigned children'
);
select is(
  (
    select count(*)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'assigned', 1, 10, 'modified_at', 'desc'
    )
  ),
  3,
  'flat Admin assigned tab lists two matching Roots and one matching Reference'
);
reset role;

update private.reviews
set state_code = -1
where id = '48200000-0000-4000-8000-000000000101';
update private.reviews
set state_code = -1
where id = '48200000-0000-4000-8000-000000000112';
set local role authenticated;
select is(
  (
    select count(*)::integer
    from api.qry_review_get_admin_root_queue_items_v2(
      'admin-rejected', 1, 10, 'modified_at', 'desc'
    )
  ),
  3,
  'Admin rejected tab groups roots matched directly or by rejected children'
);
select is(
  (
    select root_matches_status
    from api.qry_review_get_admin_root_queue_items_v2(
      'admin-rejected', 1, 10, 'modified_at', 'desc'
    )
    where id = '48200000-0000-4000-8000-000000000112'
  ),
  true,
  'Admin rejected tab marks a directly rejected root as a direct match'
);
select is(
  (
    select root_matches_status
    from api.qry_review_get_admin_root_queue_items_v2(
      'admin-rejected', 1, 10, 'modified_at', 'desc'
    )
    where id = '48200000-0000-4000-8000-000000000113'
  ),
  false,
  'Admin rejected tab marks a root included only by its rejected child'
);
select is(
  (
    select count(*)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'admin-rejected', 1, 10, 'modified_at', 'desc'
    )
  ),
  3,
  'flat Admin rejected tab lists one Root and two References independently'
);
reset role;

set local session_replication_role = replica;
update public.processes
set json_ordered = '{"processDataSet":{}}'::json,
    modified_at = now()
where id = '48200000-0000-4000-8000-000000000113'
  and version = '01.00.000';
set local session_replication_role = origin;
set local role authenticated;
select is(
  (
    select count(*)::integer
    from api.qry_review_get_admin_root_queue_items_v2(
      'admin-rejected', 1, 10, 'modified_at', 'desc'
    )
  ),
  2,
  'stale business reviews[].id candidates are removed by current JSON validation'
);
select is(
  (
    select count(*)::integer
    from api.qry_review_get_admin_queue_items_v3(
      'admin-rejected', 1, 10, 'modified_at', 'desc'
    )
  ),
  3,
  'flat queue membership is independent from changing Root relationship candidates'
);
reset role;

set local session_replication_role = replica;
insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, reviews
)
values (
  '48200000-0000-4000-8000-000000000204', '01.00.000',
  '{"contactDataSet":{}}', '{}',
  '48200000-0000-4000-8000-000000000001', 20,
  '[{"key":0,"id":"48200000-0000-4000-8000-000000000113"}]'
);
update public.processes
set json_ordered = '{"processDataSet":{"fixtures":[{"@type":"contact data set","@refObjectId":"48200000-0000-4000-8000-000000000204","@version":"01.00.000"}]}}'::json
where id = '48200000-0000-4000-8000-000000000113'
  and version = '01.00.000';
set local session_replication_role = origin;
set local role authenticated;
select throws_ok(
  $$select * from api.qry_review_get_admin_root_queue_items_v2(
    'assigned', 1, 10, 'modified_at', 'desc'
  )$$,
  '55000',
  'MISSING_CURRENT_REFERENCE_REVIEW',
  'active roots fail closed when a current JSON target has no Reference Review'
);
reset role;

select set_config('request.jwt.claim.sub', '', true);
set local role authenticated;
select throws_ok(
  $$select * from api.qry_root_review_reference_progress_v2(
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
    'api.qry_review_get_admin_root_queue_items_v2(text,integer,integer,text,text)',
    'EXECUTE'
  ),
  'anonymous callers cannot execute the grouped Admin queue'
);
select ok(
  not has_function_privilege(
    'anon',
    'api.qry_review_get_member_root_queue_items_v2(text,integer,integer,text,text)',
    'EXECUTE'
  ),
  'anonymous callers cannot execute the grouped Member queue'
);
select ok(
  not has_function_privilege(
    'anon',
    'api.qry_review_get_admin_queue_items_v3(text,integer,integer,text,text)',
    'EXECUTE'
  ),
  'anonymous callers cannot execute the flat Admin queue'
);
select ok(
  not has_function_privilege(
    'anon',
    'api.qry_review_get_member_queue_items_v3(text,integer,integer,text,text)',
    'EXECUTE'
  ),
  'anonymous callers cannot execute the flat Member queue'
);
select is(
  (
    select pg_catalog.count(*)::integer
    from private.api_capability_grants
    where routine_identity in (
      'api.qry_review_get_admin_queue_items_v3(text, integer, integer, text, text)',
      'api.qry_review_get_member_queue_items_v3(text, integer, integer, text, text)'
    )
      and capability_id = 'NX-REV-01'
      and allow_authenticated
      and not allow_anon
      and not allow_service_role
  ),
  2,
  'the exact capability manifest admits both flat queues for authenticated only'
);

select * from finish();
rollback;
