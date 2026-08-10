-- Issue #446: derive current Root/Reference Review relationships without
-- persisting Root Review ID -> Reference Review ID bindings.

create or replace function private.review_candidate_root_ids_v1(
  p_target_table text,
  p_target_id uuid,
  p_target_version text
)
returns table (root_review_id uuid)
language plpgsql
stable
security definer
set search_path = ''
as $$
begin
  if p_target_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return;
  end if;

  return query execute pg_catalog.format(
    $query$
      select distinct (entry.value->>'id')::uuid
      from public.%I as dataset_row
      cross join lateral pg_catalog.jsonb_array_elements(
        case
          when pg_catalog.jsonb_typeof(dataset_row.reviews) = 'array'
            then dataset_row.reviews
          else '[]'::jsonb
        end
      ) as entry(value)
      where dataset_row.id = $1
        and pg_catalog.btrim(dataset_row.version::text) = $2
        and coalesce(entry.value->>'id', '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    $query$,
    p_target_table
  ) using p_target_id, p_target_version;
end;
$$;

alter function private.review_candidate_root_ids_v1(text, uuid, text)
  owner to postgres;
revoke all on function private.review_candidate_root_ids_v1(text, uuid, text)
  from public, anon, authenticated, service_role;
grant execute on function private.review_candidate_root_ids_v1(text, uuid, text)
  to api_internal_executor;

comment on function private.review_candidate_root_ids_v1(text, uuid, text) is
  'Reads append-only business reviews[].id Root candidates. Callers must validate every candidate against current JSON/comments.';

create or replace function private.review_derive_current_references_v1(
  p_root_review_ids uuid[]
)
returns table (
  root_review_id uuid,
  reference_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  submitted_revision_checksum text,
  state_code integer
)
language plpgsql
security definer
set search_path = 'pg_temp'
as $$
declare
  v_root private.reviews%rowtype;
  v_comment_roots jsonb;
begin
  create temporary table if not exists review_current_targets_v1 (
    root_review_id uuid not null,
    target_table text not null,
    data_id uuid not null,
    data_version text not null,
    dataset_row jsonb not null,
    primary key (root_review_id, target_table, data_id, data_version)
  ) on commit drop;

  truncate table review_current_targets_v1;

  for v_root in
    select root_review.*
    from private.reviews as root_review
    where root_review.review_kind = 'root'
      and root_review.id = any(coalesce(p_root_review_ids, array[]::uuid[]))
    order by root_review.id
  loop
    insert into review_current_targets_v1 (
      root_review_id,
      target_table,
      data_id,
      data_version,
      dataset_row
    )
    select
      v_root.id,
      target.table_name,
      target.dataset_id,
      target.dataset_version,
      target.dataset_row
    from api.cmd_review_collect_dataset_targets(
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'table', v_root.target_table,
        'id', v_root.data_id,
        'version', pg_catalog.btrim(v_root.data_version::text),
        'is_root', true
      )),
      false
    ) as target
    where not target.is_root
    on conflict do nothing;

    select coalesce(
      pg_catalog.jsonb_agg(distinct pg_catalog.jsonb_build_object(
        'table', api.cmd_review_ref_type_to_table(ref.ref_type),
        'id', ref.ref_object_id,
        'version', ref.ref_version,
        'is_root', false
      )),
      '[]'::jsonb
    )
    into v_comment_roots
    from private.comments as comment_row
    cross join lateral api.cmd_review_extract_refs(
      coalesce(comment_row.json::jsonb, '{}'::jsonb)
    ) as ref
    where comment_row.review_id = v_root.id
      and comment_row.state_code <> -2
      and api.cmd_review_ref_type_to_table(ref.ref_type) is not null;

    if pg_catalog.jsonb_array_length(v_comment_roots) > 0 then
      insert into review_current_targets_v1 (
        root_review_id,
        target_table,
        data_id,
        data_version,
        dataset_row
      )
      select
        v_root.id,
        target.table_name,
        target.dataset_id,
        target.dataset_version,
        target.dataset_row
      from api.cmd_review_collect_dataset_targets(
        v_comment_roots,
        false
      ) as target
      where not (
        target.table_name = v_root.target_table
        and target.dataset_id = v_root.data_id
        and target.dataset_version = pg_catalog.btrim(v_root.data_version::text)
      )
      on conflict do nothing;
    end if;
  end loop;

  return query
  select
    target.root_review_id,
    reference_review.id,
    target.target_table,
    target.data_id,
    target.data_version,
    reference_review.submitted_revision_checksum,
    reference_review.state_code
  from review_current_targets_v1 as target
  join lateral (
    select candidate.*
    from private.reviews as candidate
    where candidate.review_kind = 'reference'
      and candidate.target_table = target.target_table
      and candidate.data_id = target.data_id
      and pg_catalog.btrim(candidate.data_version::text) = target.data_version
      and candidate.submitted_revision_checksum
        = private.review_revision_fingerprint_v1(
            target.target_table,
            target.dataset_row
          )
      and candidate.state_code in (-1, 0, 1, 2)
    order by
      case when candidate.state_code in (0, 1, 2) then 0 else 1 end,
      candidate.modified_at desc,
      candidate.id
    limit 1
  ) as reference_review on true
  order by
    target.root_review_id,
    target.target_table,
    target.data_id,
    target.data_version;
end;
$$;

alter function private.review_derive_current_references_v1(uuid[])
  owner to postgres;
revoke all on function private.review_derive_current_references_v1(uuid[])
  from public, anon, authenticated, service_role;
grant execute on function private.review_derive_current_references_v1(uuid[])
  to api_internal_executor;

comment on function private.review_derive_current_references_v1(uuid[]) is
  'Derives current Root/Reference Review pairs from dataset JSON, non-revoked Reviewer comments, target identity, and revision checksum; persists no pair.';

create or replace function api.qry_root_review_reference_progress_v2(
  p_root_review_id uuid
)
returns table (
  reference_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  data_name jsonb,
  submitted_revision_checksum text,
  state_code integer,
  reviewer_count integer,
  completed_reviewer_count integer,
  actor_comment_state_code integer,
  actor_comment_modified_at timestamptz
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_is_admin boolean;
  v_is_member boolean;
begin
  v_is_admin := v_actor is not null
    and api.cmd_review_is_review_admin(v_actor);
  v_is_member := v_actor is not null
    and api.cmd_review_is_review_member(v_actor);

  if not v_is_admin and not v_is_member then
    raise exception using errcode = '42501', message = 'REVIEW_ROLE_REQUIRED';
  end if;

  return query
  select
    reference_review.id,
    derived.target_table,
    derived.data_id,
    derived.data_version,
    coalesce(reference_review.json #> '{data,name}', '{}'::jsonb),
    reference_review.submitted_revision_checksum,
    reference_review.state_code,
    pg_catalog.jsonb_array_length(
      coalesce(reference_review.reviewer_id, '[]'::jsonb)
    )::integer,
    (
      select pg_catalog.count(*)::integer
      from private.comments as completed_comment
      where completed_comment.review_id = reference_review.id
        and completed_comment.state_code in (1, -3, 2)
    ),
    actor_comment.state_code,
    actor_comment.modified_at
  from private.review_derive_current_references_v1(
    array[p_root_review_id]::uuid[]
  ) as derived
  join private.reviews as reference_review
    on reference_review.id = derived.reference_review_id
  left join lateral (
    select comment_row.state_code, comment_row.modified_at
    from private.comments as comment_row
    where comment_row.review_id = reference_review.id
      and comment_row.reviewer_id = v_actor
    order by comment_row.modified_at desc, comment_row.created_at desc
    limit 1
  ) as actor_comment on true
  where v_is_admin
    or (
      v_is_member
      and api.policy_review_can_read(reference_review.id, v_actor)
      and actor_comment.state_code is not null
      and actor_comment.state_code <> -2
    )
  order by derived.target_table, derived.data_id, derived.data_version;
end;
$$;

alter function api.qry_root_review_reference_progress_v2(uuid)
  owner to postgres;
revoke all on function api.qry_root_review_reference_progress_v2(uuid)
  from public, anon;
grant execute on function api.qry_root_review_reference_progress_v2(uuid)
  to authenticated, api_internal_executor;

create or replace function api.qry_reference_review_impacted_roots(
  p_reference_review_id uuid,
  p_include_history boolean default false
)
returns table (
  root_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  state_code integer,
  is_current boolean
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_reference private.reviews%rowtype;
  v_candidate_ids uuid[];
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  select review_row.*
  into v_reference
  from private.reviews as review_row
  where review_row.id = p_reference_review_id
    and review_row.review_kind = 'reference';

  if not found then
    return;
  end if;

  select coalesce(
    pg_catalog.array_agg(candidate.root_review_id order by candidate.root_review_id),
    array[]::uuid[]
  )
  into v_candidate_ids
  from (
    select hinted.root_review_id
    from private.review_candidate_root_ids_v1(
      v_reference.target_table,
      v_reference.data_id,
      pg_catalog.btrim(v_reference.data_version::text)
    ) as hinted
    union
    select root_review.id
    from private.reviews as root_review
    where root_review.review_kind = 'root'
  ) as candidate;

  return query
  select
    root_review.id,
    root_review.target_table,
    root_review.data_id,
    pg_catalog.btrim(root_review.data_version::text),
    root_review.state_code,
    true
  from private.review_derive_current_references_v1(v_candidate_ids) as derived
  join private.reviews as root_review
    on root_review.id = derived.root_review_id
    and root_review.review_kind = 'root'
  where derived.reference_review_id = p_reference_review_id
  order by root_review.modified_at desc, root_review.id;
end;
$$;

alter function api.qry_reference_review_impacted_roots(uuid, boolean)
  owner to postgres;
revoke all on function api.qry_reference_review_impacted_roots(uuid, boolean)
  from public, anon, authenticated, service_role;
grant execute on function api.qry_reference_review_impacted_roots(uuid, boolean)
  to api_internal_executor;

comment on function api.qry_reference_review_impacted_roots(uuid, boolean) is
  'Returns current dynamically validated impacted roots. p_include_history is retained for signature compatibility and does not restore historical relationships.';

create or replace function api.qry_review_get_admin_root_queue_items_v2(
  p_status text default null,
  p_page integer default 1,
  p_page_size integer default 10,
  p_sort_by text default 'modified_at',
  p_sort_order text default 'desc'
)
returns table (
  id uuid,
  data_id uuid,
  data_version text,
  state_code integer,
  review_kind text,
  target_table text,
  reviewer_id jsonb,
  "json" jsonb,
  deadline timestamptz,
  created_at timestamptz,
  modified_at timestamptz,
  comment_state_codes jsonb,
  root_matches_status boolean,
  root_can_read boolean,
  total_count bigint
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_sort_key text := case pg_catalog.lower(coalesce(p_sort_by, ''))
    when 'created_at' then 'created_at'
    when 'createat' then 'created_at'
    when 'deadline' then 'deadline'
    when 'state_code' then 'state_code'
    when 'statecode' then 'state_code'
    else 'modified_at'
  end;
  v_order_dir text := api.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := pg_catalog.lower(coalesce(p_status, ''));
  v_state_code integer;
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    return;
  end if;

  case v_status
    when '', 'all' then v_state_code := null;
    when 'unassigned' then v_state_code := 0;
    when 'assigned' then v_state_code := 1;
    when 'admin-rejected' then v_state_code := -1;
    else return;
  end case;

  return query
  with matching_reference_tasks as materialized (
    select reference_review.*
    from private.reviews as reference_review
    where v_state_code is not null
      and reference_review.review_kind = 'reference'
      and reference_review.state_code = v_state_code
  ),
  direct_roots as materialized (
    select root_review.id
    from private.reviews as root_review
    where root_review.review_kind = 'root'
      and (v_state_code is null or root_review.state_code = v_state_code)
  ),
  hinted_roots as materialized (
    select distinct candidate.root_review_id as id
    from matching_reference_tasks as reference_review
    cross join lateral private.review_candidate_root_ids_v1(
      reference_review.target_table,
      reference_review.data_id,
      pg_catalog.btrim(reference_review.data_version::text)
    ) as candidate
  ),
  candidate_roots as materialized (
    select direct_root.id from direct_roots as direct_root
    union
    select hinted_root.id from hinted_roots as hinted_root
    union
    select root_review.id
    from private.reviews as root_review
    where root_review.review_kind = 'root'
  ),
  derived_references as materialized (
    select derived.*
    from private.review_derive_current_references_v1(
      coalesce(
        (select pg_catalog.array_agg(candidate.id order by candidate.id) from candidate_roots as candidate),
        array[]::uuid[]
      )
    ) as derived
  ),
  root_matches as (
    select
      root_review.id as root_review_id,
      true as root_matches_status,
      root_review.modified_at as matching_task_modified_at
    from private.reviews as root_review
    join direct_roots as direct_root on direct_root.id = root_review.id

    union all

    select
      derived.root_review_id,
      false,
      reference_review.modified_at
    from derived_references as derived
    join matching_reference_tasks as reference_review
      on reference_review.id = derived.reference_review_id
  ),
  grouped_roots as (
    select
      root_match.root_review_id,
      pg_catalog.bool_or(root_match.root_matches_status) as root_matches_status,
      pg_catalog.max(root_match.matching_task_modified_at) as matching_task_modified_at
    from root_matches as root_match
    group by root_match.root_review_id
  ),
  q as (
    select
      root_review.id,
      root_review.data_id,
      pg_catalog.btrim(root_review.data_version::text) as data_version,
      root_review.state_code,
      root_review.review_kind,
      root_review.target_table,
      coalesce(root_review.reviewer_id, '[]'::jsonb) as reviewer_id,
      coalesce(root_review.json, '{}'::jsonb) as json,
      root_review.deadline,
      root_review.created_at,
      grouped.matching_task_modified_at as modified_at,
      coalesce(root_comments.comment_state_codes, '[]'::jsonb) as comment_state_codes,
      grouped.root_matches_status,
      true as root_can_read
    from grouped_roots as grouped
    join private.reviews as root_review
      on root_review.id = grouped.root_review_id
      and root_review.review_kind = 'root'
    left join lateral (
      select pg_catalog.jsonb_agg(
        pg_catalog.to_jsonb(root_comment.state_code)
        order by root_comment.created_at, root_comment.reviewer_id
      ) filter (where root_comment.reviewer_id is not null) as comment_state_codes
      from private.comments as root_comment
      where root_comment.review_id = root_review.id
    ) as root_comments on true
  )
  select q.*, pg_catalog.count(*) over() as total_count
  from q
  order by
    case when v_sort_key = 'created_at' and v_order_dir = 'asc' then q.created_at end asc nulls last,
    case when v_sort_key = 'created_at' and v_order_dir = 'desc' then q.created_at end desc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'asc' then q.deadline end asc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'desc' then q.deadline end desc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'asc' then q.state_code end asc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'desc' then q.state_code end desc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'asc' then q.modified_at end asc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'desc' then q.modified_at end desc nulls last,
    q.id
  limit v_limit offset v_offset;
end;
$$;

alter function api.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) owner to postgres;
revoke all on function api.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) from public, anon;
grant execute on function api.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) to authenticated, api_internal_executor;

create or replace function api.qry_review_get_member_root_queue_items_v2(
  p_status text default 'pending',
  p_page integer default 1,
  p_page_size integer default 10,
  p_sort_by text default 'modified_at',
  p_sort_order text default 'desc'
)
returns table (
  id uuid,
  data_id uuid,
  data_version text,
  review_state_code integer,
  review_kind text,
  target_table text,
  reviewer_id jsonb,
  "json" jsonb,
  deadline timestamptz,
  created_at timestamptz,
  modified_at timestamptz,
  comment_state_code integer,
  comment_json jsonb,
  comment_created_at timestamptz,
  comment_modified_at timestamptz,
  root_matches_status boolean,
  root_can_read boolean,
  total_count bigint
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_sort_key text := case pg_catalog.lower(coalesce(p_sort_by, ''))
    when 'created_at' then 'created_at'
    when 'createat' then 'created_at'
    when 'deadline' then 'deadline'
    when 'state_code' then 'state_code'
    when 'statecode' then 'state_code'
    when 'comment_modified_at' then 'comment_modified_at'
    when 'commentmodifiedat' then 'comment_modified_at'
    else 'modified_at'
  end;
  v_order_dir text := api.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := pg_catalog.lower(coalesce(p_status, 'pending'));
begin
  if v_actor is null or not api.cmd_review_is_review_member(v_actor) then
    return;
  end if;
  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;

  return query
  with matching_tasks as materialized (
    select
      review_row.id,
      review_row.review_kind,
      review_row.target_table,
      review_row.data_id,
      pg_catalog.btrim(review_row.data_version::text) as data_version,
      comment_row.state_code as comment_state_code,
      coalesce(comment_row.json::jsonb, '{}'::jsonb) as comment_json,
      comment_row.created_at as comment_created_at,
      comment_row.modified_at as comment_modified_at,
      greatest(review_row.modified_at, comment_row.modified_at) as task_modified_at
    from private.comments as comment_row
    join private.reviews as review_row on review_row.id = comment_row.review_id
    where review_row.review_kind in ('root', 'reference')
      and comment_row.reviewer_id = v_actor
      and api.policy_review_can_read(review_row.id, v_actor)
      and (
        (v_status = 'pending' and comment_row.state_code = 0 and review_row.state_code > 0)
        or (v_status = 'reviewed' and comment_row.state_code = any(array[1, 2, -3]) and review_row.state_code > 0)
        or (v_status = 'reviewer-rejected' and comment_row.state_code = -1 and review_row.state_code = -1)
      )
  ),
  direct_roots as materialized (
    select task.id
    from matching_tasks as task
    where task.review_kind = 'root'
  ),
  hinted_roots as materialized (
    select distinct candidate.root_review_id as id
    from matching_tasks as task
    cross join lateral private.review_candidate_root_ids_v1(
      task.target_table,
      task.data_id,
      task.data_version
    ) as candidate
    where task.review_kind = 'reference'
  ),
  candidate_roots as materialized (
    select direct_root.id from direct_roots as direct_root
    union
    select hinted_root.id from hinted_roots as hinted_root
    union
    select root_review.id
    from private.reviews as root_review
    where root_review.review_kind = 'root'
  ),
  derived_references as materialized (
    select derived.*
    from private.review_derive_current_references_v1(
      coalesce(
        (select pg_catalog.array_agg(candidate.id order by candidate.id) from candidate_roots as candidate),
        array[]::uuid[]
      )
    ) as derived
  ),
  root_matches as (
    select task.id as root_review_id, true as root_matches_status, task.task_modified_at
    from matching_tasks as task
    where task.review_kind = 'root'

    union all

    select derived.root_review_id, false, task.task_modified_at
    from derived_references as derived
    join matching_tasks as task
      on task.id = derived.reference_review_id
      and task.review_kind = 'reference'
  ),
  grouped_roots as (
    select
      root_match.root_review_id,
      pg_catalog.bool_or(root_match.root_matches_status) as root_matches_status,
      pg_catalog.max(root_match.task_modified_at) as matching_task_modified_at
    from root_matches as root_match
    group by root_match.root_review_id
  ),
  q as (
    select
      root_review.id,
      root_review.data_id,
      pg_catalog.btrim(root_review.data_version::text) as data_version,
      root_review.state_code,
      root_review.review_kind,
      root_review.target_table,
      coalesce(root_review.reviewer_id, '[]'::jsonb) as reviewer_id,
      case
        when api.policy_review_can_read(root_review.id, v_actor) then coalesce(root_review.json, '{}'::jsonb)
        else pg_catalog.jsonb_build_object(
          'review_kind', 'root',
          'data', pg_catalog.jsonb_build_object(
            'id', root_review.data_id,
            'version', pg_catalog.btrim(root_review.data_version::text),
            'name', root_review.json #> '{data,name}',
            'table', root_review.target_table
          ),
          'user', pg_catalog.jsonb_build_object(
            'id', root_review.target_owner_id,
            'name', root_review.json #> '{user,name}'
          ),
          'team', pg_catalog.jsonb_build_object(
            'id', root_review.target_team_id,
            'name', root_review.json #> '{team,name}'
          )
        )
      end as json,
      root_review.deadline,
      root_review.created_at,
      grouped.matching_task_modified_at as modified_at,
      direct_task.comment_state_code,
      coalesce(direct_task.comment_json, '{}'::jsonb) as comment_json,
      direct_task.comment_created_at,
      grouped.matching_task_modified_at as comment_modified_at,
      grouped.root_matches_status,
      api.policy_review_can_read(root_review.id, v_actor) as root_can_read
    from grouped_roots as grouped
    join private.reviews as root_review
      on root_review.id = grouped.root_review_id
      and root_review.review_kind = 'root'
    left join matching_tasks as direct_task
      on direct_task.id = root_review.id
      and direct_task.review_kind = 'root'
  )
  select
    q.id,
    q.data_id,
    q.data_version,
    q.state_code,
    q.review_kind,
    q.target_table,
    q.reviewer_id,
    q.json,
    q.deadline,
    q.created_at,
    q.modified_at,
    q.comment_state_code,
    q.comment_json,
    q.comment_created_at,
    q.comment_modified_at,
    q.root_matches_status,
    q.root_can_read,
    pg_catalog.count(*) over()
  from q
  order by
    case when v_sort_key = 'created_at' and v_order_dir = 'asc' then q.created_at end asc nulls last,
    case when v_sort_key = 'created_at' and v_order_dir = 'desc' then q.created_at end desc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'asc' then q.deadline end asc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'desc' then q.deadline end desc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'asc' then q.state_code end asc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'desc' then q.state_code end desc nulls last,
    case when v_sort_key = 'comment_modified_at' and v_order_dir = 'asc' then q.comment_modified_at end asc nulls last,
    case when v_sort_key = 'comment_modified_at' and v_order_dir = 'desc' then q.comment_modified_at end desc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'asc' then q.modified_at end asc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'desc' then q.modified_at end desc nulls last,
    q.id
  limit v_limit offset v_offset;
end;
$$;

alter function api.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) owner to postgres;
revoke all on function api.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) from public, anon;
grant execute on function api.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) to authenticated, api_internal_executor;

comment on function api.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) is
  'Current-state Admin queue grouped by Root Review. Root/Reference pairs are derived from JSON/comments and are not persisted.';
comment on function api.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) is
  'Current-state Member queue grouped by Root Review. Derived child matches can include a parent even when the Root itself does not match.';

-- Replace the initial compatibility implementation with the set-oriented
-- resolver used by every queue RPC. The recursive CTE handles all requested
-- roots in one statement and deduplicates cycles by dataset identity.
create or replace function private.review_resolve_current_reference_targets_v1(
  p_root_review_ids uuid[]
)
returns table (
  root_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  revision_checksum text,
  provenance jsonb
)
language sql
security definer
set search_path = ''
as $$
  with recursive requested_roots as materialized (
    select review_row.*
    from private.reviews as review_row
    where review_row.review_kind = 'root'
      and review_row.id = any(coalesce(p_root_review_ids, array[]::uuid[]))
  ),
  seed_targets as (
    select
      root_review.id as root_review_id,
      root_review.target_table,
      root_review.data_id,
      pg_catalog.btrim(root_review.data_version::text) as data_version,
      true as is_root,
      api.cmd_review_get_dataset_row(
        root_review.target_table,
        root_review.data_id,
        pg_catalog.btrim(root_review.data_version::text),
        false
      ) as dataset_row
    from requested_roots as root_review

    union

    select
      root_review.id,
      api.cmd_review_ref_type_to_table(ref.ref_type),
      ref.ref_object_id,
      ref.ref_version,
      false,
      api.cmd_review_get_dataset_row(
        api.cmd_review_ref_type_to_table(ref.ref_type),
        ref.ref_object_id,
        ref.ref_version,
        false
      )
    from requested_roots as root_review
    join private.comments as comment_row
      on comment_row.review_id = root_review.id
      and comment_row.state_code <> -2
    cross join lateral api.cmd_review_extract_refs(
      coalesce(comment_row.json::jsonb, '{}'::jsonb)
    ) as ref
    where api.cmd_review_ref_type_to_table(ref.ref_type) is not null
  ),
  closure (
    root_review_id,
    target_table,
    data_id,
    data_version,
    is_root,
    dataset_row
  ) as (
    select
      seed.root_review_id,
      seed.target_table,
      seed.data_id,
      seed.data_version,
      seed.is_root,
      seed.dataset_row
    from seed_targets as seed
    where seed.dataset_row is not null

    union

    select
      current_target.root_review_id,
      neighbour.target_table,
      neighbour.data_id,
      neighbour.data_version,
      false,
      dataset.dataset_row
    from closure as current_target
    cross join lateral (
      select
        api.cmd_review_ref_type_to_table(ref.ref_type) as target_table,
        ref.ref_object_id as data_id,
        ref.ref_version as data_version
      from (
        select * from api.cmd_review_extract_refs(
          coalesce(current_target.dataset_row->'json_ordered', '{}'::jsonb)
        )
        union
        select * from api.cmd_review_extract_refs(
          coalesce(current_target.dataset_row->'json', '{}'::jsonb)
        )
        union
        select * from api.cmd_review_extract_refs(
          coalesce(current_target.dataset_row->'json_tg', '{}'::jsonb)
        )
      ) as ref
      where (
          current_target.is_root
          or coalesce((current_target.dataset_row->>'state_code')::integer, 0) < 100
        )
        and api.cmd_review_ref_type_to_table(ref.ref_type) is not null

      union

      select
        'lifecyclemodels',
        current_target.data_id,
        current_target.data_version
      where current_target.target_table = 'processes'
        and not current_target.is_root

      union

      select
        'processes',
        current_target.data_id,
        current_target.data_version
      where current_target.target_table = 'lifecyclemodels'
        and current_target.is_root

      union

      select
        'processes',
        (submodel.value->>'id')::uuid,
        coalesce(
          nullif(submodel.value->>'version', ''),
          current_target.data_version
        )
      from pg_catalog.jsonb_array_elements(
        case
          when pg_catalog.jsonb_typeof(
            current_target.dataset_row->'json_tg'->'submodels'
          ) = 'array'
            then current_target.dataset_row->'json_tg'->'submodels'
          else '[]'::jsonb
        end
      ) as submodel(value)
      where current_target.target_table = 'lifecyclemodels'
        and (
          current_target.is_root
          or coalesce((current_target.dataset_row->>'state_code')::integer, 0) < 100
        )
        and pg_catalog.lower(coalesce(submodel.value->>'type', '')) = 'secondary'
        and coalesce(submodel.value->>'id', '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) as neighbour
    cross join lateral (
      select api.cmd_review_get_dataset_row(
        neighbour.target_table,
        neighbour.data_id,
        neighbour.data_version,
        false
      ) as dataset_row
    ) as dataset
    where dataset.dataset_row is not null
      and not (
        neighbour.target_table = current_target.target_table
        and neighbour.data_id = current_target.data_id
        and neighbour.data_version = current_target.data_version
      )
  )
  select
    current_target.root_review_id,
    current_target.target_table,
    current_target.data_id,
    current_target.data_version,
    private.review_revision_fingerprint_v1(
      current_target.target_table,
      current_target.dataset_row
    ),
    pg_catalog.jsonb_build_array('current_json_or_comment')
  from closure as current_target
  join requested_roots as root_review
    on root_review.id = current_target.root_review_id
  where not current_target.is_root
    and not (
      current_target.target_table = root_review.target_table
      and current_target.data_id = root_review.data_id
      and current_target.data_version = pg_catalog.btrim(root_review.data_version::text)
    )
  group by
    current_target.root_review_id,
    current_target.target_table,
    current_target.data_id,
    current_target.data_version,
    current_target.dataset_row
$$;

alter function private.review_resolve_current_reference_targets_v1(uuid[])
  owner to postgres;
revoke all on function private.review_resolve_current_reference_targets_v1(uuid[])
  from public, anon, authenticated, service_role;
grant execute on function private.review_resolve_current_reference_targets_v1(uuid[])
  to api_internal_executor;

create or replace function private.review_derive_current_references_v1(
  p_root_review_ids uuid[]
)
returns table (
  root_review_id uuid,
  reference_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  submitted_revision_checksum text,
  state_code integer
)
language plpgsql
security definer
set search_path = ''
as $$
begin
  if exists (
    select 1
    from private.review_resolve_current_reference_targets_v1(
      p_root_review_ids
    ) as target
    join private.reviews as root_review
      on root_review.id = target.root_review_id
      and root_review.state_code in (0, 1)
    where not exists (
      select 1
      from private.reviews as candidate
      where candidate.review_kind = 'reference'
        and candidate.target_table = target.target_table
        and candidate.data_id = target.data_id
        and pg_catalog.btrim(candidate.data_version::text) = target.data_version
        and candidate.submitted_revision_checksum = target.revision_checksum
        and candidate.state_code in (-1, 0, 1, 2)
    )
  ) then
    raise exception using
      errcode = '55000',
      message = 'MISSING_CURRENT_REFERENCE_REVIEW';
  end if;

  return query
  select
    target.root_review_id,
    reference_review.id,
    target.target_table,
    target.data_id,
    target.data_version,
    reference_review.submitted_revision_checksum,
    reference_review.state_code
  from private.review_resolve_current_reference_targets_v1(
    p_root_review_ids
  ) as target
  join lateral (
    select candidate.*
    from private.reviews as candidate
    where candidate.review_kind = 'reference'
      and candidate.target_table = target.target_table
      and candidate.data_id = target.data_id
      and pg_catalog.btrim(candidate.data_version::text) = target.data_version
      and candidate.submitted_revision_checksum = target.revision_checksum
      and candidate.state_code in (-1, 0, 1, 2)
    order by
      case when candidate.state_code in (0, 1, 2) then 0 else 1 end,
      candidate.modified_at desc,
      candidate.id
    limit 1
  ) as reference_review on true
  order by target.root_review_id, target.target_table,
    target.data_id, target.data_version;
end;
$$;

alter function private.review_derive_current_references_v1(uuid[])
  owner to postgres;
revoke all on function private.review_derive_current_references_v1(uuid[])
  from public, anon, authenticated, service_role;
grant execute on function private.review_derive_current_references_v1(uuid[])
  to api_internal_executor;

comment on function private.review_resolve_current_reference_targets_v1(uuid[]) is
  'Set-oriented recursive current JSON/comment closure for multiple Root Reviews; returns identities/checksums only and persists no relationship.';
