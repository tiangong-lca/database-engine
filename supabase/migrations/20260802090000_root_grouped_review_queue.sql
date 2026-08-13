-- Group review-management queues by Root Review while preserving the existing
-- row-oriented queue RPCs for compatibility with other consumers.

-- Normalize every supported ILCD dataset name into the Process/Flow-style
-- shape consumed by the review UI.  The previous implementation looked for a
-- literal `name` field on the four Common-schema datasets, so their review
-- snapshots contained an empty name object.
create or replace function public.cmd_review_get_dataset_name(
  p_table text,
  p_row jsonb
) returns jsonb
language sql
immutable
set search_path = ''
as $$
  select case p_table
    when 'contacts' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,contactDataSet,contactInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json,contactDataSet,contactInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,contactDataSet,contactInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,contactDataSet,contactInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'sources' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,sourceDataSet,sourceInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,sourceDataSet,sourceInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'unitgroups' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'flowproperties' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'flows' then coalesce(
      p_row#>'{json,flowDataSet,flowInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,flowDataSet,flowInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'processes' then coalesce(
      p_row#>'{json,processDataSet,processInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,processDataSet,processInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'lifecyclemodels' then coalesce(
      p_row#>'{json,lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    else '{}'::jsonb
  end
$$;

alter function public.cmd_review_get_dataset_name(text, jsonb) owner to postgres;

create or replace function public.qry_review_get_admin_root_queue_items_v2(
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
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_sort_key text := case lower(coalesce(p_sort_by, ''))
    when 'created_at' then 'created_at'
    when 'createat' then 'created_at'
    when 'deadline' then 'deadline'
    when 'state_code' then 'state_code'
    when 'statecode' then 'state_code'
    else 'modified_at'
  end;
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, ''));
  v_state_code integer;
begin
  if v_actor is null or not public.cmd_review_is_review_admin(v_actor) then
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
      with root_matches as (
        select
          root_review.id as root_review_id,
          true as root_matches_status,
          root_review.modified_at as matching_task_modified_at
        from public.reviews as root_review
        where root_review.review_kind = 'root'
          and (v_state_code is null or root_review.state_code = v_state_code)

        union all

        select
          root_review.id as root_review_id,
          false as root_matches_status,
          reference_review.modified_at as matching_task_modified_at
        from public.reviews as reference_review
        join public.reviews as root_review
          on root_review.review_kind = 'root'
          and root_review.current_reference_review_ids
            @> array[reference_review.id]::uuid[]
        where v_state_code is not null
          and reference_review.review_kind = 'reference'
          and reference_review.state_code = v_state_code
      ),
      root_candidates as (
        select
          root_match.root_review_id,
          pg_catalog.bool_or(root_match.root_matches_status)
            as root_matches_status,
          pg_catalog.max(root_match.matching_task_modified_at)
            as matching_task_modified_at
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
          root_candidate.matching_task_modified_at as modified_at,
          coalesce(root_comments.comment_state_codes, '[]'::jsonb)
            as comment_state_codes,
          root_candidate.root_matches_status,
          true as root_can_read
        from root_candidates as root_candidate
        join public.reviews as root_review
          on root_review.id = root_candidate.root_review_id
          and root_review.review_kind = 'root'
        left join lateral (
          select pg_catalog.jsonb_agg(
            pg_catalog.to_jsonb(root_comment.state_code)
            order by root_comment.created_at, root_comment.reviewer_id
          ) filter (where root_comment.reviewer_id is not null)
            as comment_state_codes
          from public.comments as root_comment
          where root_comment.review_id = root_review.id
        ) as root_comments on true
      )
      select q.*, pg_catalog.count(*) over() as total_count
      from q
      order by
        case when v_sort_key = 'created_at' and v_order_dir = 'asc'
          then q.created_at end asc nulls last,
        case when v_sort_key = 'created_at' and v_order_dir = 'desc'
          then q.created_at end desc nulls last,
        case when v_sort_key = 'deadline' and v_order_dir = 'asc'
          then q.deadline end asc nulls last,
        case when v_sort_key = 'deadline' and v_order_dir = 'desc'
          then q.deadline end desc nulls last,
        case when v_sort_key = 'state_code' and v_order_dir = 'asc'
          then q.state_code end asc nulls last,
        case when v_sort_key = 'state_code' and v_order_dir = 'desc'
          then q.state_code end desc nulls last,
        case when v_sort_key = 'modified_at' and v_order_dir = 'asc'
          then q.modified_at end asc nulls last,
        case when v_sort_key = 'modified_at' and v_order_dir = 'desc'
          then q.modified_at end desc nulls last,
        q.id asc
      limit v_limit offset v_offset;
end;
$$;

alter function public.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) owner to postgres;
revoke all on function public.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) from public, anon;
grant execute on function public.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) to authenticated, service_role;

create or replace function public.qry_review_get_member_root_queue_items_v2(
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
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_sort_key text := case lower(coalesce(p_sort_by, ''))
    when 'created_at' then 'created_at'
    when 'createat' then 'created_at'
    when 'deadline' then 'deadline'
    when 'state_code' then 'state_code'
    when 'statecode' then 'state_code'
    when 'comment_modified_at' then 'comment_modified_at'
    when 'commentmodifiedat' then 'comment_modified_at'
    else 'modified_at'
  end;
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, 'pending'));
begin
  if v_actor is null or not public.cmd_review_is_review_member(v_actor) then
    return;
  end if;
  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;

  return query
      with matching_tasks as (
        select
          review_row.id,
          review_row.review_kind,
          comment_row.state_code as comment_state_code,
          coalesce(comment_row.json::jsonb, '{}'::jsonb) as comment_json,
          comment_row.created_at as comment_created_at,
          comment_row.modified_at as comment_modified_at,
          greatest(review_row.modified_at, comment_row.modified_at) as task_modified_at
        from public.comments as comment_row
        join public.reviews as review_row on review_row.id = comment_row.review_id
        where review_row.review_kind in ('root', 'reference')
          and comment_row.reviewer_id = v_actor
          and public.policy_review_can_read(review_row.id, v_actor)
          and (
            (v_status = 'pending' and comment_row.state_code = 0 and review_row.state_code > 0)
            or (v_status = 'reviewed'
              and comment_row.state_code = any (array[1, 2, -3])
              and review_row.state_code > 0)
            or (v_status = 'reviewer-rejected'
              and comment_row.state_code = -1
              and review_row.state_code = -1)
          )
      ),
      root_matches as (
        select
          task.id as root_review_id,
          true as root_matches_status,
          task.task_modified_at
        from matching_tasks as task
        where task.review_kind = 'root'

        union all

        select
          root_review.id as root_review_id,
          false as root_matches_status,
          task.task_modified_at
        from matching_tasks as task
        join public.reviews as root_review
          on root_review.review_kind = 'root'
          and root_review.current_reference_review_ids
            @> array[task.id]::uuid[]
        where task.review_kind = 'reference'
      ),
      root_candidates as (
        select
          root_match.root_review_id,
          pg_catalog.bool_or(root_match.root_matches_status)
            as root_matches_status,
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
            when public.policy_review_can_read(root_review.id, v_actor) then
              coalesce(root_review.json, '{}'::jsonb)
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
          root_candidate.matching_task_modified_at as modified_at,
          direct_task.comment_state_code,
          coalesce(direct_task.comment_json, '{}'::jsonb) as comment_json,
          direct_task.comment_created_at,
          root_candidate.matching_task_modified_at as comment_modified_at,
          root_candidate.root_matches_status,
          public.policy_review_can_read(root_review.id, v_actor) as root_can_read
        from root_candidates as root_candidate
        join public.reviews as root_review
          on root_review.id = root_candidate.root_review_id
          and root_review.review_kind = 'root'
        left join matching_tasks as direct_task
          on direct_task.id = root_review.id
          and direct_task.review_kind = 'root'
      )
      select
        q.id,
        q.data_id,
        q.data_version,
        q.state_code as review_state_code,
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
        pg_catalog.count(*) over() as total_count
      from q
      order by
        case when v_sort_key = 'created_at' and v_order_dir = 'asc'
          then q.created_at end asc nulls last,
        case when v_sort_key = 'created_at' and v_order_dir = 'desc'
          then q.created_at end desc nulls last,
        case when v_sort_key = 'deadline' and v_order_dir = 'asc'
          then q.deadline end asc nulls last,
        case when v_sort_key = 'deadline' and v_order_dir = 'desc'
          then q.deadline end desc nulls last,
        case when v_sort_key = 'state_code' and v_order_dir = 'asc'
          then q.state_code end asc nulls last,
        case when v_sort_key = 'state_code' and v_order_dir = 'desc'
          then q.state_code end desc nulls last,
        case when v_sort_key = 'comment_modified_at' and v_order_dir = 'asc'
          then q.comment_modified_at end asc nulls last,
        case when v_sort_key = 'comment_modified_at' and v_order_dir = 'desc'
          then q.comment_modified_at end desc nulls last,
        case when v_sort_key = 'modified_at' and v_order_dir = 'asc'
          then q.modified_at end asc nulls last,
        case when v_sort_key = 'modified_at' and v_order_dir = 'desc'
          then q.modified_at end desc nulls last,
        q.id asc
      limit v_limit offset v_offset;
end;
$$;

alter function public.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) owner to postgres;
revoke all on function public.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) from public, anon;
grant execute on function public.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) to authenticated, service_role;

create or replace function public.qry_root_review_reference_progress_v2(
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
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_is_admin boolean;
  v_is_member boolean;
begin
  v_is_admin := v_actor is not null
    and public.cmd_review_is_review_admin(v_actor);
  v_is_member := v_actor is not null
    and public.cmd_review_is_review_member(v_actor);

  if not v_is_admin and not v_is_member then
    raise exception using errcode = '42501', message = 'REVIEW_ROLE_REQUIRED';
  end if;

  return query
  select
    reference_review.id,
    reference_review.target_table,
    reference_review.data_id,
    pg_catalog.btrim(reference_review.data_version::text),
    coalesce(reference_review.json #> '{data,name}', '{}'::jsonb),
    reference_review.submitted_revision_checksum,
    reference_review.state_code,
    pg_catalog.jsonb_array_length(
      coalesce(reference_review.reviewer_id, '[]'::jsonb)
    )::integer,
    (
      select pg_catalog.count(*)::integer
      from public.comments as completed_comment
      where completed_comment.review_id = reference_review.id
        and completed_comment.state_code in (1, -3, 2)
    ),
    actor_comment.state_code,
    actor_comment.modified_at
  from public.reviews as root_review
  cross join lateral pg_catalog.unnest(
    coalesce(root_review.current_reference_review_ids, '{}'::uuid[])
  ) with ordinality as current_reference(reference_review_id, ordinal_position)
  join public.reviews as reference_review
    on reference_review.id = current_reference.reference_review_id
    and reference_review.review_kind = 'reference'
  left join lateral (
    select comment_row.state_code, comment_row.modified_at
    from public.comments as comment_row
    where comment_row.review_id = reference_review.id
      and comment_row.reviewer_id = v_actor
    order by comment_row.modified_at desc, comment_row.created_at desc
    limit 1
  ) as actor_comment on true
  where root_review.id = p_root_review_id
    and root_review.review_kind = 'root'
    and (
      v_is_admin
      or (
        v_is_member
        and public.policy_review_can_read(reference_review.id, v_actor)
        and actor_comment.state_code is not null
        and actor_comment.state_code <> -2
      )
    )
  order by
    current_reference.ordinal_position,
    reference_review.target_table,
    reference_review.id;
end;
$$;

alter function public.qry_root_review_reference_progress_v2(uuid) owner to postgres;
revoke all on function public.qry_root_review_reference_progress_v2(uuid)
  from public, anon;
grant execute on function public.qry_root_review_reference_progress_v2(uuid)
  to authenticated, service_role;

comment on function public.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) is
  'Server-paginated Review Admin queue with one row per Root Review; a matching current Reference Review includes its Root.';
comment on function public.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) is
  'Server-paginated Review Member queue grouped by Root Review without exposing unassigned sibling Reference Reviews.';
comment on function public.qry_root_review_reference_progress_v2(uuid) is
  'Current Reference Review child rows for Review Management; intentionally excludes relation paths and aggregate overview fields.';
