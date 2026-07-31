-- Keep the existing frontend queue projection while excluding immutable legacy
-- source rows. New Root and Reference Reviews share the same queues.

create or replace function public.qry_review_get_admin_queue_items(
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
  reviewer_id jsonb,
  "json" jsonb,
  deadline timestamptz,
  created_at timestamptz,
  modified_at timestamptz,
  comment_state_codes jsonb,
  total_count bigint
)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_review_resolve_queue_order_by(p_sort_by, false);
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

  return query execute format(
    $sql$
      with q as (
        select
          r.id,
          r.data_id,
          r.data_version::text as data_version,
          r.state_code,
          coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(r.json, '{}'::jsonb) as json,
          r.deadline,
          r.created_at,
          r.modified_at,
          coalesce(
            jsonb_agg(to_jsonb(c.state_code)
              order by c.created_at, c.reviewer_id)
              filter (where c.reviewer_id is not null),
            '[]'::jsonb
          ) as comment_state_codes
        from public.reviews as r
        left join public.comments as c on c.review_id = r.id
        where r.review_kind in ('root', 'reference')
          and ($1::integer is null or r.state_code = $1::integer)
        group by r.id
      )
      select q.*, count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2 offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_state_code, v_limit, v_offset;
end;
$$;

alter function public.qry_review_get_admin_queue_items(
  text, integer, integer, text, text
) owner to postgres;

create or replace function public.qry_review_get_member_queue_items(
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
  reviewer_id jsonb,
  "json" jsonb,
  deadline timestamptz,
  created_at timestamptz,
  modified_at timestamptz,
  comment_state_code integer,
  comment_json jsonb,
  comment_created_at timestamptz,
  comment_modified_at timestamptz,
  total_count bigint
)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_review_resolve_queue_order_by(p_sort_by, true);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, 'pending'));
begin
  if v_actor is null or not public.cmd_review_is_review_member(v_actor) then
    return;
  end if;
  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;

  return query execute format(
    $sql$
      with q as (
        select
          r.id,
          r.data_id,
          r.data_version::text as data_version,
          r.state_code as review_state_code,
          coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(r.json, '{}'::jsonb) as json,
          r.deadline,
          r.created_at,
          r.modified_at,
          c.state_code as comment_state_code,
          coalesce(c.json::jsonb, '{}'::jsonb) as comment_json,
          c.created_at as comment_created_at,
          c.modified_at as comment_modified_at
        from public.comments as c
        join public.reviews as r on r.id = c.review_id
        where r.review_kind in ('root', 'reference')
          and c.reviewer_id = $1
          and public.policy_review_can_read(r.id, $1)
          and (
            ($4 = 'pending' and c.state_code = 0 and r.state_code > 0)
            or ($4 = 'reviewed'
              and c.state_code = any (array[1, 2, -3])
              and r.state_code > 0)
            or ($4 = 'reviewer-rejected'
              and c.state_code = -1
              and r.state_code = -1)
          )
      )
      select q.*, count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2 offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_actor, v_limit, v_offset, v_status;
end;
$$;

alter function public.qry_review_get_member_queue_items(
  text, integer, integer, text, text
) owner to postgres;
