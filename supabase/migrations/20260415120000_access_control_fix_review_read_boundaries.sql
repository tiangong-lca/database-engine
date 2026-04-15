create or replace function public.cmd_review_is_review_member(p_actor uuid default auth.uid())
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-member'
  )
$$;

create or replace function public.policy_review_can_read(
  p_review_id uuid,
  p_actor uuid default auth.uid()
)
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select exists (
    select 1
    from public.reviews as r
    where r.id = p_review_id
      and coalesce(p_actor, auth.uid()) is not null
      and (
        public.cmd_review_is_review_admin(coalesce(p_actor, auth.uid()))
        or ((r.json -> 'user' ->> 'id')::uuid = coalesce(p_actor, auth.uid()))
        or (
          public.cmd_review_is_review_member(coalesce(p_actor, auth.uid()))
          and (
            coalesce(r.reviewer_id, '[]'::jsonb) ? coalesce(p_actor, auth.uid())::text
            or exists (
              select 1
              from public.comments as c
              where c.review_id = r.id
                and c.reviewer_id = coalesce(p_actor, auth.uid())
            )
          )
        )
      )
  )
$$;

create or replace function public.cmd_review_resolve_queue_order_by(
  p_sort_by text,
  p_allow_comment_modified boolean default false
)
returns text
language plpgsql
immutable
set search_path = public, pg_temp
as $$
begin
  case lower(coalesce(p_sort_by, ''))
    when 'created_at' then
      return 'q.created_at';
    when 'createat' then
      return 'q.created_at';
    when 'deadline' then
      return 'q.deadline';
    when 'state_code' then
      return 'q.state_code';
    when 'statecode' then
      return 'q.state_code';
    when 'comment_modified_at' then
      if p_allow_comment_modified then
        return 'q.comment_modified_at';
      end if;
    when 'commentmodifiedat' then
      if p_allow_comment_modified then
        return 'q.comment_modified_at';
      end if;
    else
      return 'q.modified_at';
  end case;

  return 'q.modified_at';
end;
$$;

drop policy if exists "Enable read open data access for reviews" on public.reviews;
drop policy if exists "reviews select by review participants" on public.reviews;

create policy "reviews select by review participants"
on public.reviews
for select
to authenticated
using (
  auth.uid() is not null
  and public.policy_review_can_read(id, auth.uid())
);

drop policy if exists "comments select by review participants" on public.comments;

create policy "comments select by review participants"
on public.comments
for select
to authenticated
using (
  auth.uid() is not null
  and public.policy_review_can_read(review_id, auth.uid())
  and (
    public.cmd_review_is_review_admin(auth.uid())
    or exists (
      select 1
      from public.reviews as r
      where r.id = comments.review_id
        and ((r.json -> 'user' ->> 'id')::uuid = auth.uid())
    )
    or comments.reviewer_id = auth.uid()
  )
);

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
  if v_actor is null then
    return;
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return;
  end if;

  case v_status
    when '', 'all' then
      v_state_code := null;
    when 'unassigned' then
      v_state_code := 0;
    when 'assigned' then
      v_state_code := 1;
    when 'admin-rejected' then
      v_state_code := -1;
    else
      return;
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
            jsonb_agg(to_jsonb(c.state_code) order by c.created_at asc, c.reviewer_id asc)
              filter (where c.reviewer_id is not null),
            '[]'::jsonb
          ) as comment_state_codes
        from public.reviews as r
        left join public.comments as c
          on c.review_id = r.id
        where ($1::integer is null or r.state_code = $1::integer)
        group by
          r.id,
          r.data_id,
          r.data_version,
          r.state_code,
          r.reviewer_id,
          r.json,
          r.deadline,
          r.created_at,
          r.modified_at
      )
      select
        q.id,
        q.data_id,
        q.data_version,
        q.state_code,
        q.reviewer_id,
        q.json,
        q.deadline,
        q.created_at,
        q.modified_at,
        q.comment_state_codes,
        count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_state_code, v_limit, v_offset;
end;
$$;

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
  if v_actor is null then
    return;
  end if;

  if not public.cmd_review_is_review_member(v_actor) then
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
        join public.reviews as r
          on r.id = c.review_id
        where c.reviewer_id = $1
          and public.policy_review_can_read(r.id, $1)
          and (
            ($4::text = 'pending' and c.state_code = 0 and r.state_code > 0)
            or ($4::text = 'reviewed' and c.state_code = any (array[1, 2, -3]) and r.state_code > 0)
            or ($4::text = 'reviewer-rejected' and c.state_code = -1 and r.state_code = -1)
          )
      )
      select
        q.id,
        q.data_id,
        q.data_version,
        q.review_state_code,
        q.reviewer_id,
        q.json,
        q.deadline,
        q.created_at,
        q.modified_at,
        q.comment_state_code,
        q.comment_json,
        q.comment_created_at,
        q.comment_modified_at,
        count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_actor, v_limit, v_offset, v_status;
end;
$$;

create or replace function public.qry_review_get_items(
  p_review_ids uuid[] default null,
  p_data_id uuid default null,
  p_data_version text default null,
  p_state_codes integer[] default null
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
  modified_at timestamptz
)
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select
    r.id,
    r.data_id,
    r.data_version::text as data_version,
    r.state_code,
    coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
    coalesce(r.json, '{}'::jsonb) as json,
    r.deadline,
    r.created_at,
    r.modified_at
  from public.reviews as r
  where (p_review_ids is null or r.id = any (p_review_ids))
    and (
      p_data_id is null
      or r.data_id = p_data_id
      or coalesce(r.json -> 'data' ->> 'id', '') = p_data_id::text
    )
    and (
      p_data_version is null
      or r.data_version = p_data_version
      or coalesce(r.json -> 'data' ->> 'version', '') = p_data_version
    )
    and (p_state_codes is null or r.state_code = any (p_state_codes))
    and public.policy_review_can_read(r.id, auth.uid())
  order by r.modified_at desc, r.id desc
$$;

create or replace function public.qry_review_get_comment_items(
  p_review_id uuid,
  p_scope text default 'auto'
)
returns table (
  review_id uuid,
  reviewer_id uuid,
  state_code integer,
  "json" jsonb,
  created_at timestamptz,
  modified_at timestamptz
)
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  with actor as (
    select
      auth.uid() as actor_id,
      public.cmd_review_is_review_admin(auth.uid()) as is_review_admin,
      exists (
        select 1
        from public.reviews as r
        where r.id = p_review_id
          and ((r.json -> 'user' ->> 'id')::uuid = auth.uid())
      ) as is_owner
  )
  select
    c.review_id,
    c.reviewer_id,
    c.state_code,
    coalesce(c.json::jsonb, '{}'::jsonb) as json,
    c.created_at,
    c.modified_at
  from public.comments as c
  cross join actor as a
  where c.review_id = p_review_id
    and public.policy_review_can_read(p_review_id, a.actor_id)
    and (
      a.is_review_admin
      or a.is_owner
      or c.reviewer_id = a.actor_id
    )
    and (
      lower(coalesce(p_scope, 'auto')) not in ('mine', 'self')
      or c.reviewer_id = a.actor_id
    )
  order by c.created_at asc, c.reviewer_id asc
$$;

revoke all on function public.cmd_review_is_review_member(uuid) from public;
revoke all on function public.policy_review_can_read(uuid, uuid) from public;
revoke all on function public.qry_review_get_admin_queue_items(text, integer, integer, text, text) from public;
revoke all on function public.qry_review_get_member_queue_items(text, integer, integer, text, text) from public;
revoke all on function public.qry_review_get_items(uuid[], uuid, text, integer[]) from public;
revoke all on function public.qry_review_get_comment_items(uuid, text) from public;

grant execute on function public.cmd_review_is_review_member(uuid) to authenticated;
grant execute on function public.policy_review_can_read(uuid, uuid) to authenticated;
grant execute on function public.qry_review_get_admin_queue_items(text, integer, integer, text, text) to authenticated;
grant execute on function public.qry_review_get_member_queue_items(text, integer, integer, text, text) to authenticated;
grant execute on function public.qry_review_get_items(uuid[], uuid, text, integer[]) to authenticated;
grant execute on function public.qry_review_get_comment_items(uuid, text) to authenticated;

grant execute on function public.cmd_review_is_review_member(uuid) to service_role;
grant execute on function public.policy_review_can_read(uuid, uuid) to service_role;
grant execute on function public.qry_review_get_admin_queue_items(text, integer, integer, text, text) to service_role;
grant execute on function public.qry_review_get_member_queue_items(text, integer, integer, text, text) to service_role;
grant execute on function public.qry_review_get_items(uuid[], uuid, text, integer[]) to service_role;
grant execute on function public.qry_review_get_comment_items(uuid, text) to service_role;
