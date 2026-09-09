-- Database #632: reuse dataset lexical projections without public/latest search scope.
-- The unexposed helper is invoker-security and has no application EXECUTE grant.
-- Only authenticated review queue facades admit actors and expose matched tasks.
create or replace function private.review_search_dataset_versions_v1(
  p_query text,
  p_target_table text default null
) returns table(target_table text, data_id uuid, data_version character(9))
language plpgsql
stable
set search_path = ''
as $$
declare
  v_table text;
  v_uuid uuid;
  v_terms text[];
  v_predicate text;
begin
  if nullif(pg_catalog.btrim(p_query), '') is null then return; end if;
  if pg_catalog.btrim(p_query) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_uuid := pg_catalog.btrim(p_query)::uuid;
  end if;
  v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  foreach v_table in array array[
    'contacts', 'sources', 'unitgroups', 'flowproperties', 'flows',
    'processes', 'lifecyclemodels'
  ] loop
    if p_target_table is not null and v_table <> p_target_table then continue; end if;
    if v_uuid is not null then
      return query execute pg_catalog.format(
        'select %L::text, d.id, d.version from public.%I d where d.id = $1',
        v_table, v_table
      ) using v_uuid;
    else
      -- Preserve the current ordinary data-list predicates: foundation query
      -- syntax and core escaped term arrays. No AI expansion or JSON flattening.
      v_predicate := case when v_table in ('flows', 'processes', 'lifecyclemodels')
        then 'd.search_text operator(extensions.&@~|) $2'
        else 'd.search_text operator(extensions.&@~) $1' end;
      return query execute pg_catalog.format(
        'select %L::text, d.id, d.version from public.%I d where %s',
        v_table, v_table, v_predicate
      ) using p_query, v_terms;
    end if;
  end loop;
end;
$$;
alter function private.review_search_dataset_versions_v1(text, text) owner to postgres;
revoke all on function private.review_search_dataset_versions_v1(text, text)
  from public, anon, authenticated, service_role, api_internal_executor;

create or replace function api.qry_review_get_admin_queue_items_v4(
  p_status text default null,
  p_page integer default 1,
  p_page_size integer default 50,
  p_sort_by text default 'modified_at',
  p_sort_order text default 'desc',
  p_display_mode text default 'all',
  p_target_table text default null,
  p_query text default null
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
  v_query text := nullif(pg_catalog.btrim(p_query), '');
  v_limit integer := greatest(1, least(coalesce(p_page_size, 50), 100));
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
  v_display_mode text := pg_catalog.lower(pg_catalog.btrim(coalesce(p_display_mode, 'all')));
  v_target_table text := nullif(
    pg_catalog.lower(pg_catalog.btrim(coalesce(p_target_table, ''))),
    ''
  );
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

  if v_display_mode not in ('all', 'model_process', 'other') then
    raise exception using
      errcode = '22023',
      message = 'INVALID_REVIEW_DISPLAY_MODE';
  end if;
  if v_target_table is not null and not (
    v_target_table = any(array[
      'contacts', 'sources', 'unitgroups', 'flowproperties', 'flows',
      'processes', 'lifecyclemodels'
    ]::text[])
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVALID_REVIEW_TARGET_TABLE';
  end if;

  if pg_catalog.char_length(v_query) > 1000 then
    raise exception using errcode = '22023', message = 'REVIEW_QUERY_TOO_LONG';
  end if;

  return query
  with matches as materialized (
    select * from private.review_search_dataset_versions_v1(v_query, v_target_table)
    where v_query is not null
  ), q as (
    select
      review_row.id,
      review_row.data_id,
      pg_catalog.btrim(review_row.data_version::text) as data_version,
      review_row.state_code,
      review_row.review_kind,
      review_row.target_table,
      coalesce(review_row.reviewer_id, '[]'::jsonb) as reviewer_id,
      coalesce(review_row.json, '{}'::jsonb) as json,
      review_row.deadline,
      review_row.created_at,
      review_row.modified_at,
      coalesce(review_comments.comment_state_codes, '[]'::jsonb) as comment_state_codes,
      true as root_matches_status,
      true as root_can_read
    from private.reviews as review_row
    left join lateral (
      select pg_catalog.jsonb_agg(
        pg_catalog.to_jsonb(comment_row.state_code)
        order by comment_row.created_at, comment_row.reviewer_id
      ) filter (where comment_row.reviewer_id is not null) as comment_state_codes
      from private.comments as comment_row
      where comment_row.review_id = review_row.id
    ) as review_comments on true
    where review_row.review_kind in ('root', 'reference')
      and (v_state_code is null or review_row.state_code = v_state_code)
      and (
        v_display_mode = 'all'
        or (
          v_display_mode = 'model_process'
          and review_row.target_table in ('processes', 'lifecyclemodels')
        )
        or (
          v_display_mode = 'other'
          and review_row.target_table not in ('processes', 'lifecyclemodels')
        )
      )
      and (v_target_table is null or review_row.target_table = v_target_table)
      and (v_query is null or exists (
        select 1 from matches
        where matches.target_table = review_row.target_table
          and matches.data_id = review_row.data_id
          and matches.data_version = review_row.data_version
      ))
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

alter function api.qry_review_get_admin_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) owner to postgres;
revoke all on function api.qry_review_get_admin_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) from public, anon, service_role;
grant execute on function api.qry_review_get_admin_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) to authenticated, api_internal_executor;

comment on function api.qry_review_get_admin_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) is
  'Admin full-text queue over exact dataset versions; search_text matches precede task count and pagination. Empty query preserves v3 behavior.';

create or replace function api.qry_review_get_member_queue_items_v4(
  p_status text default 'pending',
  p_page integer default 1,
  p_page_size integer default 50,
  p_sort_by text default 'modified_at',
  p_sort_order text default 'desc',
  p_display_mode text default 'all',
  p_target_table text default null,
  p_query text default null
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
  v_query text := nullif(pg_catalog.btrim(p_query), '');
  v_limit integer := greatest(1, least(coalesce(p_page_size, 50), 100));
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
  v_display_mode text := pg_catalog.lower(pg_catalog.btrim(coalesce(p_display_mode, 'all')));
  v_target_table text := nullif(
    pg_catalog.lower(pg_catalog.btrim(coalesce(p_target_table, ''))),
    ''
  );
begin
  if v_actor is null or not api.cmd_review_is_review_member(v_actor) then
    return;
  end if;
  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;
  if v_display_mode not in ('all', 'model_process', 'other') then
    raise exception using
      errcode = '22023',
      message = 'INVALID_REVIEW_DISPLAY_MODE';
  end if;
  if v_target_table is not null and not (
    v_target_table = any(array[
      'contacts', 'sources', 'unitgroups', 'flowproperties', 'flows',
      'processes', 'lifecyclemodels'
    ]::text[])
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVALID_REVIEW_TARGET_TABLE';
  end if;

  if pg_catalog.char_length(v_query) > 1000 then
    raise exception using errcode = '22023', message = 'REVIEW_QUERY_TOO_LONG';
  end if;

  return query
  with matches as materialized (
    select * from private.review_search_dataset_versions_v1(v_query, v_target_table)
    where v_query is not null
  ), q as (
    select
      review_row.id,
      review_row.data_id,
      pg_catalog.btrim(review_row.data_version::text) as data_version,
      review_row.state_code as review_state_code,
      review_row.review_kind,
      review_row.target_table,
      coalesce(review_row.reviewer_id, '[]'::jsonb) as reviewer_id,
      coalesce(review_row.json, '{}'::jsonb) as json,
      review_row.deadline,
      review_row.created_at,
      greatest(review_row.modified_at, comment_row.modified_at) as modified_at,
      comment_row.state_code as comment_state_code,
      coalesce(comment_row.json::jsonb, '{}'::jsonb) as comment_json,
      comment_row.created_at as comment_created_at,
      comment_row.modified_at as comment_modified_at,
      true as root_matches_status,
      true as root_can_read
    from private.comments as comment_row
    join private.reviews as review_row on review_row.id = comment_row.review_id
    where review_row.review_kind in ('root', 'reference')
      and comment_row.reviewer_id = v_actor
      and api.policy_review_can_read(review_row.id, v_actor)
      and (
        v_display_mode = 'all'
        or (
          v_display_mode = 'model_process'
          and review_row.target_table in ('processes', 'lifecyclemodels')
        )
        or (
          v_display_mode = 'other'
          and review_row.target_table not in ('processes', 'lifecyclemodels')
        )
      )
      and (v_target_table is null or review_row.target_table = v_target_table)
      and (v_query is null or exists (
        select 1 from matches
        where matches.target_table = review_row.target_table
          and matches.data_id = review_row.data_id
          and matches.data_version = review_row.data_version
      ))
      and (
        (v_status = 'pending' and comment_row.state_code = 0 and review_row.state_code > 0)
        or (v_status = 'reviewed' and comment_row.state_code = any(array[1, 2, -3]) and review_row.state_code > 0)
        or (v_status = 'reviewer-rejected' and comment_row.state_code = -1 and review_row.state_code = -1)
      )
  )
  select q.*, pg_catalog.count(*) over() as total_count
  from q
  order by
    case when v_sort_key = 'created_at' and v_order_dir = 'asc' then q.created_at end asc nulls last,
    case when v_sort_key = 'created_at' and v_order_dir = 'desc' then q.created_at end desc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'asc' then q.deadline end asc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'desc' then q.deadline end desc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'asc' then q.review_state_code end asc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'desc' then q.review_state_code end desc nulls last,
    case when v_sort_key = 'comment_modified_at' and v_order_dir = 'asc' then q.comment_modified_at end asc nulls last,
    case when v_sort_key = 'comment_modified_at' and v_order_dir = 'desc' then q.comment_modified_at end desc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'asc' then q.modified_at end asc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'desc' then q.modified_at end desc nulls last,
    q.id
  limit v_limit offset v_offset;
end;
$$;

alter function api.qry_review_get_member_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) owner to postgres;
revoke all on function api.qry_review_get_member_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) from public, anon, service_role;
grant execute on function api.qry_review_get_member_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) to authenticated, api_internal_executor;

comment on function api.qry_review_get_member_queue_items_v4(
  text, integer, integer, text, text, text, text, text
) is
  'Actor full-text queue over exact dataset versions; existing assignment/readability and tab filters apply before task count and pagination.';

insert into private.api_capability_grants (
  routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role
)
values
  (
    'api.qry_review_get_admin_queue_items_v4(text, integer, integer, text, text, text, text, text)',
    'NX-REV-01', false, true, false
  ),
  (
    'api.qry_review_get_member_queue_items_v4(text, integer, integer, text, text, text, text, text)',
    'NX-REV-01', false, true, false
  )
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;
