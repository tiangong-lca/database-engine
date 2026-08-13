CREATE OR REPLACE FUNCTION "api"."qry_review_get_member_root_queue_items_v2"("p_status" "text" DEFAULT 'pending'::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "review_state_code" integer, "review_kind" "text", "target_table" "text", "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_code" integer, "comment_json" "jsonb", "comment_created_at" timestamp with time zone, "comment_modified_at" timestamp with time zone, "root_matches_status" boolean, "root_can_read" boolean, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
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

ALTER FUNCTION "api"."qry_review_get_member_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_get_member_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_get_member_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_review_get_member_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
