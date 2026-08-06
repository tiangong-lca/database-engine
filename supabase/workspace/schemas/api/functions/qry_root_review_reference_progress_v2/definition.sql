CREATE OR REPLACE FUNCTION "api"."qry_root_review_reference_progress_v2"("p_root_review_id" "uuid") RETURNS TABLE("reference_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "data_name" "jsonb", "submitted_revision_checksum" "text", "state_code" integer, "reviewer_count" integer, "completed_reviewer_count" integer, "actor_comment_state_code" integer, "actor_comment_modified_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
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
      from private.comments as completed_comment
      where completed_comment.review_id = reference_review.id
        and completed_comment.state_code in (1, -3, 2)
    ),
    actor_comment.state_code,
    actor_comment.modified_at
  from private.reviews as root_review
  cross join lateral pg_catalog.unnest(
    coalesce(root_review.current_reference_review_ids, '{}'::uuid[])
  ) with ordinality as current_reference(reference_review_id, ordinal_position)
  join private.reviews as reference_review
    on reference_review.id = current_reference.reference_review_id
    and reference_review.review_kind = 'reference'
  left join lateral (
    select comment_row.state_code, comment_row.modified_at
    from private.comments as comment_row
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
        and api.policy_review_can_read(reference_review.id, v_actor)
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

ALTER FUNCTION "api"."qry_root_review_reference_progress_v2"("p_root_review_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_root_review_reference_progress_v2"("p_root_review_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_root_review_reference_progress_v2"("p_root_review_id" "uuid") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_root_review_reference_progress_v2"("p_root_review_id" "uuid") TO "authenticated";
