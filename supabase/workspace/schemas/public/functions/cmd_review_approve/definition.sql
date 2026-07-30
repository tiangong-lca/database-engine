CREATE OR REPLACE FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_root jsonb;
  v_assertion jsonb;
  v_comment_ref record;
begin
  if v_actor is null
    or not public.cmd_review_is_review_admin(v_actor)
    or lower(coalesce(p_table, '')) not in ('processes', 'lifecyclemodels') then
    return public.cmd_review_approve_issue304_legacy(
      p_table,
      p_review_id,
      p_audit
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('review-publication-lifecycle-closure', 0)
  );

  select review_row.*
  into v_review
  from public.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found or v_review.state_code <> 1 then
    return public.cmd_review_approve_issue304_legacy(
      p_table,
      p_review_id,
      p_audit
    );
  end if;

  v_root := public.cmd_review_get_dataset_row(
    lower(p_table),
    v_review.data_id,
    v_review.data_version,
    true
  );

  if v_root is null then
    return public.cmd_review_approve_issue304_legacy(
      p_table,
      p_review_id,
      p_audit
    );
  end if;

  if lower(p_table) = 'processes' then
    lock table public.lifecyclemodels in share row exclusive mode;
  end if;

  perform 1
  from public.comments as comment_row
  where comment_row.review_id = p_review_id
  order by comment_row.reviewer_id, comment_row.created_at
  for update;

  for v_comment_ref in
    select role_ref.*
    from public.comments as comment_row
    cross join lateral public.cmd_review_reference_roles(
      'comments',
      'comment',
      coalesce(to_jsonb(comment_row.json), '{}'::jsonb)
    ) as role_ref
    where comment_row.review_id = p_review_id
      and comment_row.state_code = 1
    order by
      role_ref.reference_path,
      role_ref.ref_table,
      role_ref.ref_object_id,
      role_ref.ref_version
  loop
    if v_comment_ref.lifecycle_role = 'PolicyGap' then
      return public.cmd_review_lifecycle_error(
        'REFERENCE_ROLE_POLICY_GAP',
        v_comment_ref.reference_path,
        v_comment_ref.ref_table,
        v_comment_ref.ref_object_id,
        v_comment_ref.ref_version
      );
    end if;
  end loop;

  v_assertion := public.cmd_review_assert_lifecycle_closure(
    jsonb_build_array(jsonb_build_object(
      'table', lower(p_table),
      'id', v_review.data_id,
      'version', v_review.data_version
    )),
    'approve',
    v_actor
  );

  if v_assertion is not null then
    return v_assertion;
  end if;

  return public.cmd_review_approve_issue304_legacy(
    p_table,
    p_review_id,
    p_audit
  );
end;
$$;

ALTER FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") TO "service_role";
