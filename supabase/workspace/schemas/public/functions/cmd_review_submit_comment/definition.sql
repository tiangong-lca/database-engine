CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select public.cmd_review_submit_comment(
    p_review_id,
    p_json,
    1,
    p_audit
  )
$$;

ALTER FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer DEFAULT 1, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_comment_json jsonb := coalesce(p_json, '{}'::jsonb);
  v_review_json jsonb;
  v_ref record;
  v_ref_table text;
  v_ref_roots jsonb := '[]'::jsonb;
  v_target record;
  v_affected_datasets jsonb := '[]'::jsonb;
  v_action text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_comment_state not in (-3, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 400,
      'message', 'commentState must be 1 or -3'
    );
  end if;

  if coalesce(jsonb_typeof(v_comment_json), 'null') <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_JSON',
      'status', 400,
      'message', 'comment json must be an object'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Review comments can only be submitted for assigned or rejected reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not public.cmd_review_json_array(v_review.reviewer_id) @> jsonb_build_array(to_jsonb(v_actor::text)) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403,
      'message', 'Only assigned reviewers can submit review comments'
    );
  end if;

  select *
    into v_comment
  from public.comments
  where review_id = p_review_id
    and reviewer_id = v_actor
  for update;

  if found and v_comment.state_code in (-2, 2) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 409,
      'message', 'This reviewer comment can no longer be submitted',
      'details', jsonb_build_object(
        'state_code', v_comment.state_code
      )
    );
  end if;

  if not found then
    insert into public.comments (
      review_id,
      reviewer_id,
      state_code
    )
    values (
      p_review_id,
      v_actor,
      case
        when v_review.state_code = -1 then -1
        else 0
      end
    )
    returning *
      into v_comment;
  end if;

  if p_comment_state = 1 then
    for v_ref in
      select *
      from public.cmd_review_extract_refs(v_comment_json)
    loop
      v_ref_table := public.cmd_review_ref_type_to_table(v_ref.ref_type);

      if v_ref_table is null then
        continue;
      end if;

      v_ref_roots := v_ref_roots || jsonb_build_array(
        jsonb_build_object(
          'table', v_ref_table,
          'id', v_ref.ref_object_id,
          'version', v_ref.ref_version,
          'is_root', false
        )
      );
    end loop;

    create temporary table if not exists cmd_review_submit_comment_targets (
      table_name text not null,
      dataset_id uuid not null,
      dataset_version text not null,
      state_code integer not null,
      reviews jsonb,
      dataset_row jsonb not null,
      is_root boolean not null default false,
      primary key (table_name, dataset_id, dataset_version)
    ) on commit drop;

    truncate table cmd_review_submit_comment_targets;

    insert into cmd_review_submit_comment_targets (
      table_name,
      dataset_id,
      dataset_version,
      state_code,
      reviews,
      dataset_row,
      is_root
    )
    select
      table_name,
      dataset_id,
      dataset_version,
      state_code,
      reviews,
      dataset_row,
      is_root
    from public.cmd_review_collect_dataset_targets(v_ref_roots, true);

    for v_target in
      select *
      from cmd_review_submit_comment_targets
      where state_code >= 20
        and state_code < 100
      order by table_name, dataset_id, dataset_version
    loop
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCED_DATA_UNDER_REVIEW',
        'status', 409,
        'message', 'Referenced data is already under review',
        'details', jsonb_build_object(
          'table', v_target.table_name,
          'id', v_target.dataset_id,
          'version', v_target.dataset_version,
          'state_code', 20,
          'review_state_code', v_target.state_code
        )
      );
    end loop;

    for v_target in
      select *
      from cmd_review_submit_comment_targets
      where state_code < 20
      order by table_name, dataset_id, dataset_version
    loop
      execute format(
        'update public.%I
            set state_code = 20,
                reviews = $1,
                modified_at = now()
          where id = $2
            and version = $3',
        v_target.table_name
      )
        using public.cmd_review_append_review_ref(v_target.reviews, p_review_id),
              v_target.dataset_id,
              v_target.dataset_version;
    end loop;

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'table', table_name,
          'id', dataset_id,
          'version', dataset_version,
          'state_code', 20
        )
        order by table_name, dataset_id, dataset_version
      ),
      '[]'::jsonb
    )
      into v_affected_datasets
    from cmd_review_submit_comment_targets
    where state_code < 20;
  end if;

  update public.comments
    set json = v_comment_json::json,
        state_code = p_comment_state,
        modified_at = now()
  where review_id = p_review_id
    and reviewer_id = v_actor
  returning *
    into v_comment;

  v_action := case
    when p_comment_state = -3 then 'reviewer_rejected'
    else 'submit_comments'
  end;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    v_action,
    v_actor,
    jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', p_comment_state
    )
  );

  update public.reviews
    set state_code = case
          when p_comment_state = 1 and state_code = -1 then 1
          else state_code
        end,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_submit_comment',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', p_comment_state,
      'affected_datasets', v_affected_datasets
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'comment', to_jsonb(v_comment),
      'affected_datasets', v_affected_datasets
    )
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "service_role";

REVOKE ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "service_role";
