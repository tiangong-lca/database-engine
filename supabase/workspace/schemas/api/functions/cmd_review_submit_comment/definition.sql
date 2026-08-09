CREATE OR REPLACE FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select api.cmd_review_submit_comment(
    p_review_id,
    p_json,
    1,
    p_audit
  )
$$;

ALTER FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer DEFAULT 1, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_review private.reviews%rowtype;
  v_comment private.comments%rowtype;
  v_ref record;
  v_target record;
  v_reference private.reviews%rowtype;
  v_ref_roots jsonb := '[]'::jsonb;
  v_current_snapshot jsonb;
  v_items jsonb;
  v_new_items jsonb := '[]'::jsonb;
  v_item jsonb;
  v_checksum text;
  v_affected jsonb := '[]'::jsonb;
  v_reason text;
begin
  select review_row.*
  into v_review
  from private.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404
    );
  end if;

  if v_review.review_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'LEGACY_REVIEW_READ_ONLY',
      'status', 409
    );
  end if;

  if v_review.review_kind = 'reference'
    or v_review.target_table in (
      'contacts',
      'sources',
      'unitgroups',
      'flowproperties',
      'flows'
    ) then
    v_reason := coalesce(
      p_json->'comment'->>'message',
      p_json->>'reason'
    );
    return api.cmd_simple_review_submit_decision(
      p_review_id,
      case when p_comment_state = 1 then 'approve' else 'reject' end,
      case when p_comment_state = -3 then v_reason else null end,
      p_audit
    );
  end if;

  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401
    );
  end if;

  if v_review.review_kind <> 'root'
    or v_review.target_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_METADATA_NOT_APPLICABLE',
      'status', 400
    );
  end if;

  if v_review.state_code <> 1
    or p_comment_state not in (-3, 1)
    or not coalesce(v_review.reviewer_id, '[]'::jsonb)
      @> jsonb_build_array(v_actor::text) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403
    );
  end if;

  select comment_row.*
  into v_comment
  from private.comments as comment_row
  where comment_row.review_id = p_review_id
    and comment_row.reviewer_id = v_actor
  for update;

  if found and v_comment.state_code in (-2, 2) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 409
    );
  end if;

  if p_comment_state = 1 then
    for v_ref in
      select *
      from api.cmd_review_extract_refs(coalesce(p_json, '{}'::jsonb))
    loop
      if api.cmd_review_ref_type_to_table(v_ref.ref_type) is not null then
        v_ref_roots := v_ref_roots || jsonb_build_array(jsonb_build_object(
          'table', api.cmd_review_ref_type_to_table(v_ref.ref_type),
          'id', v_ref.ref_object_id,
          'version', v_ref.ref_version,
          'is_root', false
        ));
      end if;
    end loop;

    create temporary table if not exists review_comment_v2_targets (
      table_name text not null,
      dataset_id uuid not null,
      dataset_version text not null,
      state_code integer not null,
      reviews jsonb,
      dataset_row jsonb not null,
      is_root boolean not null default false,
      primary key (table_name, dataset_id, dataset_version)
    ) on commit drop;
    truncate table review_comment_v2_targets;

    insert into review_comment_v2_targets
    select *
    from api.cmd_review_collect_dataset_targets(v_ref_roots, true);

    v_current_snapshot := private.review_scope_current_snapshot_v1(
      v_review.scope_history
    );
    v_items := coalesce(v_current_snapshot->'items', '[]'::jsonb);

    for v_target in
      select *
      from review_comment_v2_targets
      order by table_name, dataset_id, dataset_version
    loop
      if nullif(v_target.dataset_row->>'user_id', '') is null then
        return jsonb_build_object(
          'ok', false,
          'code', 'REFERENCE_OWNER_UNRESOLVED',
          'status', 409
        );
      end if;

      if nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor
        and not private.review_dataset_can_read_v1(
          v_actor,
          v_target.table_name,
          v_target.dataset_row
        ) then
        return jsonb_build_object(
          'ok', false,
          'code', 'REFERENCE_ACCESS_DENIED',
          'status', 403
        );
      end if;

      v_checksum := private.review_revision_fingerprint_v1(
        v_target.table_name,
        v_target.dataset_row
      );
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_checksum,
        v_actor
      );

      if not exists (
        select 1
        from private.review_scope_current_items_v1(v_review.scope_history) as current_item
        where current_item.item_kind = 'reference'
          and current_item.target_table = v_target.table_name
          and current_item.data_id = v_target.dataset_id
          and current_item.data_version = v_target.dataset_version
          and current_item.submitted_revision_checksum = v_checksum
          and current_item.reference_review_id = v_reference.id
      ) then
        v_item := jsonb_build_object(
          'item_kind', 'reference',
          'target_table', v_target.table_name,
          'data_id', v_target.dataset_id,
          'data_version', v_target.dataset_version,
          'submitted_revision_checksum', v_checksum,
          'reference_review_id', v_reference.id,
          'target_owner_id', v_target.dataset_row->>'user_id',
          'target_team_id', v_target.dataset_row->'team_id',
          'relation_type', 'reviewer_metadata',
          'relation_path', '$.modellingAndValidation',
          'introduced_by', 'reviewer_metadata',
          'introduced_field_path', '$.modellingAndValidation'
        );
        v_new_items := v_new_items || jsonb_build_array(v_item);
      end if;

      perform set_config('app.review_controlled_write', 'on', true);
      execute format(
        'update public.%I
            set state_code = case when state_code < 20 then 20 else state_code end,
                reviews = case when state_code < 100
                  then api.cmd_review_append_review_ref(reviews, $1)
                  else reviews
                end,
                modified_at = now()
          where id = $2
            and version = $3',
        v_target.table_name
      ) using p_review_id, v_target.dataset_id, v_target.dataset_version;
      perform set_config('app.review_controlled_write', 'off', true);

      if v_target.state_code < 20
        and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor then
        perform private.review_notify_event_v1(
          'reference_entered_review',
          v_reference.id,
          nullif(v_target.dataset_row->>'user_id', '')::uuid,
          v_actor,
          v_target.table_name,
          v_target.dataset_id,
          v_target.dataset_version,
          p_review_id,
          (v_review.scope_history->>'current_version')::integer + 1,
          null
        );
      end if;

      v_affected := v_affected || jsonb_build_array(jsonb_build_object(
        'table', v_target.table_name,
        'id', v_target.dataset_id,
        'version', v_target.dataset_version,
        'reference_review_id', v_reference.id
      ));
    end loop;

    if jsonb_array_length(v_new_items) > 0 then
      perform private.review_append_scope_snapshot_v1(
        p_review_id,
        'review_metadata',
        v_current_snapshot->>'root_revision_checksum',
        v_items || v_new_items,
        v_actor
      );
    end if;
  end if;

  insert into private.comments (
    review_id,
    reviewer_id,
    json,
    state_code
  )
  values (
    p_review_id,
    v_actor,
    coalesce(p_json, '{}'::jsonb)::json,
    p_comment_state
  )
  on conflict (review_id, reviewer_id) do update
  set json = excluded.json,
      state_code = excluded.state_code,
      modified_at = now()
  returning * into v_comment;

  update private.reviews
  set json = api.cmd_review_append_log(
        coalesce(json, '{}'::jsonb),
        case when p_comment_state = 1
          then 'submit_comments'
          else 'reviewer_rejected'
        end,
        v_actor,
        jsonb_build_object(
          'reviewer_id', v_actor,
          'comment_state_code', p_comment_state
        )
      ),
      modified_at = now()
  where id = p_review_id
  returning * into v_review;

  insert into private.command_audit_log (
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
      'affected_datasets', v_affected
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'comment', to_jsonb(v_comment),
      'affected_datasets', v_affected
    )
  );
exception
  when others then
    perform set_config('app.review_controlled_write', 'off', true);
    raise;
end;
$_$;

ALTER FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "authenticated";

REVOKE ALL ON FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "authenticated";
