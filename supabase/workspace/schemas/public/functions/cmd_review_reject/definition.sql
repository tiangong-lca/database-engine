CREATE OR REPLACE FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_root_table text;
  v_root_targets jsonb;
  v_comment_ref_roots jsonb := '[]'::jsonb;
  v_target record;
  v_comment_ref record;
  v_review_json jsonb;
  v_affected_datasets jsonb := '[]'::jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can reject reviews'
    );
  end if;

  v_root_table := lower(coalesce(p_table, ''));
  if v_root_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_TABLE',
      'status', 400,
      'message', 'table must be processes or lifecyclemodels'
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

  if v_review.state_code not in (-1, 0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Only pending, assigned, or rejected reviews can be rejected',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  v_root_targets := jsonb_build_array(
    jsonb_build_object(
      'table', v_root_table,
      'id', v_review.data_id,
      'version', v_review.data_version,
      'is_root', true
    )
  );

  create temporary table if not exists cmd_review_reject_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  truncate table cmd_review_reject_targets;

  insert into cmd_review_reject_targets (
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
  from public.cmd_review_collect_dataset_targets(v_root_targets, true);

  for v_comment_ref in
    select distinct
      ref.ref_type,
      ref.ref_object_id,
      ref.ref_version
    from public.comments as c
    cross join lateral public.cmd_review_extract_refs(coalesce(to_jsonb(c.json), '{}'::jsonb)) as ref
    where c.review_id = p_review_id
      and c.state_code <> -2
  loop
    v_comment_ref_roots := v_comment_ref_roots || jsonb_build_array(
      jsonb_build_object(
        'table', public.cmd_review_ref_type_to_table(v_comment_ref.ref_type),
        'id', v_comment_ref.ref_object_id,
        'version', v_comment_ref.ref_version,
        'is_root', false
      )
    );
  end loop;

  insert into cmd_review_reject_targets (
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
  from public.cmd_review_collect_dataset_targets(v_comment_ref_roots, true)
  on conflict (table_name, dataset_id, dataset_version) do nothing;

  for v_target in
    select *
    from cmd_review_reject_targets
    where state_code >= 20
      and state_code < 100
    order by table_name, dataset_id, dataset_version
  loop
    execute format(
      'update public.%I
          set state_code = 0,
              modified_at = now()
        where id = $1
          and version = $2',
      v_target.table_name
    )
      using v_target.dataset_id, v_target.dataset_version;
  end loop;

  update public.comments
    set state_code = -1,
        modified_at = now()
  where review_id = p_review_id
    and state_code <> -2;

  v_review_json := coalesce(v_review.json, '{}'::jsonb);
  v_review_json := jsonb_set(
    v_review_json,
    '{comment}',
    coalesce(v_review_json->'comment', '{}'::jsonb) || jsonb_build_object(
      'message', coalesce(p_reason, '')
    ),
    true
  );
  v_review_json := public.cmd_review_append_log(
    v_review_json,
    'rejected',
    v_actor,
    jsonb_build_object(
      'reason', coalesce(p_reason, '')
    )
  );

  update public.reviews
    set state_code = -1,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'table', table_name,
        'id', dataset_id,
        'version', dataset_version,
        'state_code', 0
      )
      order by table_name, dataset_id, dataset_version
    ),
    '[]'::jsonb
  )
    into v_affected_datasets
  from cmd_review_reject_targets
  where state_code >= 20
    and state_code < 100;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_reject',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'root_table', v_root_table,
      'reason', coalesce(p_reason, ''),
      'affected_datasets', v_affected_datasets
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'affected_datasets', v_affected_datasets
    )
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "service_role";
