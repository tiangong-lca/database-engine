CREATE OR REPLACE FUNCTION "private"."cmd_review_migrate_legacy_v2"("p_legacy_review_id" "uuid", "p_new_root_review_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_legacy private.reviews%rowtype;
  v_root_row jsonb;
  v_table text;
  v_table_count integer := 0;
  v_candidate text;
  v_owner_id uuid;
  v_checksum text;
  v_target record;
  v_reference private.reviews%rowtype;
  v_target_checksum text;
  v_item jsonb;
  v_items jsonb := '[]'::jsonb;
  v_scope_history jsonb;
  v_reference_ids uuid[] := array[]::uuid[];
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403
    );
  end if;

  select review_row.*
  into v_legacy
  from private.reviews as review_row
  where review_row.id = p_legacy_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'LEGACY_REVIEW_NOT_FOUND',
      'status', 404
    );
  end if;

  if v_legacy.review_kind is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ALREADY_MIGRATED',
      'status', 409
    );
  end if;

  if exists (select 1 from public.processes
             where id = v_legacy.data_id
               and version = btrim(v_legacy.data_version::text)) then
    v_candidate := 'processes';
    v_table_count := v_table_count + 1;
  end if;
  if exists (select 1 from public.lifecyclemodels
             where id = v_legacy.data_id
               and version = btrim(v_legacy.data_version::text)) then
    v_candidate := 'lifecyclemodels';
    v_table_count := v_table_count + 1;
  end if;

  if v_table_count <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'LEGACY_REVIEW_TARGET_AMBIGUOUS',
      'status', 409,
      'details', jsonb_build_object('matching_table_count', v_table_count)
    );
  end if;
  v_table := v_candidate;

  v_root_row := api.cmd_review_get_dataset_row(
    v_table,
    v_legacy.data_id,
    btrim(v_legacy.data_version::text),
    true
  );
  v_owner_id := nullif(v_root_row->>'user_id', '')::uuid;
  if v_root_row is null or v_owner_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'LEGACY_REVIEW_TARGET_INVALID',
      'status', 409
    );
  end if;

  if exists (
    select 1
    from private.reviews as migrated
    where migrated.review_kind = 'root'
      and migrated.json->>'legacy_review_id' = p_legacy_review_id::text
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'LEGACY_REVIEW_ALREADY_MAPPED',
      'status', 409
    );
  end if;

  create temporary table if not exists review_legacy_v2_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;
  truncate table review_legacy_v2_targets;

  insert into review_legacy_v2_targets
  select *
  from api.cmd_review_collect_dataset_targets(
    jsonb_build_array(jsonb_build_object(
      'table', v_table,
      'id', v_legacy.data_id,
      'version', btrim(v_legacy.data_version::text),
      'is_root', true
    )),
    true
  );

  for v_target in
    select *
    from review_legacy_v2_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    v_target_checksum := private.review_revision_fingerprint_v1(
      v_target.table_name,
      v_target.dataset_row
    );
    if v_target.is_root then
      v_checksum := v_target_checksum;
      v_item := jsonb_build_object(
        'item_kind', 'root',
        'target_table', v_target.table_name,
        'data_id', v_target.dataset_id,
        'data_version', v_target.dataset_version,
        'submitted_revision_checksum', v_target_checksum,
        'reference_review_id', null,
        'target_owner_id', v_target.dataset_row->>'user_id',
        'target_team_id', v_target.dataset_row->'team_id',
        'relation_type', 'root',
        'relation_path', '$',
        'introduced_by', 'legacy_migration',
        'introduced_field_path', null
      );
    else
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_target_checksum,
        v_owner_id
      );
      v_reference_ids := array_append(v_reference_ids, v_reference.id);
      v_item := jsonb_build_object(
        'item_kind', 'reference',
        'target_table', v_target.table_name,
        'data_id', v_target.dataset_id,
        'data_version', v_target.dataset_version,
        'submitted_revision_checksum', v_target_checksum,
        'reference_review_id', v_reference.id,
        'target_owner_id', v_target.dataset_row->>'user_id',
        'target_team_id', v_target.dataset_row->'team_id',
        'relation_type', 'dependency',
        'relation_path', '$',
        'introduced_by', 'legacy_migration',
        'introduced_field_path', null
      );
    end if;
    v_items := v_items || jsonb_build_array(v_item);
  end loop;

  v_scope_history := jsonb_build_object(
    'schema_version', 'review_scope.v1',
    'current_version', 1,
    'snapshots', jsonb_build_array(jsonb_build_object(
      'version_no', 1,
      'scope_basis', 'legacy_migration',
      'root_revision_checksum', v_checksum,
      'scope_checksum', private.review_scope_checksum_v1(v_items),
      'created_by', v_owner_id,
      'created_at', to_jsonb(now()),
      'items', v_items
    ))
  );

  insert into private.reviews (
    id,
    data_id,
    data_version,
    state_code,
    reviewer_id,
    json,
    deadline,
    review_kind,
    target_table,
    submitted_revision_checksum,
    approved_revision_checksum,
    target_owner_id,
    target_team_id,
    scope_schema_version,
    scope_history,
    created_at,
    modified_at
  )
  values (
    p_new_root_review_id,
    v_legacy.data_id,
    v_legacy.data_version,
    v_legacy.state_code,
    v_legacy.reviewer_id,
    coalesce(v_legacy.json, '{}'::jsonb)
      || jsonb_build_object(
        'review_kind', 'root',
        'legacy_review_id', p_legacy_review_id,
        'data', coalesce(v_legacy.json->'data', '{}'::jsonb)
          || jsonb_build_object('table', v_table)
      ),
    v_legacy.deadline,
    'root',
    v_table,
    v_checksum,
    case when v_legacy.state_code = 2 then v_checksum else null end,
    v_owner_id,
    nullif(v_root_row->>'team_id', '')::uuid,
    'review_scope.v1',
    v_scope_history,
    v_legacy.created_at,
    v_legacy.modified_at
  );

  insert into private.comments (
    review_id,
    reviewer_id,
    state_code,
    json,
    created_at,
    modified_at
  )
  select
    p_new_root_review_id,
    comment_row.reviewer_id,
    comment_row.state_code,
    comment_row.json,
    comment_row.created_at,
    comment_row.modified_at
  from private.comments as comment_row
  where comment_row.review_id = p_legacy_review_id;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_migrate_legacy_v2',
    v_owner_id,
    'reviews',
    p_new_root_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'legacy_review_id', p_legacy_review_id,
      'new_root_review_id', p_new_root_review_id,
      'target_table', v_table,
      'reference_review_ids', to_jsonb(v_reference_ids)
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'legacyReviewId', p_legacy_review_id,
      'rootReviewId', p_new_root_review_id,
      'targetTable', v_table,
      'referenceReviewIds', to_jsonb(v_reference_ids)
    )
  );
end;
$_$;

ALTER FUNCTION "private"."cmd_review_migrate_legacy_v2"("p_legacy_review_id" "uuid", "p_new_root_review_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_review_migrate_legacy_v2"("p_legacy_review_id" "uuid", "p_new_root_review_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_review_migrate_legacy_v2"("p_legacy_review_id" "uuid", "p_new_root_review_id" "uuid", "p_audit" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."cmd_review_migrate_legacy_v2"("p_legacy_review_id" "uuid", "p_new_root_review_id" "uuid", "p_audit" "jsonb") TO "api_internal_executor";
