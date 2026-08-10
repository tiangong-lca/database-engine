CREATE OR REPLACE FUNCTION "private"."review_rebind_active_roots_to_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_reference_review_id" "uuid", "p_actor" "uuid") RETURNS "uuid"[]
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_target_id uuid := nullif(p_target_row->>'id', '')::uuid;
  v_target_version text := nullif(p_target_row->>'version', '');
  v_owner_id uuid := nullif(p_target_row->>'user_id', '')::uuid;
  v_team_id uuid := nullif(p_target_row->>'team_id', '')::uuid;
  v_old_reference_ids uuid[] := array[]::uuid[];
  v_rebound_root_ids uuid[] := array[]::uuid[];
  v_impacted_root private.reviews%rowtype;
  v_current_snapshot jsonb;
  v_repair_items jsonb;
begin
  if p_target_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  )
    or v_target_id is null
    or v_target_version is null
    or v_owner_id is null
    or p_checksum !~ '^[a-f0-9]{64}$'
    or not exists (
      select 1
      from private.reviews as current_reference
      where current_reference.id = p_reference_review_id
        and current_reference.review_kind = 'reference'
        and current_reference.target_table = p_target_table
        and current_reference.data_id = v_target_id
        and btrim(current_reference.data_version::text) = v_target_version
        and current_reference.submitted_revision_checksum = p_checksum
        and current_reference.state_code in (0, 1, 2)
    ) then
    raise exception using
      errcode = '22023',
      message = 'REFERENCE_REBIND_TARGET_INVALID';
  end if;

  select coalesce(
    array_agg(rejected.id order by rejected.id),
    array[]::uuid[]
  )
  into v_old_reference_ids
  from private.reviews as rejected
  where rejected.review_kind = 'reference'
    and rejected.target_table = p_target_table
    and rejected.data_id = v_target_id
    and btrim(rejected.data_version::text) = v_target_version
    and rejected.state_code = -1;

  if cardinality(v_old_reference_ids) = 0 then
    return v_rebound_root_ids;
  end if;

  for v_impacted_root in
    select root_review.*
    from private.reviews as root_review
    where root_review.review_kind = 'root'
      and root_review.state_code in (0, 1)
      and root_review.current_reference_review_ids && v_old_reference_ids
    order by root_review.id
    for update
  loop
    v_current_snapshot := private.review_scope_current_snapshot_v1(
      v_impacted_root.scope_history
    );
    v_repair_items := private.review_replace_reference_item_v1(
      v_current_snapshot->'items',
      p_target_table,
      v_target_id,
      v_target_version,
      p_checksum,
      p_reference_review_id,
      v_owner_id,
      v_team_id
    );

    if v_repair_items = v_current_snapshot->'items' then
      continue;
    end if;

    perform private.review_append_scope_snapshot_v1(
      v_impacted_root.id,
      'reference_repair',
      v_current_snapshot->>'root_revision_checksum',
      v_repair_items,
      p_actor
    );

    perform private.review_notify_event_v1(
      'reference_repaired',
      p_reference_review_id,
      v_impacted_root.target_owner_id,
      p_actor,
      p_target_table,
      v_target_id,
      v_target_version,
      v_impacted_root.id,
      (v_impacted_root.scope_history->>'current_version')::integer + 1,
      null
    );

    v_rebound_root_ids := array_append(
      v_rebound_root_ids,
      v_impacted_root.id
    );
  end loop;

  return v_rebound_root_ids;
end;
$_$;

ALTER FUNCTION "private"."review_rebind_active_roots_to_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_reference_review_id" "uuid", "p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_rebind_active_roots_to_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_reference_review_id" "uuid", "p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_rebind_active_roots_to_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_reference_review_id" "uuid", "p_actor" "uuid") TO "api_internal_executor";
