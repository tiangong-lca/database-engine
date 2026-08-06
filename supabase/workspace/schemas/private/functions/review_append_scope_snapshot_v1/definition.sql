CREATE OR REPLACE FUNCTION "private"."review_append_scope_snapshot_v1"("p_root_review_id" "uuid", "p_scope_basis" "text", "p_root_revision_checksum" "text", "p_items" "jsonb", "p_created_by" "uuid" DEFAULT "auth"."uid"()) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_root private.reviews%rowtype;
  v_version integer;
  v_snapshot jsonb;
  v_history jsonb;
begin
  select review_row.*
  into v_root
  from private.reviews as review_row
  where review_row.id = p_root_review_id
  for update;

  if not found or v_root.review_kind <> 'root' then
    raise exception using errcode = 'P0002', message = 'ROOT_REVIEW_NOT_FOUND';
  end if;

  if p_scope_basis not in (
    'submitted',
    'review_metadata',
    'approved',
    'reference_repair',
    'migration'
  ) or p_root_revision_checksum !~ '^[a-f0-9]{64}$'
    or pg_catalog.jsonb_typeof(p_items) <> 'array' then
    raise exception using errcode = '22023', message = 'REVIEW_SCOPE_INVALID';
  end if;

  v_version := coalesce((v_root.scope_history->>'current_version')::integer, 0) + 1;
  v_snapshot := pg_catalog.jsonb_build_object(
    'version_no', v_version,
    'scope_basis', p_scope_basis,
    'root_revision_checksum', p_root_revision_checksum,
    'scope_checksum', private.review_scope_checksum_v1(p_items),
    'created_by', p_created_by,
    'created_at', pg_catalog.to_jsonb(pg_catalog.now()),
    'items', p_items
  );

  v_history := case
    when v_root.scope_history is null then pg_catalog.jsonb_build_object(
      'schema_version', 'review_scope.v1',
      'current_version', v_version,
      'snapshots', pg_catalog.jsonb_build_array(v_snapshot)
    )
    else pg_catalog.jsonb_set(
      pg_catalog.jsonb_set(
        v_root.scope_history,
        '{current_version}',
        pg_catalog.to_jsonb(v_version),
        false
      ),
      '{snapshots}',
      (v_root.scope_history->'snapshots') || pg_catalog.jsonb_build_array(v_snapshot),
      false
    )
  end;

  perform private.review_validate_scope_history_v1(p_root_review_id, v_history);

  perform pg_catalog.set_config('app.review_scope_write', 'on', true);
  update private.reviews
  set scope_history = v_history,
      modified_at = pg_catalog.now()
  where id = p_root_review_id;
  perform pg_catalog.set_config('app.review_scope_write', 'off', true);

  return v_history;
exception
  when others then
    perform pg_catalog.set_config('app.review_scope_write', 'off', true);
    raise;
end;
$_$;

ALTER FUNCTION "private"."review_append_scope_snapshot_v1"("p_root_review_id" "uuid", "p_scope_basis" "text", "p_root_revision_checksum" "text", "p_items" "jsonb", "p_created_by" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_append_scope_snapshot_v1"("p_root_review_id" "uuid", "p_scope_basis" "text", "p_root_revision_checksum" "text", "p_items" "jsonb", "p_created_by" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_append_scope_snapshot_v1"("p_root_review_id" "uuid", "p_scope_basis" "text", "p_root_revision_checksum" "text", "p_items" "jsonb", "p_created_by" "uuid") TO "api_internal_executor";
