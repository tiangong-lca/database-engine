CREATE OR REPLACE FUNCTION "private"."review_root_currently_references_target_v1"("p_root_review_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'pg_temp'
    AS $$
declare
  v_root private.reviews%rowtype;
  v_comment_roots jsonb;
begin
  select review_row.*
  into v_root
  from private.reviews as review_row
  where review_row.id = p_root_review_id
    and review_row.review_kind = 'root';

  if not found then
    return false;
  end if;

  if exists (
    select 1
    from api.cmd_review_collect_dataset_targets(
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'table', v_root.target_table,
        'id', v_root.data_id,
        'version', pg_catalog.btrim(v_root.data_version::text),
        'is_root', true
      )),
      false
    ) as target
    where not target.is_root
      and target.table_name = p_target_table
      and target.dataset_id = p_target_id
      and target.dataset_version = pg_catalog.btrim(p_target_version)
  ) then
    return true;
  end if;

  select coalesce(
    pg_catalog.jsonb_agg(distinct pg_catalog.jsonb_build_object(
      'table', api.cmd_review_ref_type_to_table(ref.ref_type),
      'id', ref.ref_object_id,
      'version', ref.ref_version,
      'is_root', false
    )),
    '[]'::jsonb
  )
  into v_comment_roots
  from private.comments as comment_row
  cross join lateral api.cmd_review_extract_refs(
    coalesce(comment_row.json::jsonb, '{}'::jsonb)
  ) as ref
  where comment_row.review_id = p_root_review_id
    and comment_row.state_code <> -2
    and api.cmd_review_ref_type_to_table(ref.ref_type) is not null;

  return exists (
    select 1
    from api.cmd_review_collect_dataset_targets(v_comment_roots, false) as target
    where target.table_name = p_target_table
      and target.dataset_id = p_target_id
      and target.dataset_version = pg_catalog.btrim(p_target_version)
  );
end;
$$;

ALTER FUNCTION "private"."review_root_currently_references_target_v1"("p_root_review_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_root_currently_references_target_v1"("p_root_review_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_root_currently_references_target_v1"("p_root_review_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") TO "api_internal_executor";
