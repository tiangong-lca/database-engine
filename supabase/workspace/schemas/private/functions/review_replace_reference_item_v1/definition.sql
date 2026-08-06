CREATE OR REPLACE FUNCTION "private"."review_replace_reference_item_v1"("p_items" "jsonb", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_checksum" "text", "p_reference_review_id" "uuid", "p_owner_id" "uuid", "p_team_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
  select coalesce(jsonb_agg(
    case
      when item.value->>'item_kind' = 'reference'
        and item.value->>'target_table' = p_target_table
        and item.value->>'data_id' = p_target_id::text
        and item.value->>'data_version' = p_target_version
      then item.value || jsonb_build_object(
        'submitted_revision_checksum', p_checksum,
        'reference_review_id', p_reference_review_id,
        'target_owner_id', p_owner_id,
        'target_team_id', p_team_id,
        'introduced_by', 'reference_repair'
      )
      else item.value
    end
    order by item.ordinality
  ), '[]'::jsonb)
  from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
    with ordinality as item(value, ordinality)
$$;

ALTER FUNCTION "private"."review_replace_reference_item_v1"("p_items" "jsonb", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_checksum" "text", "p_reference_review_id" "uuid", "p_owner_id" "uuid", "p_team_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_replace_reference_item_v1"("p_items" "jsonb", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_checksum" "text", "p_reference_review_id" "uuid", "p_owner_id" "uuid", "p_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_replace_reference_item_v1"("p_items" "jsonb", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_checksum" "text", "p_reference_review_id" "uuid", "p_owner_id" "uuid", "p_team_id" "uuid") TO "api_internal_executor";
