CREATE OR REPLACE FUNCTION "private"."review_scope_current_items_v1"("p_scope_history" "jsonb") RETURNS TABLE("item_kind" "text", "target_table" "text", "data_id" "uuid", "data_version" "text", "submitted_revision_checksum" "text", "reference_review_id" "uuid", "target_owner_id" "uuid", "target_team_id" "uuid", "relation_type" "text", "relation_path" "text", "introduced_by" "text", "introduced_field_path" "text")
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select
    item.value->>'item_kind',
    item.value->>'target_table',
    nullif(item.value->>'data_id', '')::uuid,
    item.value->>'data_version',
    item.value->>'submitted_revision_checksum',
    nullif(item.value->>'reference_review_id', '')::uuid,
    nullif(item.value->>'target_owner_id', '')::uuid,
    nullif(item.value->>'target_team_id', '')::uuid,
    item.value->>'relation_type',
    item.value->>'relation_path',
    item.value->>'introduced_by',
    nullif(item.value->>'introduced_field_path', '')
  from pg_catalog.jsonb_array_elements(
    coalesce(
      private.review_scope_current_snapshot_v1(p_scope_history)->'items',
      '[]'::jsonb
    )
  ) as item(value)
$$;

ALTER FUNCTION "private"."review_scope_current_items_v1"("p_scope_history" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_scope_current_items_v1"("p_scope_history" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_scope_current_items_v1"("p_scope_history" "jsonb") TO "api_internal_executor";
