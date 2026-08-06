CREATE OR REPLACE FUNCTION "private"."review_scope_current_reference_ids_v1"("p_scope_history" "jsonb") RETURNS "uuid"[]
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    pg_catalog.array_agg(distinct item.reference_review_id
      order by item.reference_review_id)
      filter (where item.reference_review_id is not null),
    array[]::uuid[]
  )
  from private.review_scope_current_items_v1(p_scope_history) as item
  where item.item_kind = 'reference'
$$;

ALTER FUNCTION "private"."review_scope_current_reference_ids_v1"("p_scope_history" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_scope_current_reference_ids_v1"("p_scope_history" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_scope_current_reference_ids_v1"("p_scope_history" "jsonb") TO "api_internal_executor";
