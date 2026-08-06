CREATE OR REPLACE FUNCTION "private"."review_scope_all_reference_ids_v1"("p_scope_history" "jsonb") RETURNS "uuid"[]
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  select case
    when pg_catalog.jsonb_typeof(p_scope_history) <> 'object'
      or p_scope_history->>'schema_version' <> 'review_scope.v1'
      or pg_catalog.jsonb_typeof(p_scope_history->'snapshots') <> 'array'
      then array[]::uuid[]
    else coalesce((
      select pg_catalog.array_agg(distinct
        nullif(item.value->>'reference_review_id', '')::uuid
        order by nullif(item.value->>'reference_review_id', '')::uuid
      ) filter (
        where item.value->>'item_kind' = 'reference'
          and coalesce(item.value->>'reference_review_id', '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
      from pg_catalog.jsonb_array_elements(p_scope_history->'snapshots')
        as snapshot(value)
      cross join lateral pg_catalog.jsonb_array_elements(
        coalesce(snapshot.value->'items', '[]'::jsonb)
      ) as item(value)
    ), array[]::uuid[])
  end
$_$;

ALTER FUNCTION "private"."review_scope_all_reference_ids_v1"("p_scope_history" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_scope_all_reference_ids_v1"("p_scope_history" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_scope_all_reference_ids_v1"("p_scope_history" "jsonb") TO "api_internal_executor";
