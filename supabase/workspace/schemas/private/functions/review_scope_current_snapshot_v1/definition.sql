CREATE OR REPLACE FUNCTION "private"."review_scope_current_snapshot_v1"("p_scope_history" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case
    when pg_catalog.jsonb_typeof(p_scope_history) <> 'object'
      or p_scope_history->>'schema_version' <> 'review_scope.v1'
      or pg_catalog.jsonb_typeof(p_scope_history->'snapshots') <> 'array'
      then null
    else (
      select snapshot.value
      from pg_catalog.jsonb_array_elements(p_scope_history->'snapshots')
        with ordinality as snapshot(value, ordinality)
      where snapshot.value->>'version_no' = p_scope_history->>'current_version'
      order by snapshot.ordinality desc
      limit 1
    )
  end
$$;

ALTER FUNCTION "private"."review_scope_current_snapshot_v1"("p_scope_history" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_scope_current_snapshot_v1"("p_scope_history" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_scope_current_snapshot_v1"("p_scope_history" "jsonb") TO "api_internal_executor";
