CREATE OR REPLACE FUNCTION "private"."portal_process_rank_name_keys_v1"("p_card" "jsonb") RETURNS "text"[]
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    pg_catalog.array_agg(
      distinct normalized.value order by normalized.value
    ),
    '{}'::text[]
  )
  from pg_catalog.jsonb_array_elements(
    case when pg_catalog.jsonb_typeof(p_card -> 'names') = 'array'
      then p_card -> 'names' else '[]'::jsonb end
  ) as item(value)
  cross join lateral (
    select pg_catalog.lower(
      pg_catalog.btrim(item.value ->> 'value')
    ) as value
  ) as normalized
  where nullif(normalized.value, '') is not null
$$;

ALTER FUNCTION "private"."portal_process_rank_name_keys_v1"("p_card" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_process_rank_name_keys_v1"("p_card" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_process_rank_name_keys_v1"("p_card" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "private"."portal_process_rank_name_keys_v1"("p_card" "jsonb") TO "postgres";
