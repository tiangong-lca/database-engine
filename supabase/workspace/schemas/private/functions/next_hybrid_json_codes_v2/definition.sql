CREATE OR REPLACE FUNCTION "private"."next_hybrid_json_codes_v2"("p_value" "jsonb", "p_attribute" "text") RETURNS "text"[]
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    pg_catalog.array_agg(code.value order by code.value),
    '{}'::text[]
  )
  from (
    select distinct nullif(pg_catalog.btrim(item.value ->> p_attribute), '') as value
    from pg_catalog.jsonb_array_elements(
      private.dataset_alias_jsonb_array_v1(p_value)
    ) as item(value)
  ) as code
  where code.value is not null;
$$;

ALTER FUNCTION "private"."next_hybrid_json_codes_v2"("p_value" "jsonb", "p_attribute" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."next_hybrid_json_codes_v2"("p_value" "jsonb", "p_attribute" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."next_hybrid_json_codes_v2"("p_value" "jsonb", "p_attribute" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "private"."next_hybrid_json_codes_v2"("p_value" "jsonb", "p_attribute" "text") TO "next_public_search_executor";
