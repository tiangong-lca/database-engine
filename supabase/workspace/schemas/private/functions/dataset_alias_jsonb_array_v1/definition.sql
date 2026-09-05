CREATE OR REPLACE FUNCTION "private"."dataset_alias_jsonb_array_v1"("p_value" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case pg_catalog.jsonb_typeof(p_value)
    when 'array' then p_value
    when 'object' then pg_catalog.jsonb_build_array(p_value)
    else '[]'::jsonb
  end;
$$;

ALTER FUNCTION "private"."dataset_alias_jsonb_array_v1"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_alias_jsonb_array_v1"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_alias_jsonb_array_v1"("p_value" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."dataset_alias_jsonb_array_v1"("p_value" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "private"."dataset_alias_jsonb_array_v1"("p_value" "jsonb") TO "next_public_search_executor";
