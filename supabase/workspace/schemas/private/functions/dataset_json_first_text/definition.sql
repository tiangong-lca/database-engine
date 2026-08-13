CREATE OR REPLACE FUNCTION "private"."dataset_json_first_text"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
  select nullif(
    btrim(
      case
        when p_value is null or p_value = 'null'::jsonb then null
        when jsonb_typeof(p_value) = 'string' then p_value #>> '{}'
        when jsonb_typeof(p_value) = 'object' then p_value ->> '#text'
        when jsonb_typeof(p_value) = 'array' then (
          select coalesce(item ->> '#text', item #>> '{}')
          from jsonb_array_elements(p_value) as value(item)
          where nullif(btrim(coalesce(item ->> '#text', item #>> '{}')), '') is not null
          limit 1
        )
        else null
      end
    ),
    ''
  );
$$;

ALTER FUNCTION "private"."dataset_json_first_text"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_json_first_text"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_json_first_text"("p_value" "jsonb") TO "api_internal_executor";
