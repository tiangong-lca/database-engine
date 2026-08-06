CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_text_values"("p_value" "jsonb") RETURNS "text"[]
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
  select coalesce(array_agg(distinct candidate.value order by candidate.value),
    array[]::text[])
  from (
    select case jsonb_typeof(p_value)
      when 'string' then p_value #>> '{}'
      when 'object' then p_value->>'#text'
      else null
    end as value
    union all
    select case jsonb_typeof(item.value)
      when 'string' then item.value #>> '{}'
      when 'object' then item.value->>'#text'
      else null
    end
    from jsonb_array_elements(
      case when jsonb_typeof(p_value) = 'array'
        then p_value else '[]'::jsonb end
    ) as item(value)
  ) as candidate
  where nullif(btrim(candidate.value), '') is not null
$$;

ALTER FUNCTION "private"."dataset_flow_identity_text_values"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_text_values"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_text_values"("p_value" "jsonb") TO "api_internal_executor";
