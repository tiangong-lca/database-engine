CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_short_description_v2"("p_value" "jsonb") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case jsonb_typeof(p_value)
    when 'string' then true
    when 'object' then
      private.dataset_flow_identity_exact_keys(
        p_value, array['@xml:lang', '#text']
      )
      and jsonb_typeof(p_value->'@xml:lang') = 'string'
      and jsonb_typeof(p_value->'#text') = 'string'
    when 'array' then
      jsonb_array_length(p_value) >= 1
      and not exists (
        select 1
        from jsonb_array_elements(p_value) as item(value)
        where jsonb_typeof(item.value) <> 'object'
          or not private.dataset_flow_identity_exact_keys(
            item.value, array['@xml:lang', '#text']
          )
          or jsonb_typeof(item.value->'@xml:lang') <> 'string'
          or jsonb_typeof(item.value->'#text') <> 'string'
      )
    else false
  end
$$;

ALTER FUNCTION "private"."dataset_flow_identity_short_description_v2"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_short_description_v2"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_short_description_v2"("p_value" "jsonb") TO "api_internal_executor";
