CREATE OR REPLACE FUNCTION "private"."review_canonical_json_text_v1"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_result text;
begin
  case pg_catalog.jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(pg_catalog.string_agg(
        pg_catalog.to_jsonb(item.key)::text
          || ':'
          || private.review_canonical_json_text_v1(item.value),
        ',' order by item.key collate "C"
      ), '') || '}'
      into v_result
      from pg_catalog.jsonb_each(p_value) as item(key, value);
    when 'array' then
      select '[' || coalesce(pg_catalog.string_agg(
        private.review_canonical_json_text_v1(item.value),
        ',' order by item.ordinality
      ), '') || ']'
      into v_result
      from pg_catalog.jsonb_array_elements(p_value)
        with ordinality as item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

ALTER FUNCTION "private"."review_canonical_json_text_v1"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_canonical_json_text_v1"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_canonical_json_text_v1"("p_value" "jsonb") TO "api_internal_executor";
