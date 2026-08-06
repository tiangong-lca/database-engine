CREATE OR REPLACE FUNCTION "private"."dataset_alias_canonical_jsonb_v1"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "plpgsql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
declare
  v_result text;
begin
  case jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(string_agg(
        to_jsonb(object_item.key)::text
          || ':'
          || private.dataset_alias_canonical_jsonb_v1(object_item.value),
        ',' order by private.dataset_alias_js_object_key_sort_key_v1(object_item.key)
      ), '') || '}'
      into v_result
      from jsonb_each(p_value) as object_item(key, value);
    when 'array' then
      select '[' || coalesce(string_agg(
        private.dataset_alias_canonical_jsonb_v1(array_item.value),
        ',' order by array_item.ordinality
      ), '') || ']'
      into v_result
      from jsonb_array_elements(p_value)
        with ordinality as array_item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

ALTER FUNCTION "private"."dataset_alias_canonical_jsonb_v1"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_alias_canonical_jsonb_v1"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_alias_canonical_jsonb_v1"("p_value" "jsonb") TO "api_internal_executor";
