CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_worker_canonical_json_text"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_result text;
begin
  case jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(string_agg(
        to_jsonb(item.key)::text
          || ':'
          || private.lcia_scope_closure_worker_canonical_json_text(item.value),
        ',' order by item.key collate "C"
      ), '') || '}'
      into v_result
      from jsonb_each(p_value) as item(key, value);
    when 'array' then
      select '[' || coalesce(string_agg(
        private.lcia_scope_closure_worker_canonical_json_text(item.value),
        ',' order by item.ordinality
      ), '') || ']'
      into v_result
      from jsonb_array_elements(p_value)
        with ordinality as item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

ALTER FUNCTION "private"."lcia_scope_closure_worker_canonical_json_text"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_worker_canonical_json_text"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_worker_canonical_json_text"("p_value" "jsonb") TO "api_internal_executor";
