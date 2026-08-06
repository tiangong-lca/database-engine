CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_safe_json_v2"("p_value" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_kind text := jsonb_typeof(p_value);
  v_number numeric;
  v_result jsonb;
  v_children_valid boolean;
begin
  case v_kind
    when 'null' then return 'null'::jsonb;
    when 'string' then return p_value;
    when 'boolean' then return p_value;
    when 'number' then
      begin
        v_number := (p_value #>> '{}')::numeric;
      exception when others then
        return null;
      end;
      if v_number <> trunc(v_number)
        or v_number < -9007199254740991::numeric
        or v_number > 9007199254740991::numeric then
        return null;
      end if;
      return to_jsonb(v_number::bigint);
    when 'array' then
      with children as materialized (
        select item.ordinality,
          private.dataset_flow_identity_safe_json_v2(item.value) as value
        from jsonb_array_elements(p_value)
          with ordinality as item(value, ordinality)
      )
      select coalesce(bool_and(children.value is not null), true),
        coalesce(jsonb_agg(children.value order by children.ordinality),
          '[]'::jsonb)
      into v_children_valid, v_result
      from children;
      if not v_children_valid then return null; end if;
      return v_result;
    when 'object' then
      with children as materialized (
        select item.key,
          private.dataset_alias_js_object_key_sort_key_v1(item.key)
            as sort_key,
          private.dataset_flow_identity_safe_json_v2(item.value) as value
        from jsonb_each(p_value) as item(key, value)
      )
      select coalesce(bool_and(children.value is not null), true),
        coalesce(jsonb_object_agg(
          children.key, children.value order by children.sort_key
        ), '{}'::jsonb)
      into v_children_valid, v_result
      from children;
      if not v_children_valid then return null; end if;
      return v_result;
    else
      return null;
  end case;
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_safe_json_v2"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_safe_json_v2"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_safe_json_v2"("p_value" "jsonb") TO "api_internal_executor";
