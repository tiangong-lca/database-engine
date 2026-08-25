CREATE OR REPLACE FUNCTION "private"."portal_access_restrictions_open_v1"("p_value" "jsonb") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case
    when p_value is null or p_value = 'null'::jsonb then true
    when jsonb_typeof(p_value) not in ('array', 'object', 'string') then false
    else not exists (
      select 1
      from jsonb_array_elements(
        case jsonb_typeof(p_value)
          when 'array' then p_value
          else jsonb_build_array(p_value)
        end
      ) as restriction(value)
      where case jsonb_typeof(restriction.value)
        when 'object' then case
          when restriction.value ? '#text'
            and jsonb_typeof(restriction.value -> '#text') = 'string'
            then lower(private.portal_scalar_text_v1(restriction.value -> '#text'))
          else '__invalid__'
        end
        when 'string' then lower(private.portal_scalar_text_v1(restriction.value))
        else '__invalid__'
      end not in ('', 'none')
    )
  end
$$;

ALTER FUNCTION "private"."portal_access_restrictions_open_v1"("p_value" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_access_restrictions_open_v1"("p_value" "jsonb") FROM PUBLIC;
