CREATE OR REPLACE FUNCTION "private"."portal_lcia_safe_audit_v1"("p_value" "jsonb") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  with recursive nodes(key_name, value) as (
    select null::text, p_value
    union all
    select child.key_name, child.value
    from nodes as parent
    cross join lateral (
      select member.key as key_name, member.value
      from jsonb_each(
        case jsonb_typeof(parent.value)
          when 'object' then parent.value
          else '{}'::jsonb
        end
      ) as member(key, value)
      union all
      select null::text, member.value
      from jsonb_array_elements(
        case jsonb_typeof(parent.value)
          when 'array' then parent.value
          else '[]'::jsonb
        end
      ) as member(value)
    ) as child
  )
  select coalesce(jsonb_typeof(p_value) = 'object', false)
    and pg_catalog.pg_column_size(p_value) <= 16384
    and not exists (
      select 1
      from nodes
      where coalesce(lower(key_name), '') ~
              '(url|uri|bucket|objectpath|storagepath|locator|credential|secret|token|authorization|cookie|password|api.?key)'
         or (
           jsonb_typeof(value) = 'string'
           and lower(value #>> '{}') ~ '(https?://|s3://|gs://)'
         )
    )
$$;

ALTER FUNCTION "private"."portal_lcia_safe_audit_v1"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_safe_audit_v1"("p_value" "jsonb") FROM PUBLIC;
