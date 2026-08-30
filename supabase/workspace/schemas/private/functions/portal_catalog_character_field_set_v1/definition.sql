CREATE OR REPLACE FUNCTION "private"."portal_catalog_character_field_set_v1"("p_items" "jsonb", "p_key" "text", "p_exact_one" boolean) RETURNS "text"
    LANGUAGE "sql" IMMUTABLE SECURITY DEFINER PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select private.portal_catalog_character_set_v1(
    coalesce(
      pg_catalog.string_agg(normalized.value, '' order by normalized.ordinality),
      ''
    )
  )
  from (
    select item.ordinality,
      pg_catalog.lower(pg_catalog.btrim(item.value ->> p_key)) as value
    from pg_catalog.jsonb_array_elements(
      case
        when pg_catalog.jsonb_typeof(p_items) = 'array' then p_items
        else '[]'::jsonb
      end
    ) with ordinality as item(value, ordinality)
    where p_key in ('value', 'code')
      and pg_catalog.jsonb_typeof(item.value) = 'object'
      and pg_catalog.jsonb_typeof(item.value -> p_key) = 'string'
      and nullif(pg_catalog.btrim(item.value ->> p_key), '') is not null
      and (
        not p_exact_one
        or pg_catalog.char_length(
          pg_catalog.lower(pg_catalog.btrim(item.value ->> p_key))
        ) = 1
      )
  ) as normalized
$$;

ALTER FUNCTION "private"."portal_catalog_character_field_set_v1"("p_items" "jsonb", "p_key" "text", "p_exact_one" boolean) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_character_field_set_v1"("p_items" "jsonb", "p_key" "text", "p_exact_one" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_catalog_character_field_set_v1"("p_items" "jsonb", "p_key" "text", "p_exact_one" boolean) TO "api_internal_executor";
