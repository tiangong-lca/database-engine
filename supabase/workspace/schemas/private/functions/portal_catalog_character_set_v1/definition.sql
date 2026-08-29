CREATE OR REPLACE FUNCTION "private"."portal_catalog_character_set_v1"("p_value" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE SECURITY DEFINER PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    pg_catalog.string_agg(
      distinct_character.value,
      ''
      order by distinct_character.value collate pg_catalog."C"
    ),
    ''
  )
  from (
    select distinct character.value
    from pg_catalog.regexp_split_to_table(
      coalesce(p_value, ''),
      ''
    ) as character(value)
    where character.value <> ''
  ) as distinct_character
$$;

ALTER FUNCTION "private"."portal_catalog_character_set_v1"("p_value" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_catalog_character_set_v1"("p_value" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_catalog_character_set_v1"("p_value" "text") TO "api_internal_executor";
