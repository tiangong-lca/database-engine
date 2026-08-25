CREATE OR REPLACE FUNCTION "private"."portal_lcia_localized_text_frame_hex_v1"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select pg_catalog.encode(
    private.portal_lcia_projection_frame_v1(
      variadic (
        array[jsonb_array_length(p_value)::text]
        || coalesce(
          (
            select array_agg(field.value order by item.ordinality, field.position)
            from jsonb_array_elements(p_value)
              with ordinality as item(value, ordinality)
            cross join lateral (
              values
                (1, item.value ->> 'language'),
                (2, item.value ->> 'value')
            ) as field(position, value)
          ),
          '{}'::text[]
        )
      )
    ),
    'hex'
  )
$$;

ALTER FUNCTION "private"."portal_lcia_localized_text_frame_hex_v1"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_localized_text_frame_hex_v1"("p_value" "jsonb") FROM PUBLIC;
