CREATE OR REPLACE FUNCTION "private"."portal_normalize_filters_v1"("p_filters" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    jsonb_object_agg(
      filter.key,
      case
        when filter.key in (
          'accessLevel', 'geography', 'classification', 'processSubtype', 'source'
        ) and jsonb_typeof(filter.value) = 'string'
          then to_jsonb(lower(btrim(filter.value #>> '{}')))
        else filter.value
      end
      order by filter.key
    ),
    '{}'::jsonb
  )
  from jsonb_each(coalesce(p_filters, '{}'::jsonb)) as filter(key, value)
$$;

ALTER FUNCTION "private"."portal_normalize_filters_v1"("p_filters" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_normalize_filters_v1"("p_filters" "jsonb") FROM PUBLIC;
