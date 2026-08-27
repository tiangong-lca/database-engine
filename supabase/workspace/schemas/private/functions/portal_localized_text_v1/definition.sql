CREATE OR REPLACE FUNCTION "private"."portal_localized_text_v1"("p_value" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
  with items as (
    select item.value, item.ordinality
    from jsonb_array_elements(
      case jsonb_typeof(p_value)
        when 'array' then p_value
        when 'null' then '[]'::jsonb
        else jsonb_build_array(p_value)
      end
    ) with ordinality as item(value, ordinality)
  ), normalized as (
    select
      case
        when jsonb_typeof(value) = 'object'
          and btrim(coalesce(value ->> '@xml:lang', '')) ~ '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
          and length(btrim(value ->> '@xml:lang')) <= 35
          then btrim(value ->> '@xml:lang')
        else 'und'
      end as language,
      case
        when jsonb_typeof(value) = 'object'
          then private.portal_scalar_text_v1(value -> '#text')
        when jsonb_typeof(value) = 'string'
          then private.portal_scalar_text_v1(value)
        else null
      end as text_value,
      ordinality
    from items
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object('language', language, 'value', btrim(text_value))
      order by ordinality
    ) filter (where nullif(btrim(text_value), '') is not null),
    '[]'::jsonb
  )
  from normalized
$_$;

ALTER FUNCTION "private"."portal_localized_text_v1"("p_value" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_localized_text_v1"("p_value" "jsonb") FROM PUBLIC;
