CREATE OR REPLACE FUNCTION "private"."portal_first_text_v1"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select item ->> 'value'
  from jsonb_array_elements(private.portal_localized_text_v1(p_value)) as localized(item)
  order by case item ->> 'language' when 'en' then 0 when 'zh' then 1 else 2 end
  limit 1
$$;

ALTER FUNCTION "private"."portal_first_text_v1"("p_value" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_first_text_v1"("p_value" "jsonb") FROM PUBLIC;
