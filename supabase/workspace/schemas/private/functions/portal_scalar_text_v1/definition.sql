CREATE OR REPLACE FUNCTION "private"."portal_scalar_text_v1"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case
    when jsonb_typeof(p_value) = 'string' then btrim(p_value #>> '{}')
    else null
  end
$$;

ALTER FUNCTION "private"."portal_scalar_text_v1"("p_value" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_scalar_text_v1"("p_value" "jsonb") FROM PUBLIC;
