CREATE OR REPLACE FUNCTION "private"."portal_cursor_encode_v1"("p_payload" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select rtrim(
    translate(
      replace(
        replace(encode(convert_to(p_payload::text, 'UTF8'), 'base64'), E'\n', ''),
        E'\r',
        ''
      ),
      '+/',
      '-_'
    ),
    '='
  )
$$;

ALTER FUNCTION "private"."portal_cursor_encode_v1"("p_payload" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_cursor_encode_v1"("p_payload" "jsonb") FROM PUBLIC;
