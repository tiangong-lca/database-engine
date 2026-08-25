CREATE OR REPLACE FUNCTION "private"."portal_cursor_decode_v1"("p_cursor" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_base64 text;
  v_result jsonb;
begin
  if p_cursor is null
     or length(p_cursor) = 0
     or length(p_cursor) > 4096
     or p_cursor !~ '^[A-Za-z0-9_-]+$' then
    return null;
  end if;
  v_base64 := translate(p_cursor, '-_', '+/');
  v_base64 := v_base64 || repeat('=', (4 - length(v_base64) % 4) % 4);
  begin
    v_result := convert_from(decode(v_base64, 'base64'), 'UTF8')::jsonb;
  exception
    when others then
      return null;
  end;
  if jsonb_typeof(v_result) <> 'object' then
    return null;
  end if;
  return v_result;
end
$_$;

ALTER FUNCTION "private"."portal_cursor_decode_v1"("p_cursor" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_cursor_decode_v1"("p_cursor" "text") FROM PUBLIC;
