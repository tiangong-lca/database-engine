CREATE OR REPLACE FUNCTION "private"."portal_datetime_v1"("p_value" "text") RETURNS "text"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_timestamp timestamptz;
begin
  if nullif(btrim(coalesce(p_value, '')), '') is null
     or length(p_value) > 64
     or p_value !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$' then
    return null;
  end if;
  begin
    v_timestamp := p_value::timestamptz;
  exception
    when others then
      return null;
  end;
  return private.portal_timestamp_v1(v_timestamp);
end
$_$;

ALTER FUNCTION "private"."portal_datetime_v1"("p_value" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_datetime_v1"("p_value" "text") FROM PUBLIC;
