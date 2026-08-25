CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_frame_v1"(VARIADIC "p_fields" "text"[]) RETURNS "bytea"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_field text;
  v_bytes bytea;
  v_result bytea := ''::bytea;
begin
  foreach v_field in array p_fields loop
    if v_field is null then
      v_result := v_result || pg_catalog.int4send(-1);
    else
      v_bytes := pg_catalog.convert_to(v_field, 'UTF8');
      v_result := v_result
        || pg_catalog.int4send(pg_catalog.octet_length(v_bytes))
        || v_bytes;
    end if;
  end loop;
  return v_result;
end
$$;

ALTER FUNCTION "private"."portal_lcia_projection_frame_v1"(VARIADIC "p_fields" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_frame_v1"(VARIADIC "p_fields" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_lcia_projection_frame_v1"(VARIADIC "p_fields" "text"[]) TO "portal_public_executor";
