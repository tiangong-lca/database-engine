CREATE OR REPLACE FUNCTION "private"."dataset_alias_js_object_key_sort_key_v1"("p_value" "text") RETURNS "bytea"
    LANGUAGE "plpgsql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_result bytea := '\x01'::bytea;
  v_array_index bigint;
  v_code_point integer;
  v_supplementary integer;
begin
  -- stableJsonText sorts keys by JavaScript UTF-16 order before rebuilding an
  -- object. JSON.stringify then enumerates canonical array-index keys first in
  -- ascending numeric order. Reproduce both rules with a binary prefix plus a
  -- big-endian numeric or UTF-16 payload. PostgreSQL text cannot contain lone
  -- surrogates.
  if p_value ~ '^(0|[1-9][0-9]{0,9})$' then
    v_array_index := p_value::bigint;
    if v_array_index <= 4294967294 then
      return '\x00'::bytea || int8send(v_array_index);
    end if;
  end if;

  for v_character_index in 1..character_length(p_value) loop
    v_code_point := ascii(substring(p_value from v_character_index for 1));

    if v_code_point <= 65535 then
      v_result := v_result || decode(
        lpad(to_hex(v_code_point), 4, '0'),
        'hex'
      );
    else
      v_supplementary := v_code_point - 65536;
      v_result := v_result || decode(
        lpad(to_hex(55296 + (v_supplementary / 1024)), 4, '0')
          || lpad(to_hex(56320 + (v_supplementary % 1024)), 4, '0'),
        'hex'
      );
    end if;
  end loop;

  return v_result;
end;
$_$;

ALTER FUNCTION "private"."dataset_alias_js_object_key_sort_key_v1"("p_value" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_alias_js_object_key_sort_key_v1"("p_value" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_alias_js_object_key_sort_key_v1"("p_value" "text") TO "api_internal_executor";
