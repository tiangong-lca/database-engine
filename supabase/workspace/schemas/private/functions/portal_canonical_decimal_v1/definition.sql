CREATE OR REPLACE FUNCTION "private"."portal_canonical_decimal_v1"("p_value" "text") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_input text := btrim(p_value);
  v_match text[];
  v_exponent integer;
  v_number numeric;
  v_output text;
  v_digits text;
begin
  if p_value is null
     or length(v_input) = 0
     or length(v_input) > 128 then
    return null;
  end if;

  v_match := regexp_match(
    v_input,
    '^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$'
  );
  if v_match is null
     or coalesce(length(v_match[2]), 0) + coalesce(length(v_match[3]), 0) = 0 then
    return null;
  end if;

  if v_match[4] is not null then
    if length(ltrim(v_match[4], '+-')) > 4 then
      return null;
    end if;
    v_exponent := v_match[4]::integer;
    if abs(v_exponent) > 1000 then
      return null;
    end if;
  end if;

  begin
    v_number := v_input::numeric;
    v_output := trim_scale(v_number)::text;
  exception
    when others then
      return null;
  end;

  if v_output ~ '[eE+]' or length(v_output) > 2048 then
    return null;
  end if;
  if v_number = 0 then
    return '0';
  end if;

  v_digits := regexp_replace(v_output, '[^0-9]', '', 'g');
  if length(v_digits) not between 1 and 38 then
    return null;
  end if;

  return v_output;
end
$_$;

ALTER FUNCTION "private"."portal_canonical_decimal_v1"("p_value" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_canonical_decimal_v1"("p_value" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_canonical_decimal_v1"("p_value" "text") TO "postgres";
