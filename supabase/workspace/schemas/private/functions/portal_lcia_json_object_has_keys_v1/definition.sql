CREATE OR REPLACE FUNCTION "private"."portal_lcia_json_object_has_keys_v1"("p_value" "jsonb", "p_keys" "text"[]) RETURNS boolean
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select jsonb_typeof(p_value) = 'object'
    and (select count(*) from jsonb_object_keys(p_value)) = cardinality(p_keys)
    and not exists (
      select 1
      from jsonb_object_keys(p_value) as key(value)
      where not (key.value = any (p_keys))
    )
$$;

ALTER FUNCTION "private"."portal_lcia_json_object_has_keys_v1"("p_value" "jsonb", "p_keys" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_json_object_has_keys_v1"("p_value" "jsonb", "p_keys" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_lcia_json_object_has_keys_v1"("p_value" "jsonb", "p_keys" "text"[]) TO "portal_public_executor";
