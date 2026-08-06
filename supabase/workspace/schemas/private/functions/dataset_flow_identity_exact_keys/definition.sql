CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_exact_keys"("p_value" "jsonb", "p_keys" "text"[]) RETURNS boolean
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select jsonb_typeof(p_value) = 'object'
    and p_value ?& p_keys
    and not exists (
      select 1
      from jsonb_object_keys(p_value) as actual(key)
      where actual.key <> all (p_keys)
    )
$$;

ALTER FUNCTION "private"."dataset_flow_identity_exact_keys"("p_value" "jsonb", "p_keys" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_exact_keys"("p_value" "jsonb", "p_keys" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_exact_keys"("p_value" "jsonb", "p_keys" "text"[]) TO "api_internal_executor";
