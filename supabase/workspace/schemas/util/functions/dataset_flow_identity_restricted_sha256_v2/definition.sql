CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_restricted_sha256_v2"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "plpgsql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
declare
  v_normalized jsonb;
begin
  v_normalized := private.dataset_flow_identity_safe_json_v2(p_value);
  if v_normalized is null then return null; end if;
  return encode(
    extensions.digest(
      convert_to(private.dataset_alias_canonical_jsonb_v1(v_normalized), 'UTF8'),
      'sha256'
    ),
    'hex'
  );
end;
$$;

ALTER FUNCTION "util"."dataset_flow_identity_restricted_sha256_v2"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_restricted_sha256_v2"("p_value" "jsonb") FROM PUBLIC;
