CREATE OR REPLACE FUNCTION "private"."assert_portal_card_context_contract_v1"() RETURNS "void"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_expected_digest constant text :=
    'e0516d5f3a641d26221a5c44b92a2e7a87cab125e9145e8141074d9bc2af39fa';
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  if private.portal_card_context_manifest_sha256_v1()
       is distinct from v_expected_digest then
    raise exception using
      errcode = '55000',
      message = 'Portal card context derivation contract drifted';
  end if;
end
$$;

ALTER FUNCTION "private"."assert_portal_card_context_contract_v1"() OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."assert_portal_card_context_contract_v1"() FROM PUBLIC;
