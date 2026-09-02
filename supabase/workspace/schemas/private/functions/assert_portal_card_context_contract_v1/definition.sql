CREATE OR REPLACE FUNCTION "private"."assert_portal_card_context_contract_v1"() RETURNS "void"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_expected_digest constant text :=
    'db78336c8604848af1e068352f8a39d9ee740308c44c59c639b986ed2660c47e';
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
