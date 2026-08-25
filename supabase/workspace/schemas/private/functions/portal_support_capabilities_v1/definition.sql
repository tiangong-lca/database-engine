CREATE OR REPLACE FUNCTION "private"."portal_support_capabilities_v1"("p_kind" "text", "p_state_code" integer) RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'exchangesVisible', p_kind in ('flow', 'flowproperty', 'unitgroup') and p_state_code = 100,
    'policyVersion', 'portal-capability-policy.v1',
    'reasonCodes', case
      when p_kind not in ('flow', 'flowproperty', 'unitgroup')
        then jsonb_build_array('unsupported_support_kind')
      when p_state_code = 100
        then jsonb_build_array('support_state_100_public')
      when p_state_code = 200
        then jsonb_build_array('support_state_200_metadata_only')
      else jsonb_build_array('support_state_not_public')
    end
  )
$$;

ALTER FUNCTION "private"."portal_support_capabilities_v1"("p_kind" "text", "p_state_code" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_support_capabilities_v1"("p_kind" "text", "p_state_code" integer) FROM PUBLIC;
