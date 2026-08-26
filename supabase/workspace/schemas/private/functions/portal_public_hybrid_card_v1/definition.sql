CREATE OR REPLACE FUNCTION "private"."portal_public_hybrid_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $$
  select private.portal_catalog_card_v1(p_kind, p_state_code, p_json)
$$;

ALTER FUNCTION "private"."portal_public_hybrid_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_public_hybrid_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_public_hybrid_card_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") TO "api_internal_executor";
