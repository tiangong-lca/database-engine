CREATE OR REPLACE FUNCTION "private"."catalog_portal_projection_payload_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $$
declare
  v_card jsonb;
begin
  v_card := private.portal_catalog_card_v1(
    p_kind,
    p_state_code,
    p_json
  );
  if pg_catalog.jsonb_typeof(v_card) <> 'object' then
    return null;
  end if;
  return pg_catalog.jsonb_build_object(
    'card', v_card,
    'document', coalesce(v_card ->> 'document', '')
  );
end
$$;

ALTER FUNCTION "private"."catalog_portal_projection_payload_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_projection_payload_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."catalog_portal_projection_payload_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") TO "api_internal_executor";
