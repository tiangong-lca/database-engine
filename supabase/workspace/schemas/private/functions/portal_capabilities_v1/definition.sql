CREATE OR REPLACE FUNCTION "private"."portal_capabilities_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
declare
  v_publication jsonb := private.portal_publication_root_v1(p_kind, p_json);
  v_license text := private.portal_scalar_text_v1(v_publication -> 'common:licenseType');
  v_exclusive jsonb := v_publication -> 'common:referenceToEntitiesWithExclusiveAccess';
  v_restrictions jsonb := v_publication -> 'common:accessRestrictions';
  v_exclusive_missing boolean;
  v_restrictions_open boolean;
  v_open boolean;
  v_reasons jsonb := '[]'::jsonb;
begin
  v_exclusive_missing := v_exclusive is null
    or v_exclusive = 'null'::jsonb;
  v_restrictions_open := private.portal_access_restrictions_open_v1(v_restrictions);
  v_open := coalesce(p_state_code = 100
    and v_license = 'Free of charge for all users and uses'
    and v_exclusive_missing
    and v_restrictions_open, false);

  if p_state_code = 200 then
    v_reasons := v_reasons || '"state_200_metadata_only"'::jsonb;
  elsif p_state_code <> 100 then
    v_reasons := v_reasons || '"state_not_public"'::jsonb;
  end if;
  if v_license is distinct from 'Free of charge for all users and uses' then
    v_reasons := v_reasons || '"license_not_fully_open"'::jsonb;
  end if;
  if not v_exclusive_missing then
    v_reasons := v_reasons || '"exclusive_access_declared"'::jsonb;
  end if;
  if not v_restrictions_open then
    v_reasons := v_reasons || '"access_restrictions_present"'::jsonb;
  end if;
  if v_open then
    v_reasons := '[]'::jsonb || '"public_license_confirmed"'::jsonb;
  end if;

  return jsonb_build_object(
    'metadataVisible', p_state_code in (100, 200),
    'exchangesVisible', v_open,
    'lciaVisible', false,
    'publicArtifactVisible', false,
    'citationVisible', p_state_code in (100, 200),
    'policyVersion', 'portal-capability-policy.v1',
    'reasonCodes', v_reasons
  );
end
$$;

ALTER FUNCTION "private"."portal_capabilities_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_capabilities_v1"("p_kind" "text", "p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;
