CREATE OR REPLACE FUNCTION "api"."qry_portal_lcia_projection_publication_readback_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_binding record;
  v_projection record;
  v_recomputed jsonb;
  v_publicly_visible boolean;
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_lcia_result_publication_id is null
     or coalesce(p_projection_content_hash, '') !~ '^[0-9a-f]{64}$' then
    return api.lcia_result_error(
      'invalid_projection_request', 400, 'Invalid projection readback request'
    );
  end if;

  select binding.* into v_binding
  from private.portal_lcia_projection_publications as binding
  where binding.lcia_result_publication_id = p_lcia_result_publication_id;
  if v_binding.id is null then
    return api.lcia_result_error(
      'projection_publication_not_found', 404,
      'Projection publication binding was not found'
    );
  end if;
  if v_binding.projection_content_hash <> p_projection_content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection content hash does not match the binding'
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_binding.projection_id;
  v_recomputed := private.portal_lcia_projection_recompute_evidence_v1(
    v_projection.id
  );
  if coalesce((v_recomputed ->> 'ok')::boolean, false) is not true
     or v_recomputed -> 'data' ->> 'processAxisHash'
          is distinct from v_projection.process_axis_hash
     or v_recomputed -> 'data' ->> 'impactAxisHash'
          is distinct from v_projection.impact_axis_hash
     or v_recomputed -> 'data' ->> 'valueGridHash'
          is distinct from v_projection.value_grid_hash
     or v_recomputed -> 'data' ->> 'relationHash'
          is distinct from v_projection.relation_hash
     or v_recomputed -> 'data' ->> 'contentHash'
          is distinct from v_projection.content_hash
     or v_projection.content_hash is distinct from v_binding.projection_content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection readback does not match the typed persisted rows'
    );
  end if;
  v_publicly_visible := private.portal_lcia_projection_is_public_v1(
    v_projection.id
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'projectionPublicationId', v_binding.id,
      'projectionId', v_binding.projection_id,
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'packageId', v_binding.package_id,
      'packageVersion', v_binding.package_version,
      'status', v_binding.status,
      'isCurrent', coalesce(v_publicly_visible, false),
      'isPubliclyVisible', coalesce(v_publicly_visible, false),
      'contentHash', v_binding.projection_content_hash,
      'evidenceHash', v_binding.evidence_hash,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count,
      'finalizedAt', private.portal_timestamp_v1(v_binding.finalized_at),
      'revokedAt', case when v_binding.revoked_at is null then null
        else private.portal_timestamp_v1(v_binding.revoked_at) end
    )
  );
end
$_$;

ALTER FUNCTION "api"."qry_portal_lcia_projection_publication_readback_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_portal_lcia_projection_publication_readback_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_portal_lcia_projection_publication_readback_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text") TO "authenticated";
