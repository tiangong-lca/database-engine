CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_package_ready_readback_v1"("p_build_worker_job_id" "uuid", "p_current_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_package private.lcia_result_packages%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_projection_id uuid;
  v_projection_content_hash text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  if private.portal_lcia_projection_v3_job_binding_valid_v1(
       p_build_worker_job_id, p_current_lease_token
     ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;

  select package.* into v_package
  from private.lcia_result_packages as package
  where package.build_worker_job_id = p_build_worker_job_id;
  if v_package.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_not_found', 'status', 404
    );
  end if;

  begin
    v_projection_id := nullif(
      v_package.artifact_manifest ->> 'portalProjectionId', ''
    )::uuid;
  exception when invalid_text_representation then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end;
  v_projection_content_hash := nullif(
    v_package.artifact_manifest ->> 'portalProjectionContentHash', ''
  );

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_projection_id
    and projection.build_worker_job_id = p_build_worker_job_id
    and projection.status = 'prepared'
    and projection.content_hash = v_projection_content_hash;
  if v_projection.id is null
     or private.portal_lcia_projection_package_binding_valid_v1(
       v_package.id, p_build_worker_job_id, v_projection.id
     ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'reused', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'status', v_package.status,
      'buildWorkerJobId', v_package.build_worker_job_id,
      'includedInputCount', v_package.included_input_count,
      'projection', jsonb_build_object(
        'projectionId', v_projection.id,
        'contentHash', v_projection.content_hash,
        'hashContractVersion',
          'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    )
  );
end
$$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_package_ready_readback_v1"("p_build_worker_job_id" "uuid", "p_current_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_package_ready_readback_v1"("p_build_worker_job_id" "uuid", "p_current_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_package_ready_readback_v1"("p_build_worker_job_id" "uuid", "p_current_lease_token" "uuid") TO "service_role";
