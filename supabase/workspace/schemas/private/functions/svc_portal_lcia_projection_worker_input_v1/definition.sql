CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_worker_input_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id;
  if v_job.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_job_not_found', 'status', 404
    );
  end if;
  if private.portal_lcia_projection_v3_job_binding_valid_v1(
    p_build_worker_job_id, p_lease_token
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'buildWorkerJobId', v_job.id,
      'buildId', v_job.subject_id,
      'payloadSchemaVersion', v_job.payload_schema_version,
      'projectionContractVersion', 'portal.lcia-projection.v1',
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1',
      'payload', v_job.payload_json,
      'payloadRef', v_job.payload_ref
    )
  );
end
$$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_worker_input_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_worker_input_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_worker_input_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") TO "service_role";
