CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_stage_status_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_process_count bigint;
  v_impact_count bigint;
  v_value_count bigint;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;

  select count(*) into v_process_count
  from private.portal_lcia_projection_process_axis
  where projection_id = v_projection.id;
  select count(*) into v_impact_count
  from private.portal_lcia_projection_impact_axis
  where projection_id = v_projection.id;
  select count(*) into v_value_count
  from private.portal_lcia_projection_values
  where projection_id = v_projection.id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(jsonb_build_object(
      'projectionId', v_projection.id,
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'status', v_projection.status,
      'processCount', v_process_count,
      'expectedProcessCount', v_projection.process_count,
      'impactCount', v_impact_count,
      'expectedImpactCount', v_projection.impact_count,
      'valueCount', v_value_count,
      'expectedValueCount', v_projection.expected_value_count,
      'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1',
      'processAxisHash', v_projection.process_axis_hash,
      'impactAxisHash', v_projection.impact_axis_hash,
      'valueGridHash', v_projection.value_grid_hash,
      'relationHash', v_projection.relation_hash,
      'contentHash', v_projection.content_hash,
      'failureCode', v_projection.failure_code
    ))
  );
end
$$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_stage_status_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_status_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_status_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") TO "service_role";
