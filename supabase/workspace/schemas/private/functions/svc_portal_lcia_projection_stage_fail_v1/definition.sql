CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_stage_fail_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_code" "text", "p_message" "text", "p_audit" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_code text := btrim(coalesce(p_code, ''));
  v_message text := nullif(btrim(coalesce(p_message, '')), '');
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  if length(v_code) not between 1 and 128
     or v_code !~ '^[a-z0-9_]+$'
     or (
       v_message is not null
       and private.portal_lcia_public_text_valid_v1(v_message, 2000)
             is not true
     )
     or private.portal_lcia_safe_audit_v1(p_audit) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_request', 'status', 400
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for update;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id
  for share;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.status = 'failed'
     and v_projection.failure_code = v_code
     and v_projection.failure_message is not distinct from v_message then
    return jsonb_build_object(
      'ok', true, 'idempotentReplay', true,
      'data', jsonb_build_object(
        'projectionId', v_projection.id, 'status', v_projection.status
      )
    );
  end if;
  if v_projection.status <> 'staging' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_staging', 'status', 409
    );
  end if;

  update private.portal_lcia_projection_headers
  set status = 'failed',
      failure_code = v_code,
      failure_message = v_message,
      failed_at = clock_timestamp()
  where id = v_projection.id
  returning * into v_projection;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'svc_portal_lcia_projection_stage_fail_v1',
    v_job.requested_by,
    'portal_lcia_projection_headers',
    v_projection.id,
    v_projection.projection_contract_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'failureCode', v_code,
      'failureMessage', v_message
    )
  );

  return jsonb_build_object(
    'ok', true, 'idempotentReplay', false,
    'data', jsonb_build_object(
      'projectionId', v_projection.id, 'status', v_projection.status
    )
  );
end
$_$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_stage_fail_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_code" "text", "p_message" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_fail_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_code" "text", "p_message" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_fail_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_code" "text", "p_message" "text", "p_audit" "jsonb") TO "service_role";
