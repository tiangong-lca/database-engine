CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_certificate_event"("p_closure_check_id" "uuid", "p_certificate_status" "text", "p_reason" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_check private.lcia_scope_closure_checks%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if p_certificate_status not in ('stale', 'revoked')
     or coalesce(nullif(trim(p_reason), ''), '') = '' then
    return api.lcia_scope_closure_error('invalid_certificate_event', 400, 'Certificate event status and reason are required');
  end if;

  select *
    into v_check
  from private.lcia_scope_closure_checks
  where id = p_closure_check_id
  for update;

  if v_check.id is null then
    return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if not (
    (v_check.certificate_status = 'valid' and p_certificate_status in ('stale', 'revoked'))
    or (v_check.certificate_status = 'stale' and p_certificate_status = 'revoked')
  ) then
    return api.lcia_scope_closure_error('invalid_certificate_transition', 409, 'Certificate validity transition is not allowed');
  end if;

  insert into private.lcia_scope_closure_certificate_events (
    closure_check_id,
    certificate_status,
    reason
  ) values (
    v_check.id,
    p_certificate_status,
    trim(p_reason)
  );

  update private.lcia_scope_closure_checks
  set certificate_status = p_certificate_status,
      updated_at = clock_timestamp()
  where id = v_check.id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'closureCheckId', v_check.id,
      'certificateStatus', p_certificate_status
    )
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_certificate_event"("p_closure_check_id" "uuid", "p_certificate_status" "text", "p_reason" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_certificate_event"("p_closure_check_id" "uuid", "p_certificate_status" "text", "p_reason" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_certificate_event"("p_closure_check_id" "uuid", "p_certificate_status" "text", "p_reason" "text") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_certificate_event"("p_closure_check_id" "uuid", "p_certificate_status" "text", "p_reason" "text") TO "api_internal_executor";
