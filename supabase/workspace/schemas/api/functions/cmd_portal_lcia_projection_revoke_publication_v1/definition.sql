CREATE OR REPLACE FUNCTION "api"."cmd_portal_lcia_projection_revoke_publication_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text", "p_reason" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_binding private.portal_lcia_projection_publications%rowtype;
  v_reason text := nullif(btrim(coalesce(p_reason, '')), '');
  v_now timestamptz := clock_timestamp();
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
     or coalesce(p_projection_content_hash, '') !~ '^[0-9a-f]{64}$'
     or private.portal_lcia_public_text_valid_v1(v_reason, 2000) is not true
     or private.portal_lcia_safe_audit_v1(p_audit) is not true then
    return api.lcia_result_error(
      'invalid_projection_request', 400, 'Invalid projection revoke request'
    );
  end if;

  select binding.* into v_binding
  from private.portal_lcia_projection_publications as binding
  where binding.lcia_result_publication_id = p_lcia_result_publication_id
  for update;
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
  if v_binding.status = 'revoked' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', jsonb_build_object(
        'projectionPublicationId', v_binding.id,
        'lciaResultPublicationId', v_binding.lcia_result_publication_id,
        'status', v_binding.status,
        'revokedAt', private.portal_timestamp_v1(v_binding.revoked_at)
      )
    );
  end if;

  update private.portal_lcia_projection_publications
  set status = 'revoked',
      revoked_by = v_actor,
      revoked_at = v_now,
      revoke_reason = v_reason
  where id = v_binding.id
  returning * into v_binding;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_portal_lcia_projection_revoke_publication_v1',
    v_actor,
    'portal_lcia_projection_publications',
    v_binding.id,
    v_binding.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'contentHash', v_binding.projection_content_hash,
      'reason', v_reason
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'projectionPublicationId', v_binding.id,
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'status', v_binding.status,
      'revokedAt', private.portal_timestamp_v1(v_binding.revoked_at)
    )
  );
end
$_$;

ALTER FUNCTION "api"."cmd_portal_lcia_projection_revoke_publication_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_portal_lcia_projection_revoke_publication_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_portal_lcia_projection_revoke_publication_v1"("p_lcia_result_publication_id" "uuid", "p_projection_content_hash" "text", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
