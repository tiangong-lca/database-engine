CREATE OR REPLACE FUNCTION "api"."cmd_lcia_result_publication_unpublish"("p_publication_id" "uuid", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_publication private.lcia_result_publications%rowtype;
begin
  if v_actor is null then
    return api.lcia_result_error('auth_required', 401, 'Authentication required');
  end if;

  if not api.lcia_result_is_manager() then
    return api.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  update private.lcia_result_publications
    set is_current = false,
        status = 'unpublished',
        unpublished_by = v_actor,
        unpublished_at = now(),
        reason = coalesce(nullif(trim(p_reason), ''), reason),
        updated_at = now()
  where id = p_publication_id
  returning *
    into v_publication;

  if v_publication.id is null then
    return api.lcia_result_error('publication_not_found', 404, 'Publication not found');
  end if;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_lcia_result_publication_unpublish',
    v_actor,
    'lcia_result_publications',
    v_publication.id,
    null,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('reason', p_reason)
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'packageId', v_publication.package_id,
      'status', v_publication.status
    )
  );
end;
$$;

ALTER FUNCTION "api"."cmd_lcia_result_publication_unpublish"("p_publication_id" "uuid", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lcia_result_publication_unpublish"("p_publication_id" "uuid", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_publication_unpublish"("p_publication_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_publication_unpublish"("p_publication_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
