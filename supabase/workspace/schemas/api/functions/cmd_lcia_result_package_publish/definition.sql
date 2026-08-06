CREATE OR REPLACE FUNCTION "api"."cmd_lcia_result_package_publish"("p_package_id" "uuid", "p_display_default_impact_category" "text" DEFAULT NULL::"text", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_package private.lcia_result_packages%rowtype;
  v_current_manifest jsonb;
  v_default_impact text;
  v_previous_id uuid;
  v_publication private.lcia_result_publications%rowtype;
begin
  if v_actor is null then
    return api.lcia_result_error('auth_required', 401, 'Authentication required');
  end if;

  if not api.lcia_result_is_manager() then
    return api.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select *
    into v_package
  from private.lcia_result_packages
  where id = p_package_id
  for update;

  if v_package.id is null or v_package.status <> 'preview_ready' then
    return api.lcia_result_error('package_not_ready', 400, 'Package must be preview_ready before publish');
  end if;

  if v_package.coverage_mode <> 'global_eligible'
     or v_package.included_input_count <> v_package.eligible_input_count then
    return api.lcia_result_error('package_not_global_eligible', 400, 'Only full global eligible packages can publish as global latest');
  end if;

  v_current_manifest := api.lcia_result_current_eligible_manifest();

  if v_package.eligible_input_count <> (v_current_manifest->>'eligibleInputCount')::integer
     or v_package.input_manifest_hash <> v_current_manifest->>'inputManifestHash' then
    return api.lcia_result_error('package_stale_eligibility', 409, 'Eligible process set changed after package creation');
  end if;

  v_default_impact := coalesce(
    nullif(trim(p_display_default_impact_category), ''),
    v_package.default_impact_category
  );

  if v_default_impact is null then
    return api.lcia_result_error('default_impact_missing', 400, 'Default impact category is required before publication');
  end if;

  if jsonb_array_length(v_package.available_impact_categories) > 0
     and not exists (
       select 1
       from jsonb_array_elements_text(v_package.available_impact_categories) as impact(value)
       where impact.value = v_default_impact
     ) then
    return api.lcia_result_error('default_impact_missing', 400, 'Default impact category is not present in the package impact category list');
  end if;

  if v_package.result_artifact_ref = '{}'::jsonb then
    return api.lcia_result_error('result_artifact_missing', 400, 'Package result artifact is required before publication');
  end if;

  lock table private.lcia_result_publications in exclusive mode;

  update private.lcia_result_publications
    set is_current = false,
        status = 'superseded',
        updated_at = now()
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
  returning id
    into v_previous_id;

  insert into private.lcia_result_publications (
    package_id,
    publication_series_key,
    publication_channel,
    visibility_scope,
    is_current,
    status,
    display_default_impact_category,
    published_by,
    published_at,
    reason
  )
  values (
    v_package.id,
    'global',
    'public',
    'public',
    true,
    'current',
    v_default_impact,
    v_actor,
    now(),
    nullif(trim(coalesce(p_reason, '')), '')
  )
  returning *
    into v_publication;

  update private.lca_results
    set is_pinned = true
  where id = v_package.result_id;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_lcia_result_package_publish',
    v_actor,
    'lcia_result_publications',
    v_publication.id,
    v_package.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'packageId', v_package.id,
      'previousPublicationId', v_previous_id,
      'displayDefaultImpactCategory', v_default_impact
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'packageId', v_package.id,
      'previousPublicationId', v_previous_id,
      'isCurrent', v_publication.is_current
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error('latest_conflict', 409, 'Another current publication already exists');
end;
$$;

ALTER FUNCTION "api"."cmd_lcia_result_package_publish"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lcia_result_package_publish"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_package_publish"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_reason" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_package_publish"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
