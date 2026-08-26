CREATE OR REPLACE FUNCTION "private"."portal_lcia_package_publish_unchecked_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_prepared jsonb;
  v_package private.lcia_result_packages%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_existing private.lcia_result_publications%rowtype;
  v_retry_audit jsonb;
  v_previous_id uuid;
  v_publication private.lcia_result_publications%rowtype;
  v_reason text := nullif(btrim(coalesce(p_reason, '')), '');
  v_default_impact text;
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
  if p_package_id is null
     or coalesce(p_expected_publish_plan_hash, '') !~ '^[0-9a-f]{64}$'
     or private.portal_lcia_safe_audit_v1(p_audit) is not true
     or (
       v_reason is not null
       and private.portal_lcia_public_text_valid_v1(v_reason, 2000) is not true
     ) then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Invalid Portal LCIA package publication request'
    );
  end if;

  lock table private.lcia_result_publications in exclusive mode;
  select publication.* into v_existing
  from private.lcia_result_publications as publication
  where publication.package_id = p_package_id
  order by publication.published_at desc nulls last, publication.id
  limit 1;
  if v_existing.id is not null then
    select audit.payload into v_retry_audit
    from private.command_audit_log as audit
    where audit.command = 'cmd_portal_lcia_result_package_publish_v1'
      and audit.target_table = 'lcia_result_publications'
      and audit.target_id = v_existing.id
      and audit.payload ->> 'publishPlanHash'
            = p_expected_publish_plan_hash
    order by audit.created_at desc
    limit 1;
    if v_existing.is_current
       and v_existing.status = 'current'
       and v_retry_audit is not null then
      select package.* into v_package
      from private.lcia_result_packages as package
      where package.id = p_package_id;
      select projection.* into v_projection
      from private.portal_lcia_projection_headers as projection
      where projection.id::text =
        v_package.artifact_manifest ->> 'portalProjectionId';
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'publicationId', v_existing.id,
          'packageId', v_package.id,
          'previousPublicationId', v_retry_audit -> 'previousPublicationId',
          'isCurrent', true,
          'packageVersion', v_package.package_version,
          'projectionId', v_projection.id,
          'projectionContentHash', v_projection.content_hash,
          'publishPlanHash', p_expected_publish_plan_hash,
          'publishedAt', private.portal_timestamp_v1(v_existing.published_at)
        )
      );
    end if;
    return api.lcia_result_error(
      'package_publication_conflict', 409,
      'Package already has a different or non-current publication history'
    );
  end if;

  v_prepared := private.portal_lcia_v3_package_publish_prepare_v1(
    p_package_id, p_display_default_impact_category
  );
  if coalesce((v_prepared ->> 'ok')::boolean, false) is not true then
    return v_prepared;
  end if;
  if v_prepared #>> '{data,publishPlanHash}'
       <> p_expected_publish_plan_hash then
    return api.lcia_result_error(
      'publish_plan_drift', 409,
      'Portal LCIA package publication evidence changed after approval'
    );
  end if;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id
  for share;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id::text = v_prepared #>> '{data,projection,id}';
  v_default_impact := v_prepared #>> '{data,displayDefaultImpactCategory}';

  update private.lcia_result_publications
  set is_current = false,
      status = 'superseded',
      updated_at = v_now
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current
  returning id into v_previous_id;

  insert into private.lcia_result_publications (
    package_id, publication_series_key, publication_channel,
    visibility_scope, is_current, status,
    display_default_impact_category, published_by, published_at, reason
  ) values (
    v_package.id, 'global', 'public', 'public', true, 'current',
    v_default_impact, v_actor, v_now, v_reason
  ) returning * into v_publication;

  update private.lca_results
  set is_pinned = true
  where id = v_package.result_id;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_portal_lcia_result_package_publish_v1',
    v_actor,
    'lcia_result_publications',
    v_publication.id,
    v_package.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'packageId', v_package.id,
      'projectionId', v_projection.id,
      'projectionContentHash', v_projection.content_hash,
      'publishPlanHash', p_expected_publish_plan_hash,
      'previousPublicationId', v_previous_id,
      'displayDefaultImpactCategory', v_default_impact
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'publicationId', v_publication.id,
      'packageId', v_package.id,
      'previousPublicationId', v_previous_id,
      'isCurrent', true,
      'packageVersion', v_package.package_version,
      'projectionId', v_projection.id,
      'projectionContentHash', v_projection.content_hash,
      'publishPlanHash', p_expected_publish_plan_hash,
      'publishedAt', private.portal_timestamp_v1(v_publication.published_at)
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error(
      'latest_conflict', 409,
      'Another current publication already exists'
    );
end
$_$;

ALTER FUNCTION "private"."portal_lcia_package_publish_unchecked_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_package_publish_unchecked_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;
