CREATE OR REPLACE FUNCTION "api"."svc_data_product_current_public_package"() RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_publication private.lcia_result_publications%rowtype;
  v_package private.lcia_result_packages%rowtype;
begin
  select publication.*
  into v_publication
  from private.lcia_result_publications as publication
  where publication.publication_series_key = 'global'
    and publication.publication_channel = 'public'
    and publication.visibility_scope = 'public'
    and publication.is_current = true
    and publication.status = 'current'
  order by publication.published_at desc nulls last, publication.created_at desc,
    publication.id
  limit 1;

  if v_publication.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select package.*
  into v_package
  from private.lcia_result_packages as package
  where package.id = v_publication.package_id
    and package.status = 'preview_ready';

  if v_package.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publication', jsonb_strip_nulls(jsonb_build_object(
        'id', v_publication.id,
        'package_id', v_publication.package_id,
        'publication_series_key', v_publication.publication_series_key,
        'publication_channel', v_publication.publication_channel,
        'visibility_scope', v_publication.visibility_scope,
        'is_current', v_publication.is_current,
        'status', v_publication.status,
        'display_default_impact_category', v_publication.display_default_impact_category,
        'published_at', v_publication.published_at,
        'created_at', v_publication.created_at
      )),
      'package', jsonb_strip_nulls(jsonb_build_object(
        'id', v_package.id,
        'package_version', v_package.package_version,
        'eligible_input_count', v_package.eligible_input_count,
        'included_input_count', v_package.included_input_count,
        'input_manifest', v_package.input_manifest,
        'snapshot_id', v_package.snapshot_id,
        'result_id', v_package.result_id,
        'result_artifact_ref', v_package.result_artifact_ref,
        'query_artifact_ref', v_package.query_artifact_ref,
        'artifact_manifest', v_package.artifact_manifest,
        'available_impact_categories', v_package.available_impact_categories,
        'default_impact_category', v_package.default_impact_category,
        'status', v_package.status
      ))
    )
  );
end
$$;

ALTER FUNCTION "api"."svc_data_product_current_public_package"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_data_product_current_public_package"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_data_product_current_public_package"() TO "service_role";
