CREATE OR REPLACE FUNCTION "public"."get_published_lcia_result_package"("p_process_id" "uuid", "p_process_version" "text", "p_impact_category_id" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_publication public.lcia_result_publications%rowtype;
  v_package public.lcia_result_packages%rowtype;
  v_process_in_package boolean := false;
begin
  select *
    into v_publication
  from public.lcia_result_publications
  where publication_series_key = 'global'
    and publication_channel = 'public'
    and visibility_scope = 'public'
    and is_current = true
    and status = 'current'
  order by published_at desc nulls last, created_at desc
  limit 1;

  if v_publication.id is null then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', null,
        'package', null,
        'resultArtifact', null,
        'queryArtifact', null,
        'rowCount', 0
      )
    );
  end if;

  select *
    into v_package
  from public.lcia_result_packages
  where id = v_publication.package_id
    and status = 'preview_ready';

  if v_package.id is null then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', null,
        'package', null,
        'resultArtifact', null,
        'queryArtifact', null,
        'rowCount', 0
      )
    );
  end if;

  select exists (
    select 1
    from jsonb_array_elements(coalesce(v_package.input_manifest->'processes', '[]'::jsonb)) as process(value)
    where process.value->>'id' = p_process_id::text
      and process.value->>'version' = p_process_version
  )
    into v_process_in_package;

  if not v_process_in_package then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'publication', jsonb_build_object(
          'publicationId', v_publication.id,
          'publicationSeriesKey', v_publication.publication_series_key,
          'publicationChannel', v_publication.publication_channel,
          'visibilityScope', v_publication.visibility_scope,
          'displayDefaultImpactCategory', v_publication.display_default_impact_category,
          'publishedAt', v_publication.published_at
        ),
        'package', null,
        'resultArtifact', null,
        'queryArtifact', null,
        'rowCount', 0
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publication', jsonb_build_object(
        'publicationId', v_publication.id,
        'publicationSeriesKey', v_publication.publication_series_key,
        'publicationChannel', v_publication.publication_channel,
        'visibilityScope', v_publication.visibility_scope,
        'displayDefaultImpactCategory', v_publication.display_default_impact_category,
        'publishedAt', v_publication.published_at
      ),
      'package', jsonb_build_object(
        'packageId', v_package.id,
        'packageVersion', v_package.package_version,
        'defaultImpactCategory', v_package.default_impact_category,
        'availableImpactCategories', v_package.available_impact_categories
      ),
      'process', jsonb_build_object(
        'processId', p_process_id,
        'processVersion', p_process_version,
        'impactCategoryId', p_impact_category_id
      ),
      'resultArtifact', v_package.result_artifact_ref,
      'queryArtifact', v_package.query_artifact_ref,
      'artifactManifest', v_package.artifact_manifest,
      'rowCount', 1
    )
  );
end;
$$;

ALTER FUNCTION "public"."get_published_lcia_result_package"("p_process_id" "uuid", "p_process_version" "text", "p_impact_category_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."get_published_lcia_result_package"("p_process_id" "uuid", "p_process_version" "text", "p_impact_category_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."get_published_lcia_result_package"("p_process_id" "uuid", "p_process_version" "text", "p_impact_category_id" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."get_published_lcia_result_package"("p_process_id" "uuid", "p_process_version" "text", "p_impact_category_id" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_published_lcia_result_package"("p_process_id" "uuid", "p_process_version" "text", "p_impact_category_id" "text") TO "service_role";
