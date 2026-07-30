CREATE OR REPLACE FUNCTION "public"."get_lcia_result_calculation_bundle"("p_package_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_package public.lcia_result_packages%rowtype;
  v_result public.lca_results%rowtype;
  v_bundle jsonb;
begin
  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select * into v_package
  from public.lcia_result_packages
  where id = p_package_id;
  if v_package.id is null then
    return public.lcia_result_error('package_not_found', 404, 'Package not found');
  end if;

  select * into v_result
  from public.lca_results
  where id = v_package.result_id;

  v_bundle := coalesce(
    v_package.artifact_manifest->'calculationBundle',
    v_result.diagnostics->'calculation_bundle'
  );
  if v_bundle is null then
    return public.lcia_result_error('calculation_bundle_not_available', 404, 'Calculation Bundle is not available for this legacy package');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'snapshotId', v_package.snapshot_id,
      'resultId', v_package.result_id,
      'calculationBundle', v_bundle,
      'availableImpactCategories', v_package.available_impact_categories
    )
  );
end;
$$;

ALTER FUNCTION "public"."get_lcia_result_calculation_bundle"("p_package_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."get_lcia_result_calculation_bundle"("p_package_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."get_lcia_result_calculation_bundle"("p_package_id" "uuid") TO "authenticated";
