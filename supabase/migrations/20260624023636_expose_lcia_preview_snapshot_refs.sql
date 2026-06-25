create or replace function public.get_lcia_result_package_preview(
  p_package_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_package public.lcia_result_packages%rowtype;
begin
  if not public.lcia_result_is_manager() then
    return public.lcia_result_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;

  select *
    into v_package
  from public.lcia_result_packages
  where id = p_package_id;

  if v_package.id is null then
    return public.lcia_result_error('package_not_found', 404, 'Package not found');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'summary', jsonb_build_object(
        'packageId', v_package.id,
        'packageVersion', v_package.package_version,
        'status', v_package.status,
        'coverageMode', v_package.coverage_mode,
        'snapshotId', v_package.snapshot_id,
        'resultId', v_package.result_id,
        'latestAllUnitResultId', v_package.latest_all_unit_result_id,
        'eligibleInputCount', v_package.eligible_input_count,
        'includedInputCount', v_package.included_input_count,
        'inputManifestHash', v_package.input_manifest_hash,
        'defaultImpactCategory', v_package.default_impact_category,
        'availableImpactCategories', v_package.available_impact_categories
      ),
      'resultArtifact', v_package.result_artifact_ref,
      'queryArtifact', v_package.query_artifact_ref,
      'artifactManifest', v_package.artifact_manifest,
      'inputManifest', v_package.input_manifest
    )
  );
end;
$$;

revoke all on function public.get_lcia_result_package_preview(uuid) from public;
grant execute on function public.get_lcia_result_package_preview(uuid) to authenticated;
