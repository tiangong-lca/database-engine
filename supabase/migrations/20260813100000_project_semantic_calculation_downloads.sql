-- Issue #487: project Worker-produced semantic calculation files as durable,
-- role-bound product downloads without turning canonical bundle shards into UI products.

create or replace function api.get_lcia_result_calculation_bundle(
  p_package_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
as $$
declare
  v_package private.lcia_result_packages%rowtype;
  v_result private.lca_results%rowtype;
  v_bundle jsonb;
  v_downloads jsonb;
  v_download jsonb;
  v_expected jsonb := jsonb_build_object(
    'lcia_results_xlsx', jsonb_build_object(
      'group', 'results',
      'fileName', 'lcia-results.xlsx',
      'mediaType', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ),
    'lcia_results_csv_zip', jsonb_build_object(
      'group', 'results',
      'fileName', 'lcia-results.csv.zip',
      'mediaType', 'application/zip'
    ),
    'lci_inventory_parquet', jsonb_build_object(
      'group', 'advanced_data',
      'fileName', 'lci-inventory.parquet',
      'mediaType', 'application/vnd.apache.parquet'
    ),
    'lci_inventory_csv_zip', jsonb_build_object(
      'group', 'advanced_data',
      'fileName', 'lci-inventory-csv.zip',
      'mediaType', 'application/zip'
    ),
    'calculation_evidence_bundle', jsonb_build_object(
      'group', 'audit_evidence',
      'fileName', 'calculation-evidence-bundle.zip',
      'mediaType', 'application/zip'
    )
  );
begin
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403, 'Data product manager role is required'
    );
  end if;

  select * into v_package
  from private.lcia_result_packages
  where id = p_package_id;
  if v_package.id is null then
    return api.lcia_result_error('package_not_found', 404, 'Package not found');
  end if;

  select * into v_result
  from private.lca_results
  where id = v_package.result_id;

  v_bundle := coalesce(
    v_package.artifact_manifest->'calculationBundle',
    v_result.diagnostics->'calculation_bundle'
  );
  if v_bundle is null then
    return api.lcia_result_error(
      'calculation_bundle_not_available', 404,
      'Calculation Bundle is not available for this legacy package'
    );
  end if;

  v_downloads := coalesce(v_bundle->'downloads', '[]'::jsonb);
  if jsonb_typeof(v_downloads) <> 'array' then
    return api.lcia_result_error(
      'calculation_downloads_invalid', 409,
      'Calculation product downloads must be a role-bound array'
    );
  end if;
  if jsonb_array_length(v_downloads) not in (0, 5) then
    return api.lcia_result_error(
      'calculation_downloads_incomplete', 409,
      'Calculation product downloads must contain all five semantic artifacts'
    );
  end if;

  for v_download in select value from jsonb_array_elements(v_downloads)
  loop
    if jsonb_typeof(v_download) <> 'object'
       or not v_expected ? coalesce(v_download->>'role', '')
       or v_download->>'group' is distinct from v_expected->(v_download->>'role')->>'group'
       or v_download->>'fileName' is distinct from v_expected->(v_download->>'role')->>'fileName'
       or v_download->>'mediaType' is distinct from v_expected->(v_download->>'role')->>'mediaType'
       or v_download->>'schemaVersion' is distinct from 'tiangong.calculation-download.v1'
       or coalesce(v_download->>'artifactUrl', '') = ''
       or coalesce(v_download->>'sha256', '') !~ '^[0-9a-f]{64}$'
       or coalesce(v_download->>'byteSize', '') !~ '^[1-9][0-9]*$'
       or coalesce(v_download->>'recordCount', '') !~ '^[0-9]+$' then
      return api.lcia_result_error(
        'calculation_download_ref_invalid', 409,
        'Calculation product download metadata is incomplete or invalid'
      );
    end if;
  end loop;

  if jsonb_array_length(v_downloads) = 5 and (
    select count(distinct item->>'role')
    from jsonb_array_elements(v_downloads) as item
  ) <> 5 then
    return api.lcia_result_error(
      'calculation_download_role_conflict', 409,
      'Calculation product download roles must be unique'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'snapshotId', v_package.snapshot_id,
      'resultId', v_package.result_id,
      'calculationBundle', v_bundle - 'downloads',
      'productDownloads', v_downloads,
      'availableImpactCategories', v_package.available_impact_categories
    )
  );
end;
$$;

alter function api.get_lcia_result_calculation_bundle(uuid) owner to postgres;
revoke all on function api.get_lcia_result_calculation_bundle(uuid)
  from public, anon, authenticated, service_role;
grant execute on function api.get_lcia_result_calculation_bundle(uuid)
  to api_internal_executor, authenticated;

comment on function api.get_lcia_result_calculation_bundle(uuid) is
  'Projects a verified Calculation Bundle plus five role-bound semantic downloads; canonical shards remain internal preview and audit evidence.';
