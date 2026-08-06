CREATE OR REPLACE FUNCTION "private"."lcia_result_prevent_ready_package_content_update"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  if old.status = 'preview_ready'
     and (
       old.build_id is distinct from new.build_id
       or old.build_worker_job_id is distinct from new.build_worker_job_id
       or old.package_version is distinct from new.package_version
       or old.coverage_mode is distinct from new.coverage_mode
       or old.input_status_filter is distinct from new.input_status_filter
       or old.eligibility_definition is distinct from new.eligibility_definition
       or old.eligibility_resolved_at is distinct from new.eligibility_resolved_at
       or old.eligible_input_count is distinct from new.eligible_input_count
       or old.included_input_count is distinct from new.included_input_count
       or old.input_manifest_hash is distinct from new.input_manifest_hash
       or old.input_manifest is distinct from new.input_manifest
       or old.snapshot_id is distinct from new.snapshot_id
       or old.result_id is distinct from new.result_id
       or old.latest_all_unit_result_id is distinct from new.latest_all_unit_result_id
       or old.result_artifact_ref is distinct from new.result_artifact_ref
       or old.query_artifact_ref is distinct from new.query_artifact_ref
       or old.artifact_manifest is distinct from new.artifact_manifest
       or old.package_result_hash is distinct from new.package_result_hash
       or old.lcia_method_set is distinct from new.lcia_method_set
       or old.available_impact_categories is distinct from new.available_impact_categories
       or old.postprocess_manifest is distinct from new.postprocess_manifest
       or old.default_impact_category is distinct from new.default_impact_category
     ) then
    raise exception 'lcia_result_package_immutable'
      using errcode = '23514';
  end if;

  new.updated_at := now();
  return new;
end;
$$;

ALTER FUNCTION "private"."lcia_result_prevent_ready_package_content_update"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_result_prevent_ready_package_content_update"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_result_prevent_ready_package_content_update"() TO "service_role";

GRANT ALL ON FUNCTION "private"."lcia_result_prevent_ready_package_content_update"() TO "api_internal_executor";
