CREATE OR REPLACE FUNCTION "api"."cmd_portal_lcia_result_package_publish_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_package private.lcia_result_packages%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
begin
  if auth.uid() is null or not api.lcia_result_is_manager() then
    return private.portal_lcia_package_publish_unchecked_v1(
      p_package_id, p_display_default_impact_category,
      p_expected_publish_plan_hash, p_reason, p_audit
    );
  end if;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id::text =
    v_package.artifact_manifest ->> 'portalProjectionId'
    and projection.build_worker_job_id = v_package.build_worker_job_id;
  if v_package.id is not null and v_projection.id is not null then
    perform 1
    from private.worker_jobs as job
    where job.id = v_package.build_worker_job_id
    for share;
    perform 1
    from private.lca_results as result
    where result.id = v_package.result_id
    for share;
    if v_package.latest_all_unit_result_id is not null then
      perform 1
      from private.lca_latest_all_unit_results as latest
      where latest.id = v_package.latest_all_unit_result_id
      for share;
    end if;
    if private.portal_lcia_projection_package_binding_valid_v1(
         v_package.id, v_package.build_worker_job_id, v_projection.id
       ) is not true then
      return api.lcia_result_error(
        'projection_package_binding_invalid', 409,
        'Portal LCIA package binding is no longer authoritative'
      );
    end if;
  end if;
  return private.portal_lcia_package_publish_unchecked_v1(
    p_package_id, p_display_default_impact_category,
    p_expected_publish_plan_hash, p_reason, p_audit
  );
end
$$;

ALTER FUNCTION "api"."cmd_portal_lcia_result_package_publish_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_portal_lcia_result_package_publish_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_portal_lcia_result_package_publish_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text", "p_expected_publish_plan_hash" "text", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
