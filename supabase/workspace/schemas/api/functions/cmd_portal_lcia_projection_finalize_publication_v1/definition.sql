CREATE OR REPLACE FUNCTION "api"."cmd_portal_lcia_projection_finalize_publication_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_publication private.lcia_result_publications%rowtype;
  v_package private.lcia_result_packages%rowtype;
begin
  if auth.uid() is null or not api.lcia_result_is_manager() then
    return private.portal_lcia_projection_finalize_unchecked_v1(
      p_projection_id, p_lcia_result_publication_id, p_package_version,
      p_package_result_hash, p_projection_content_hash, p_idempotency_key,
      p_audit
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id;
  select publication.* into v_publication
  from private.lcia_result_publications as publication
  where publication.id = p_lcia_result_publication_id;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = v_publication.package_id;
  if v_projection.id is not null
     and v_publication.id is not null
     and v_package.id is not null
     and v_package.build_worker_job_id = v_projection.build_worker_job_id
     and v_package.package_version = p_package_version
     and v_package.package_result_hash = p_package_result_hash
     and v_projection.content_hash = p_projection_content_hash
     and v_package.artifact_manifest ->> 'portalProjectionId'
           = v_projection.id::text then
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
  return private.portal_lcia_projection_finalize_unchecked_v1(
    p_projection_id, p_lcia_result_publication_id, p_package_version,
    p_package_result_hash, p_projection_content_hash, p_idempotency_key,
    p_audit
  );
end
$$;

ALTER FUNCTION "api"."cmd_portal_lcia_projection_finalize_publication_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_portal_lcia_projection_finalize_publication_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_portal_lcia_projection_finalize_publication_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") TO "authenticated";
