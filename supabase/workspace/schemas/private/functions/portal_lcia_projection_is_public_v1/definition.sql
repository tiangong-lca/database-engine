CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_is_public_v1"("p_projection_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select exists (
    select 1
    from private.portal_lcia_projection_publications as binding
    join private.portal_lcia_projection_headers as projection
      on projection.id = binding.projection_id
    join private.lcia_result_publications as publication
      on publication.id = binding.lcia_result_publication_id
    join private.lcia_result_packages as package
      on package.id = binding.package_id
     and package.id = publication.package_id
    join private.worker_jobs as job
      on job.id = projection.build_worker_job_id
     and job.id = package.build_worker_job_id
    where binding.projection_id = p_projection_id
      and binding.status = 'finalized'
      and binding.revoked_at is null
      and projection.status = 'prepared'
      and projection.content_hash = binding.projection_content_hash
      and publication.is_current
      and publication.status = 'current'
      and publication.publication_series_key = 'global'
      and publication.publication_channel = 'public'
      and publication.visibility_scope = 'public'
      and publication.published_at = binding.source_published_at
      and package.status = 'preview_ready'
      and package.package_version = binding.package_version
      and package.package_result_hash = binding.package_result_hash
      and package.artifact_manifest ->> 'portalProjectionId'
            = projection.id::text
      and package.artifact_manifest ->> 'portalProjectionContentHash'
            = projection.content_hash
      and job.job_kind = 'lcia_result.package_build'
      and job.payload_schema_version = 'lcia_result.package_build.request.v3'
      and job.payload_json ->> 'portalProjectionContractVersion'
            = 'portal.lcia-projection.v1'
  )
$$;

ALTER FUNCTION "private"."portal_lcia_projection_is_public_v1"("p_projection_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_is_public_v1"("p_projection_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_lcia_projection_is_public_v1"("p_projection_id" "uuid") TO "portal_public_executor";
