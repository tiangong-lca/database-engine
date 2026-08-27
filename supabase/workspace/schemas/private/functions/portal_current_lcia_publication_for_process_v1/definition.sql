CREATE OR REPLACE FUNCTION "private"."portal_current_lcia_publication_for_process_v1"("p_process_id" "uuid", "p_process_version" "text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
  with visible as materialized (
    select
      binding.projection_id,
      binding.lcia_result_publication_id,
      binding.package_id,
      binding.package_version,
      binding.source_published_at
    from private.portal_lcia_projection_publications as binding
    join private.portal_lcia_projection_headers as projection
      on projection.id = binding.projection_id
    join private.portal_lcia_projection_process_axis as process_axis
      on process_axis.projection_id = binding.projection_id
     and process_axis.process_id = p_process_id
     and process_axis.process_version = p_process_version
    join private.lcia_result_publications as publication
      on publication.id = binding.lcia_result_publication_id
     and publication.package_id = binding.package_id
    join private.lcia_result_packages as package
      on package.id = binding.package_id
    join public.processes as process
      on process.id = process_axis.process_id
     and process.version::text = process_axis.process_version
    where p_process_id is not null
      and p_process_version ~ '^\d{2}\.\d{2}\.\d{3}$'
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
      and process.state_code = 100
      and jsonb_typeof(process.json) = 'object'
      and private.portal_process_open_capability_bridge_v1(
        process.state_code, process.json
      )
      and private.portal_lcia_projection_is_public_v1(binding.projection_id)
    order by binding.source_published_at desc, binding.id
    limit 1
  ), methods as materialized (
    select
      visible.projection_id,
      jsonb_agg(
        jsonb_build_object(
          'id', method.method_id::text,
          'version', method.method_version
        )
        order by method.method_id, method.method_version
      ) as lcia_methods
    from visible
    cross join lateral (
      select distinct impact.method_id, impact.method_version
      from private.portal_lcia_projection_impact_axis as impact
      where impact.projection_id = visible.projection_id
    ) as method
    group by visible.projection_id
  )
  select jsonb_build_object(
    'publicationId', visible.lcia_result_publication_id::text,
    'packageId', visible.package_id::text,
    'packageVersion', visible.package_version,
    'publishedAt', private.portal_timestamp_v1(visible.source_published_at),
    'lciaMethods', methods.lcia_methods
  )
  from visible
  join methods using (projection_id)
  where jsonb_array_length(methods.lcia_methods) > 0
$_$;

ALTER FUNCTION "private"."portal_current_lcia_publication_for_process_v1"("p_process_id" "uuid", "p_process_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_current_lcia_publication_for_process_v1"("p_process_id" "uuid", "p_process_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_current_lcia_publication_for_process_v1"("p_process_id" "uuid", "p_process_version" "text") TO "portal_public_executor";
