CREATE OR REPLACE FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_release_run_id uuid;
  v_release jsonb;
  v_datasets jsonb;
begin
  if coalesce(p_process_version, '') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return private.lca_release_error('process_version_invalid', 400, 'Process version must use XX.XX.XXX');
  end if;

  select publication.release_run_id into v_release_run_id
  from private.lca_release_publications as publication
  where publication.is_current = true and publication.status = 'current'
  order by publication.published_at desc
  limit 1;

  if v_release_run_id is null then
    return private.lca_release_error('publication_not_found', 404, 'No current public LCA release exists');
  end if;

  v_release := api.get_lca_release_run(v_release_run_id);
  if coalesce((v_release->>'ok')::boolean, false) is not true then
    return v_release;
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'datasetType', dataset.dataset_type,
    'role', dataset.dataset_role,
    'uuid', dataset.dataset_uuid,
    'version', dataset.dataset_version,
    'sourceProcess', jsonb_build_object(
      'id', dataset.source_process_uuid,
      'version', dataset.source_process_version
    ),
    'versionSignificantHash', dataset.version_significant_hash,
    'semanticHash', dataset.semantic_hash,
    'canonicalContentHash', dataset.canonical_content_hash
  ) order by dataset.dataset_role, dataset.dataset_uuid), '[]'::jsonb)
  into v_datasets
  from private.lca_release_dataset_versions as dataset
  where dataset.release_run_id = v_release_run_id
    and dataset.source_process_uuid = p_process_uuid
    and dataset.source_process_version = p_process_version
    and dataset.dataset_role in ('unit_process', 'lifecycle_model', 'result_process');

  if jsonb_array_length(v_datasets) = 0 then
    return private.lca_release_error(
      'release_process_not_found', 404,
      'Process is not included in the current public LCA release'
    );
  end if;

  return jsonb_set(v_release, '{data,datasets}', v_datasets, true);
end;
$_$;

ALTER FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") TO "authenticated";

GRANT ALL ON FUNCTION "api"."get_current_lca_release_process"("p_process_uuid" "uuid", "p_process_version" "text") TO "service_role";
