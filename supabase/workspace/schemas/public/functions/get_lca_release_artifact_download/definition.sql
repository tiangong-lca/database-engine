CREATE OR REPLACE FUNCTION "public"."get_lca_release_artifact_download"("p_artifact_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_artifact public.lca_release_artifacts%rowtype;
  v_is_public boolean;
begin
  select * into v_artifact
  from public.lca_release_artifacts
  where id = p_artifact_id;

  if v_artifact.id is null then
    return public.lca_release_error('artifact_not_found', 404, 'Release artifact not found');
  end if;
  select exists (
    select 1 from public.lca_release_publications
    where release_run_id = v_artifact.release_run_id
      and status in ('current', 'superseded')
  ) into v_is_public;

  if not v_is_public and not public.lca_release_is_manager() then
    return public.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required for private artifacts');
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'artifactId', v_artifact.id,
      'releaseRunId', v_artifact.release_run_id,
      'profileId', v_artifact.profile_id,
      'format', v_artifact.artifact_format,
      'storageBucket', v_artifact.storage_bucket,
      'objectKey', v_artifact.object_key,
      'sha256', v_artifact.sha256,
      'byteSize', v_artifact.byte_size,
      'mediaType', v_artifact.media_type,
      'public', v_is_public
    )
  );
end;
$$;

ALTER FUNCTION "public"."get_lca_release_artifact_download"("p_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."get_lca_release_artifact_download"("p_artifact_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."get_lca_release_artifact_download"("p_artifact_id" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."get_lca_release_artifact_download"("p_artifact_id" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_lca_release_artifact_download"("p_artifact_id" "uuid") TO "service_role";
