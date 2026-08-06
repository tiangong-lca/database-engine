CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_v2_required_roles"("p_publication_mode" "text") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case p_publication_mode
    when 'fresh' then jsonb_build_array(
      jsonb_build_object(
        'artifactRole', 'closure_report',
        'artifactType', 'closure_report_xlsx',
        'mediaType',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'exactCount', 1
      ),
      jsonb_build_object(
        'artifactRole', 'complete_machine_result',
        'artifactType', 'closure_complete_machine_result',
        'mediaType',
          'application/vnd.tiangong.scope-closure-manifest+json',
        'exactCount', 1
      ),
      jsonb_build_object(
        'artifactRole', 'closure_bundle',
        'artifactType', 'closure_bundle',
        'mediaType', 'application/json',
        'exactCount', 1
      )
    )
    when 'reused' then jsonb_build_array(
      jsonb_build_object(
        'artifactRole', 'closure_report',
        'artifactType', 'closure_report_xlsx',
        'mediaType',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'exactCount', 1
      )
    )
    else null
  end
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_v2_required_roles"("p_publication_mode" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_required_roles"("p_publication_mode" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_required_roles"("p_publication_mode" "text") TO "api_internal_executor";
