CREATE OR REPLACE FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select api.get_lcia_scope_closure_report_download(
    p_closure_check_id,
    'closure_report_xlsx'::text
  );
$$;

ALTER FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid", "p_artifact_role" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_check private.lcia_scope_closure_checks%rowtype;
  v_artifact private.worker_job_artifacts%rowtype;
  v_artifact_id uuid;
  v_expected_media_type text;
  v_format text;
  v_filename text;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_scope_closure_is_manager() then
    return api.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  select * into v_check
  from private.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return api.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if p_artifact_role = 'closure_report_xlsx' then
    v_artifact_id := v_check.report_artifact_id;
    v_expected_media_type :=
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    v_format := 'xlsx';
    v_filename := 'scope-closure-' || v_check.id::text || '.xlsx';
  elsif p_artifact_role = 'closure_issue_manifest' then
    v_artifact_id := v_check.complete_machine_result_artifact_id;
    v_expected_media_type :=
      'application/vnd.tiangong.scope-closure-manifest+json';
    v_format := 'json';
    v_filename :=
      'scope-closure-' || v_check.id::text || '-manifest.json';
  else
    return api.lcia_scope_closure_error(
      'closure_artifact_role_invalid',
      400,
      'Unsupported closure artifact role'
    );
  end if;
  select * into v_artifact
  from private.worker_job_artifacts
  where id = v_artifact_id;
  if v_artifact.id is not null
     and private.lcia_scope_closure_artifact_lineage_eligible(
       v_check,
       v_artifact,
       p_artifact_role
     )
     and (
       v_artifact.lifecycle_state = 'expired'
       or (
         v_artifact.lifecycle_state <> 'deleted'
         and v_artifact.expires_at <= now()
       )
     ) then
    return api.lcia_scope_closure_error(
      'closure_report_expired', 410, 'Closure report has expired'
    );
  end if;
  if v_artifact.id is null
     or not private.lcia_scope_closure_artifact_lineage_eligible(
       v_check,
       v_artifact,
       p_artifact_role
     )
     or v_check.status not in ('passed', 'blocked')
     or v_artifact.lifecycle_state <> 'ready'
     or v_artifact.expires_at is null
     or v_artifact.expires_at <= now()
     or nullif(trim(v_artifact.storage_bucket), '') is null
     or nullif(trim(v_artifact.storage_path), '') is null
     or v_artifact.content_type is distinct from v_expected_media_type
     or v_artifact.byte_size is null
     or v_artifact.byte_size < 0
     or v_artifact.checksum_sha256 is null
     or v_artifact.checksum_sha256 !~ '^[a-f0-9]{64}$' then
    return api.lcia_scope_closure_error(
      'closure_report_unavailable', 404, 'Closure report is not available'
    );
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'artifactId', v_artifact.id,
    'artifactRole', p_artifact_role,
    'artifactState', 'ready',
    'filename', v_filename,
    'format', v_format,
    'mediaType', v_expected_media_type,
    'size', v_artifact.byte_size,
    'checksumSha256', v_artifact.checksum_sha256,
    'artifactExpiresAt', v_artifact.expires_at,
    'bucket', v_artifact.storage_bucket,
    'objectPath', v_artifact.storage_path
  ));
end;
$_$;

ALTER FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid", "p_artifact_role" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid") TO "authenticated";

REVOKE ALL ON FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid", "p_artifact_role" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid", "p_artifact_role" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_lcia_scope_closure_report_download"("p_closure_check_id" "uuid", "p_artifact_role" "text") TO "authenticated";
