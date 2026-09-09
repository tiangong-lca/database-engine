CREATE OR REPLACE FUNCTION "private"."tidas_import_guard_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare v_user uuid;
begin
  select j.requested_by into v_user
  from private.worker_jobs j
  join private.lca_package_artifacts a on a.worker_job_id = j.id
  where j.id = p_worker_job_id and j.lease_token = p_lease_token
    and j.status = 'running' and j.lease_expires_at > clock_timestamp()
    and j.job_kind = 'tidas.import_package'
    and j.payload_schema_version = 'tidas.import_package.request.v2'
    and j.payload_json ->> 'import_policy' = 'root_closure_v2'
    and j.payload_json ->> 'source_artifact_id' = p_source_artifact_id::text
    and a.id = p_source_artifact_id and a.artifact_kind = 'import_source'
    and a.status = 'ready' and a.artifact_sha256 ~ '^[0-9a-f]{64}$'
    and a.metadata ->> 'requested_by' = j.requested_by::text;
  if v_user is null then
    raise exception using errcode = '55000', message = 'TIDAS_IMPORT_LEASE_OR_SOURCE_INVALID';
  end if;
  return v_user;
end $_$;

ALTER FUNCTION "private"."tidas_import_guard_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."tidas_import_guard_v2"("p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_source_artifact_id" "uuid") FROM PUBLIC;
