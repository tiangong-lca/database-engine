CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_header_guard_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
begin
  if tg_op = 'DELETE' then
    raise exception 'portal_lcia_projection_header_immutable'
      using errcode = '55000';
  end if;
  if old.status <> 'staging'
     or new.id is distinct from old.id
     or new.build_worker_job_id is distinct from old.build_worker_job_id
     or new.stage_lease_token is distinct from old.stage_lease_token
     or new.projection_contract_version is distinct from old.projection_contract_version
     or new.process_count is distinct from old.process_count
     or new.impact_count is distinct from old.impact_count
     or new.input_manifest_hash is distinct from old.input_manifest_hash
     or new.closure_certificate_hash is distinct from old.closure_certificate_hash
     or new.snapshot_hash is distinct from old.snapshot_hash
     or new.closure_bundle_hash is distinct from old.closure_bundle_hash
     or new.snapshot_index_sha256 is distinct from old.snapshot_index_sha256
     or new.snapshot_build_contract_hash is distinct from old.snapshot_build_contract_hash
     or new.bundle_content_hash is distinct from old.bundle_content_hash
     or new.bundle_manifest_sha256 is distinct from old.bundle_manifest_sha256
     or new.lcia_chunk_set_sha256 is distinct from old.lcia_chunk_set_sha256
     or new.result_artifact_sha256 is distinct from old.result_artifact_sha256
     or new.query_artifact_sha256 is distinct from old.query_artifact_sha256
     or new.created_at is distinct from old.created_at
     or new.status not in ('prepared', 'failed') then
    raise exception 'portal_lcia_projection_header_immutable'
      using errcode = '55000';
  end if;
  return new;
end
$$;

ALTER FUNCTION "private"."portal_lcia_projection_header_guard_v1"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_header_guard_v1"() FROM PUBLIC;
