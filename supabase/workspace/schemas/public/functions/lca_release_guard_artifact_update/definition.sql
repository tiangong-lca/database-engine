CREATE OR REPLACE FUNCTION "public"."lca_release_guard_artifact_update"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  if old.id is distinct from new.id
     or old.release_run_id is distinct from new.release_run_id
     or old.profile_id is distinct from new.profile_id
     or old.artifact_format is distinct from new.artifact_format
     or old.storage_bucket is distinct from new.storage_bucket
     or old.object_key is distinct from new.object_key
     or old.sha256 is distinct from new.sha256
     or old.byte_size is distinct from new.byte_size
     or old.media_type is distinct from new.media_type
     or old.closure_hash is distinct from new.closure_hash
     or old.verified_at is distinct from new.verified_at
     or old.created_at is distinct from new.created_at
     or (old.pinned and not new.pinned)
     or (old.published_at is not null and old.published_at is distinct from new.published_at) then
    raise exception 'lca_release_artifact_immutable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

ALTER FUNCTION "public"."lca_release_guard_artifact_update"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_release_guard_artifact_update"() FROM PUBLIC;
