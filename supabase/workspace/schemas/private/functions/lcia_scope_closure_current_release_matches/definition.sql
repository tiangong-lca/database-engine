CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") RETURNS boolean
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select exists (
    select 1 from private.lcia_scope_closure_data_snapshots s
    join private.lca_release_publications p on p.is_current=true and p.status='current'
    join private.lca_release_runs r on r.id=p.release_run_id
    where s.data_snapshot_token=p_snapshot_token
      and s.root_manifest->'currentPublicRelease'->>'releaseRunId'=r.id::text
      and s.root_manifest->'currentPublicRelease'->>'releaseManifestHash'=r.release_manifest_hash
  );
$$;

ALTER FUNCTION "private"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") TO "api_internal_executor";
