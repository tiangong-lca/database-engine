CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") RETURNS boolean
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1 from public.lcia_scope_closure_data_snapshots s
    join public.lca_release_publications p on p.is_current=true and p.status='current'
    join public.lca_release_runs r on r.id=p.release_run_id
    where s.data_snapshot_token=p_snapshot_token
      and s.root_manifest->'currentPublicRelease'->>'releaseRunId'=r.id::text
      and s.root_manifest->'currentPublicRelease'->>'releaseManifestHash'=r.release_manifest_hash
  );
$$;

ALTER FUNCTION "public"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_current_release_matches"("p_snapshot_token" "text") FROM PUBLIC;
