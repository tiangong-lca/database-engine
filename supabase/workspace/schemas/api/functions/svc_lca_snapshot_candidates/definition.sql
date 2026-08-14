CREATE OR REPLACE FUNCTION "api"."svc_lca_snapshot_candidates"("p_scope" "text", "p_snapshot_id" "uuid" DEFAULT NULL::"uuid", "p_process_filter_contains" "jsonb" DEFAULT NULL::"jsonb", "p_limit" integer DEFAULT 100) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_scope text := lower(btrim(coalesce(p_scope, '')));
  v_limit integer := greatest(1, least(coalesce(p_limit, 100), 100));
  v_rows jsonb;
begin
  if v_scope not in ('full_library', 'data_product') then
    return jsonb_build_object('ok', false, 'code', 'INVALID_LCA_SCOPE', 'status', 400);
  end if;

  with candidates as (
    select
      snapshot.id,
      snapshot.scope,
      snapshot.process_filter,
      snapshot.source_hash,
      snapshot.created_at,
      artifact.id as artifact_id,
      artifact.artifact_url,
      artifact.artifact_sha256,
      artifact.artifact_byte_size,
      artifact.artifact_format,
      artifact.process_count,
      artifact.flow_count,
      artifact.impact_count,
      artifact.snapshot_index_sha256,
      artifact.snapshot_build_contract_hash,
      artifact.effective_scope_hash,
      artifact.data_snapshot_token,
      artifact.closure_bundle_hash,
      active.snapshot_id is not null as is_active
    from private.lca_network_snapshots as snapshot
    join lateral (
      select candidate_artifact.*
      from private.lca_snapshot_artifacts as candidate_artifact
      where candidate_artifact.snapshot_id = snapshot.id
        and candidate_artifact.status = 'ready'
      order by candidate_artifact.created_at desc, candidate_artifact.id
      limit 1
    ) as artifact on true
    left join private.lca_active_snapshots as active
      on active.scope = v_scope and active.snapshot_id = snapshot.id
    where snapshot.status = 'ready'
      and (
        (p_snapshot_id is not null and snapshot.id = p_snapshot_id)
        or (
          p_snapshot_id is null
          and snapshot.scope in (v_scope, 'full_library')
          and (
            p_process_filter_contains is null
            or coalesce(snapshot.process_filter, '{}'::jsonb) @> p_process_filter_contains
          )
        )
      )
    order by
      case when p_snapshot_id is not null then 0 when active.snapshot_id is not null then 0 else 1 end,
      snapshot.created_at desc,
      snapshot.id
    limit v_limit
  )
  select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
    'snapshotId', id,
    'scope', scope,
    'processFilter', process_filter,
    'sourceHash', source_hash,
    'createdAt', created_at,
    'isActive', is_active,
    'artifact', jsonb_build_object(
      'artifactId', artifact_id,
      'artifactUrl', artifact_url,
      'artifactSha256', artifact_sha256,
      'artifactByteSize', artifact_byte_size,
      'artifactFormat', artifact_format,
      'processCount', process_count,
      'flowCount', flow_count,
      'impactCount', impact_count,
      'snapshotIndexSha256', snapshot_index_sha256,
      'snapshotBuildContractHash', snapshot_build_contract_hash,
      'effectiveScopeHash', effective_scope_hash,
      'dataSnapshotToken', data_snapshot_token,
      'closureBundleHash', closure_bundle_hash
    )
  ))), '[]'::jsonb)
  into v_rows
  from candidates;

  return jsonb_build_object('ok', true, 'data', v_rows);
end
$$;

ALTER FUNCTION "api"."svc_lca_snapshot_candidates"("p_scope" "text", "p_snapshot_id" "uuid", "p_process_filter_contains" "jsonb", "p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_lca_snapshot_candidates"("p_scope" "text", "p_snapshot_id" "uuid", "p_process_filter_contains" "jsonb", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_lca_snapshot_candidates"("p_scope" "text", "p_snapshot_id" "uuid", "p_process_filter_contains" "jsonb", "p_limit" integer) TO "service_role";
