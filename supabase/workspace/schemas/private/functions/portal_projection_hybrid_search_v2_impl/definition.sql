CREATE OR REPLACE FUNCTION "private"."portal_projection_hybrid_search_v2_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text", "p_cursor" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
declare
  v_items jsonb;
  v_next jsonb;
  v_count integer;
  v_dataset_count integer;
  v_groups jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  with candidates as materialized (
    select candidate.*
    from private.portal_projection_hybrid_candidates_v2(
      p_kind,p_query_terms,p_query_embedding,p_filters) as candidate
  ), eligible as materialized (
    -- Recheck the exact public key before hydration; never substitute a newer version.
    select candidate.*, projection.card, projection.modified_at,
      pg_catalog.jsonb_build_object(
        'kind','hybrid','algorithmVersion','portal-hybrid-rank-v2','score',candidate.score,
        'reasonCodes',pg_catalog.to_jsonb(pg_catalog.array_remove(array[
          case when candidate.lexical_rank is not null then 'lexical_public_projection'::text end,
          case when candidate.semantic_rank is not null then 'semantic_public_projection'::text end
        ],null)),
        'evidence',pg_catalog.jsonb_build_object(
          'lexicalRank',candidate.lexical_rank,'semanticRank',candidate.semantic_rank,
          'semanticDistance',case when candidate.semantic_distance is null then null
            else pg_catalog.trim_scale(candidate.semantic_distance::numeric)::text end
        )
      ) as match_data
    from candidates as candidate
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind and projection.id = candidate.id
        and projection.version = candidate.version
    where projection.state_code in (100,200)
      and (p_filters = '{}'::jsonb or private.portal_card_matches_filters_v2(projection.card,p_filters))
  ), representative as materialized (
    -- Rank a dataset by its BEST matching version, never the number of versions.
    -- Group before pagination, so one version-rich dataset cannot consume a page.
    select distinct on (candidate.id) candidate.* from eligible as candidate
    order by candidate.id,candidate.score desc,candidate.version desc
  ), after_cursor as materialized (
    select * from representative as candidate
    where p_cursor is null
      or candidate.score < (p_cursor ->> 'rankKey')::numeric
      or (candidate.score = (p_cursor ->> 'rankKey')::numeric and (
        candidate.id > (p_cursor ->> 'id')::uuid
        or (candidate.id = (p_cursor ->> 'id')::uuid and candidate.version < (p_cursor ->> 'version'))
      ))
  ), page as materialized (
    select candidate.*,
      pg_catalog.row_number() over(order by score desc,id,version desc) as ordinal
    from after_cursor as candidate
    order by score desc,id,version desc
    limit p_limit + 1
  )
  select
    coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'key', pg_catalog.jsonb_build_object('kind',p_kind,'id',page.id::text,'version',page.version),
      'accessLevel',page.card -> 'accessLevel',
      'capabilities',page.card -> 'capabilities',
      'names',page.card -> 'names',
      'summary',page.card -> 'summary',
      'geography',page.card -> 'geography',
      'referenceYear',page.card -> 'referenceYear',
      'modifiedAt',pg_catalog.to_char(page.modified_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'match',page.match_data
    ) order by page.ordinal) filter(where page.ordinal <= p_limit),'[]'::jsonb),
    case when max(page.ordinal) > p_limit then (
      pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'v',1,'fp',p_query_fingerprint,'rankKey',page.score::text,
        'kind',p_kind,'id',page.id::text,'version',page.version
      ) order by page.ordinal) filter(where page.ordinal = p_limit)
    ) -> 0 else null end,
    (select count(*)::integer from eligible),
    (select count(*)::integer from representative),
    coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'key',pg_catalog.jsonb_build_object('kind',p_kind,'id',page.id::text,'version',page.version),
      'matches',(
        select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'key',pg_catalog.jsonb_build_object('kind',p_kind,'id',member.id::text,'version',member.version),
          'match',member.match_data
        ) order by member.score desc,member.version desc)
        from eligible as member where member.id=page.id
      )
    ) order by page.ordinal) filter(where page.ordinal<=p_limit),'[]'::jsonb)
  into v_items,v_next,v_count,v_dataset_count,v_groups from page;

  -- The immutable context decorator accepts only the v1 internal envelope.
  -- Adapt that envelope here; the new API relabels ONLY after exact-key context/LCIA decoration.
  return pg_catalog.jsonb_build_object(
    'schemaVersion','portal.public-hybrid-candidate-page.v1',
    'kind',p_kind,'queryFingerprint',p_query_fingerprint,
    'items',v_items,'candidateCount',v_count,'datasetCount',v_dataset_count,
    'versionGroups',v_groups,'nextCursorPayload',v_next
  );
end;
$$;

ALTER FUNCTION "private"."portal_projection_hybrid_search_v2_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text", "p_cursor" "jsonb") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_hybrid_search_v2_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text", "p_cursor" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_projection_hybrid_search_v2_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text", "p_cursor" "jsonb") TO "portal_public_executor";
