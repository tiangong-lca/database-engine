CREATE OR REPLACE FUNCTION "private"."portal_projection_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "row_security" TO 'on'
    AS $$
declare
  v_items jsonb;
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();

  with portal_lexical_matches as materialized (
    select match.id,
      match.version,
      match.term_ordinal
    from private.catalog_portal_hybrid_pattern_matches_v1(
      p_kind,
      p_query_terms
    ) as match
  ), portal_latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = p_kind
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), portal_lexical_counts as materialized (
    select portal_lexical_matches.id,
      portal_lexical_matches.version,
      pg_catalog.count(distinct portal_lexical_matches.term_ordinal)::integer
        as lexical_hit_count
    from portal_lexical_matches
    join portal_latest_keys
      on portal_latest_keys.id = portal_lexical_matches.id
     and portal_latest_keys.version = portal_lexical_matches.version
    group by portal_lexical_matches.id,
      portal_lexical_matches.version
  ), portal_lexical_candidates as materialized (
    select portal_lexical_counts.*
    from portal_lexical_counts
    where portal_lexical_counts.lexical_hit_count > 0
    order by portal_lexical_counts.lexical_hit_count desc,
      portal_lexical_counts.id asc,
      portal_lexical_counts.version desc
    limit 200
  ), portal_lexical_ranked as materialized (
    select portal_lexical_candidates.*,
      pg_catalog.row_number() over (
        order by portal_lexical_candidates.lexical_hit_count desc,
          portal_lexical_candidates.id asc,
          portal_lexical_candidates.version desc
      )::integer as lexical_rank
    from portal_lexical_candidates
  ), portal_semantic_candidates as materialized (
    select semantic.*
    from private.portal_projection_semantic_candidates_v1(
      p_kind,
      p_query_embedding
    ) as semantic
  ), portal_semantic_ranked as materialized (
    select portal_semantic_candidates.*,
      pg_catalog.row_number() over (
        order by portal_semantic_candidates.semantic_distance asc,
          portal_semantic_candidates.id asc,
          portal_semantic_candidates.version desc
      )::integer as semantic_rank
    from portal_semantic_candidates
  ), portal_fused as materialized (
    select
      coalesce(portal_lexical_ranked.id, portal_semantic_ranked.id) as id,
      coalesce(portal_lexical_ranked.version, portal_semantic_ranked.version)
        as version,
      portal_lexical_ranked.lexical_rank,
      portal_semantic_ranked.semantic_rank,
      portal_semantic_ranked.semantic_distance,
      pg_catalog.round(
        least(
          1::numeric,
          greatest(
            0::numeric,
            (
              coalesce(
                0.5::numeric / (60 + portal_lexical_ranked.lexical_rank),
                0::numeric
              )
              + coalesce(
                0.5::numeric / (60 + portal_semantic_ranked.semantic_rank),
                0::numeric
              )
            ) * 61::numeric
          )
        ),
        12
      ) as normalized_score
    from portal_lexical_ranked
    full outer join portal_semantic_ranked
      on portal_semantic_ranked.id = portal_lexical_ranked.id
     and portal_semantic_ranked.version = portal_lexical_ranked.version
  ), portal_fused_decorated as materialized (
    select portal_fused.*,
      projection.card,
      projection.state_code,
      projection.modified_at
    from portal_fused
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind
     and projection.id = portal_fused.id
     and projection.version = portal_fused.version
  ), portal_filtered as materialized (
    select portal_fused_decorated.*
    from portal_fused_decorated
    where (
        not (p_filters ? 'accessLevel')
        or portal_fused_decorated.card ->> 'accessLevel'
          = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card #>> '{geography,code}',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(coalesce(
            portal_fused_decorated.card -> 'classifications',
            '[]'::jsonb
          )) as classification(item)
          where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
            = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), portal_ordered as materialized (
    select portal_filtered.*
    from portal_filtered
    order by portal_filtered.normalized_score desc,
      portal_filtered.id asc,
      portal_filtered.version desc
    limit p_limit
  )
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', portal_ordered.id::text,
          'version', portal_ordered.version
        ),
        'accessLevel', portal_ordered.card -> 'accessLevel',
        'capabilities', portal_ordered.card -> 'capabilities',
        'names', portal_ordered.card -> 'names',
        'summary', portal_ordered.card -> 'summary',
        'geography', portal_ordered.card -> 'geography',
        'referenceYear', portal_ordered.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          portal_ordered.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', 'hybrid',
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'score', portal_ordered.normalized_score,
          'reasonCodes', pg_catalog.to_jsonb(pg_catalog.array_remove(array[
            case when portal_ordered.lexical_rank is not null
              then 'lexical_public_projection'::text end,
            case when portal_ordered.semantic_rank is not null
              then 'semantic_public_projection'::text end
          ], null)),
          'evidence', pg_catalog.jsonb_build_object(
            'lexicalRank', portal_ordered.lexical_rank,
            'semanticRank', portal_ordered.semantic_rank,
            'semanticDistance', case
              when portal_ordered.semantic_distance is null then null
              else pg_catalog.trim_scale(
                portal_ordered.semantic_distance::numeric
              )::text
            end
          )
        )
      )
      order by portal_ordered.normalized_score desc,
        portal_ordered.id asc,
        portal_ordered.version desc
    ),
    '[]'::jsonb
  )
  into v_items
  from portal_ordered;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-hybrid-candidate-page.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'items', v_items
  );
  if pg_catalog.octet_length(
    pg_catalog.convert_to(v_result::text, 'UTF8')
  ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_result;
end
$$;

ALTER FUNCTION "private"."portal_projection_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") FROM PUBLIC;

REVOKE ALL ON FUNCTION "private"."portal_projection_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") FROM "api_internal_executor";

GRANT ALL ON FUNCTION "private"."portal_projection_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") TO "portal_public_executor";
