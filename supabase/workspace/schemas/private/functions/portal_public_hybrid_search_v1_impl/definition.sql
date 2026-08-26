CREATE OR REPLACE FUNCTION "private"."portal_public_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    AS $$
declare
  v_items jsonb;
  v_result jsonb;
begin
  with source_rows as materialized (
    select
      process.id,
      process.version::text as version,
      process.json as json_data,
      process.state_code,
      process.modified_at,
      process.embedding_ft
    from public.processes as process
    where p_kind = 'process'
      and process.state_code in (100, 200)
      and process.modified_at is not null
      and pg_catalog.jsonb_typeof(process.json) = 'object'
      and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    union all
    select
      flow.id,
      flow.version::text as version,
      flow.json as json_data,
      flow.state_code,
      flow.modified_at,
      flow.embedding_ft
    from public.flows as flow
    where p_kind = 'flow'
      and flow.state_code in (100, 200)
      and flow.modified_at is not null
      and pg_catalog.jsonb_typeof(flow.json) = 'object'
      and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
  ), latest_ranked as materialized (
    select source_rows.*,
      pg_catalog.row_number() over (
        partition by source_rows.id
        order by source_rows.version desc, source_rows.modified_at desc, source_rows.state_code desc
      ) as latest_rank
    from source_rows
  ), latest as materialized (
    select latest_ranked.*
    from latest_ranked
    where latest_ranked.latest_rank = 1
  ), decorated as materialized (
    select latest.*,
      private.portal_public_hybrid_card_v1(
        p_kind,
        latest.state_code,
        latest.json_data
      ) as card
    from latest
  ), lexical_counts as materialized (
    select decorated.*,
      (
        select count(*)::integer
        from pg_catalog.unnest(p_query_terms) as query_term(term)
        where pg_catalog.strpos(
          lower(coalesce(decorated.card ->> 'document', '')),
          query_term.term
        ) > 0
      ) as lexical_hit_count
    from decorated
    where decorated.card is not null
  ), lexical_candidates as materialized (
    select lexical_counts.*
    from lexical_counts
    where lexical_counts.lexical_hit_count > 0
    order by lexical_counts.lexical_hit_count desc,
      lexical_counts.id asc,
      lexical_counts.version desc
    limit 200
  ), lexical_ranked as materialized (
    select lexical_candidates.id,
      lexical_candidates.version,
      pg_catalog.row_number() over (
        order by lexical_candidates.lexical_hit_count desc,
          lexical_candidates.id asc,
          lexical_candidates.version desc
      )::integer as lexical_rank
    from lexical_candidates
  ), semantic_distances as materialized (
    select latest.id,
      latest.version,
      (
        latest.embedding_ft operator(extensions.<=>) p_query_embedding
      ) as semantic_distance
    from latest
    where latest.embedding_ft is not null
  ), semantic_candidates as materialized (
    select semantic_distances.*
    from semantic_distances
    where semantic_distances.semantic_distance is not null
      and semantic_distances.semantic_distance >= 0::double precision
      and semantic_distances.semantic_distance <= 0.5::double precision
    order by semantic_distances.semantic_distance asc,
      semantic_distances.id asc,
      semantic_distances.version desc
    limit 200
  ), semantic_ranked as materialized (
    select semantic_candidates.id,
      semantic_candidates.version,
      semantic_candidates.semantic_distance,
      pg_catalog.row_number() over (
        order by semantic_candidates.semantic_distance asc,
          semantic_candidates.id asc,
          semantic_candidates.version desc
      )::integer as semantic_rank
    from semantic_candidates
  ), fused as materialized (
    select
      coalesce(lexical_ranked.id, semantic_ranked.id) as id,
      coalesce(lexical_ranked.version, semantic_ranked.version) as version,
      lexical_ranked.lexical_rank,
      semantic_ranked.semantic_rank,
      semantic_ranked.semantic_distance,
      pg_catalog.round(
        least(
          1::numeric,
          greatest(
            0::numeric,
            (
              coalesce(0.5::numeric / (60 + lexical_ranked.lexical_rank), 0::numeric)
              + coalesce(0.5::numeric / (60 + semantic_ranked.semantic_rank), 0::numeric)
            ) * 61::numeric
          )
        ),
        12
      ) as normalized_score
    from lexical_ranked
    full outer join semantic_ranked
      on semantic_ranked.id = lexical_ranked.id
     and semantic_ranked.version = lexical_ranked.version
  ), filtered as materialized (
    select fused.*,
      decorated.card,
      decorated.modified_at
    from fused
    join decorated
      on decorated.id = fused.id
     and decorated.version = fused.version
    where (
        not (p_filters ? 'accessLevel')
        or decorated.card ->> 'accessLevel' = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or lower(btrim(coalesce(decorated.card #>> '{geography,code}', '')))
          = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(
            coalesce(decorated.card -> 'classifications', '[]'::jsonb)
          ) as classification(item)
          where lower(btrim(classification.item ->> 'code'))
            = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (decorated.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (decorated.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or lower(btrim(coalesce(decorated.card ->> 'processSubtype', '')))
          = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or lower(btrim(coalesce(decorated.card ->> 'source', '')))
          = p_filters ->> 'source'
      )
  ), ordered as materialized (
    select filtered.*
    from filtered
    order by filtered.normalized_score desc,
      filtered.id asc,
      filtered.version desc
    limit p_limit
  )
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', ordered.id::text,
          'version', ordered.version
        ),
        'accessLevel', ordered.card -> 'accessLevel',
        'capabilities', ordered.card -> 'capabilities',
        'names', ordered.card -> 'names',
        'summary', ordered.card -> 'summary',
        'geography', ordered.card -> 'geography',
        'referenceYear', ordered.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          ordered.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', 'hybrid',
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'score', ordered.normalized_score,
          'reasonCodes', pg_catalog.to_jsonb(pg_catalog.array_remove(array[
            case when ordered.lexical_rank is not null
              then 'lexical_public_projection'::text end,
            case when ordered.semantic_rank is not null
              then 'semantic_public_projection'::text end
          ], null)),
          'evidence', pg_catalog.jsonb_build_object(
            'lexicalRank', ordered.lexical_rank,
            'semanticRank', ordered.semantic_rank,
            'semanticDistance', case
              when ordered.semantic_distance is null then null
              else pg_catalog.trim_scale(ordered.semantic_distance::numeric)::text
            end
          )
        )
      )
      order by ordered.normalized_score desc,
        ordered.id asc,
        ordered.version desc
    ),
    '[]'::jsonb
  )
  into v_items
  from ordered;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-hybrid-candidate-page.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'items', v_items
  );
  if pg_catalog.octet_length(pg_catalog.convert_to(v_result::text, 'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_result;
end
$$;

ALTER FUNCTION "private"."portal_public_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_public_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_public_hybrid_search_v1_impl"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb", "p_limit" integer, "p_query_fingerprint" "text") TO "portal_public_executor";
