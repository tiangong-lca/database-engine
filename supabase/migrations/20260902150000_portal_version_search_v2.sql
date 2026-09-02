-- Database #600: additive, version-aware Portal retrieval.
-- Existing v1 card/document derivation, writers, indexes and APIs are unchanged.
-- Public versions are an allowlist (100,200), never "anything except draft".
begin;

-- Register pgvector's user-settable GUCs in this migration connection before
-- storing them in function configuration (a fresh connection has not loaded it).
select extensions.vector_dims('[1]'::extensions.vector);

grant portal_public_executor, api_internal_executor to postgres;
grant create on schema private, api to portal_public_executor, api_internal_executor;

create function private.portal_card_matches_filters_v2(p_card jsonb, p_filters jsonb)
returns boolean language sql immutable parallel safe
set search_path = ''
as $$
  select
    (not (p_filters ? 'accessLevel') or p_card ->> 'accessLevel' = p_filters ->> 'accessLevel')
    and (not (p_filters ? 'geography') or pg_catalog.lower(pg_catalog.btrim(coalesce(
      p_card #>> '{geography,code}', ''))) = p_filters ->> 'geography')
    and (not (p_filters ? 'classification') or exists (
      select 1 from pg_catalog.jsonb_array_elements(coalesce(
        p_card -> 'classifications', '[]'::jsonb)) as classification(item)
      where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
        = p_filters ->> 'classification'))
    and (not (p_filters ? 'referenceYearFrom') or (p_card ->> 'referenceYear')::integer
      >= (p_filters ->> 'referenceYearFrom')::integer)
    and (not (p_filters ? 'referenceYearTo') or (p_card ->> 'referenceYear')::integer
      <= (p_filters ->> 'referenceYearTo')::integer)
    and (not (p_filters ? 'processSubtype') or pg_catalog.lower(pg_catalog.btrim(coalesce(
      p_card ->> 'processSubtype', ''))) = p_filters ->> 'processSubtype')
    and (not (p_filters ? 'source') or pg_catalog.lower(pg_catalog.btrim(coalesce(
      p_card ->> 'source', ''))) = p_filters ->> 'source');
$$;
alter function private.portal_card_matches_filters_v2(jsonb,jsonb) owner to portal_public_executor;
revoke all on function private.portal_card_matches_filters_v2(jsonb,jsonb) from public, anon, authenticated, service_role;
grant execute on function private.portal_card_matches_filters_v2(jsonb,jsonb) to api_internal_executor;

create function private.catalog_portal_candidate_rows_v2(
  p_kind text, p_query text, p_exact_id uuid, p_like_pattern text
) returns table(id uuid, version text, card jsonb, state_code integer, modified_at timestamptz)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $$
begin
  if p_kind not in ('process','flow') or p_kind is null then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_query = '' then
    return query
    select p.id, p.version, p.card, p.state_code, p.modified_at
    from private.portal_catalog_search_rows_v1 as p
    where p.dataset_kind = p_kind and p.state_code in (100,200);
    return;
  end if;
  if p_kind = 'flow' and private.portal_catalog_summary_valid_cas_v1(p_query) then
    return query
    select p.id, p.version, p.card, p.state_code, p.modified_at
    from private.portal_catalog_search_rows_v1 as p
    where p.dataset_kind = 'flow' and p.state_code in (100,200)
      and pg_catalog.jsonb_typeof(p.card -> 'casNumber') = 'string'
      and p.card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
      and pg_catalog.length(p.card ->> 'casNumber') between 7 and 12
      and p.card ->> 'casNumber' = p_query;
    return;
  end if;
  return query
  with pattern_matches as materialized (
    select pattern.id,pattern.version
    from private.catalog_portal_process_pattern_versions_v1(p_like_pattern) as pattern
    where p_kind='process'
    union all
    select pattern.id,pattern.version
    from private.catalog_portal_flow_pattern_versions_v1(p_like_pattern) as pattern
    where p_kind='flow'
  ), matched as materialized (
    select pattern.id, pattern.version
    from pattern_matches as pattern
    union
    select p.id, p.version
    from private.portal_catalog_search_rows_v1 as p
    where p.dataset_kind = p_kind and p.id = p_exact_id and p.state_code in (100,200)
  )
  select p.id, p.version, p.card, p.state_code, p.modified_at
  from matched
  join private.portal_catalog_search_rows_v1 as p
    on p.dataset_kind = p_kind and p.id = matched.id and p.version = matched.version
  where p.state_code in (100,200);
end;
$$;
alter function private.catalog_portal_candidate_rows_v2(text,text,uuid,text) owner to portal_public_executor;
revoke all on function private.catalog_portal_candidate_rows_v2(text,text,uuid,text) from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_candidate_rows_v2(text,text,uuid,text) to api_internal_executor;

-- Helpers below have a fixed 200 eligible-row output bound. Iterative HNSW
-- compensates for visibility/filter rejection only within its scan/memory budget.
-- Underfill is a valid answer; there is NO fill-driven full-source exact fallback.

create function private.portal_projection_semantic_process_v2(
  p_query_embedding extensions.vector, p_filters jsonb
) returns table(id uuid, version text, semantic_distance double precision)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '20s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
set hnsw.ef_search = '200'
set hnsw.max_scan_tuples = '20000'
set hnsw.scan_mem_multiplier = '2'
set jit = 'off'
set row_security = 'on'
as $$
begin
  if p_query_embedding is null or extensions.vector_dims(p_query_embedding) <> 1024 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return query
  with nearest as materialized (
    select source.id, source.version::text as version,
      source.embedding_ft operator(extensions.<=>) p_query_embedding as distance
    from public.processes as source
    where source.state_code in (100,200)
      and source.embedding_ft is not null
      and exists (
        select 1 from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'process'
          and projection.id = source.id and projection.version = source.version::text
          and projection.state_code in (100,200)
          and (p_filters = '{}'::jsonb
            or private.portal_card_matches_filters_v2(projection.card, p_filters))
        offset 0
      )
    order by source.embedding_ft operator(extensions.<=>) p_query_embedding
    limit 200
  )
  select nearest.id, nearest.version, nearest.distance
  from nearest
  where nearest.distance >= 0::double precision and nearest.distance <= 0.5::double precision
  order by nearest.distance + 0::double precision, nearest.id, nearest.version desc;
end;
$$;
alter function private.portal_projection_semantic_process_v2(extensions.vector,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_semantic_process_v2(extensions.vector,jsonb) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_semantic_process_v2(extensions.vector,jsonb) to portal_public_executor;


create function private.portal_projection_semantic_flow_v2(
  p_query_embedding extensions.vector, p_filters jsonb
) returns table(id uuid, version text, semantic_distance double precision)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '20s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
set hnsw.ef_search = '200'
set hnsw.max_scan_tuples = '20000'
set hnsw.scan_mem_multiplier = '2'
set jit = 'off'
set row_security = 'on'
as $$
begin
  if p_query_embedding is null or extensions.vector_dims(p_query_embedding) <> 1024 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return query
  with nearest as materialized (
    select source.id, source.version::text as version,
      source.embedding_ft operator(extensions.<=>) p_query_embedding as distance
    from public.flows as source
    where source.state_code in (100,200)
      and source.embedding_ft is not null
      and exists (
        select 1 from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = source.id and projection.version = source.version::text
          and projection.state_code in (100,200)
          and (p_filters = '{}'::jsonb
            or private.portal_card_matches_filters_v2(projection.card, p_filters))
        offset 0
      )
    order by source.embedding_ft operator(extensions.<=>) p_query_embedding
    limit 200
  )
  select nearest.id, nearest.version, nearest.distance
  from nearest
  where nearest.distance >= 0::double precision and nearest.distance <= 0.5::double precision
  order by nearest.distance + 0::double precision, nearest.id, nearest.version desc;
end;
$$;
alter function private.portal_projection_semantic_flow_v2(extensions.vector,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_semantic_flow_v2(extensions.vector,jsonb) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_semantic_flow_v2(extensions.vector,jsonb) to portal_public_executor;


create function private.portal_projection_semantic_candidates_v2(
  p_kind text, p_query_embedding extensions.vector, p_filters jsonb
) returns table(id uuid, version text, semantic_distance double precision)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '20s'
set row_security = 'on'
as $$
begin
  if p_kind = 'process' then
    return query select * from private.portal_projection_semantic_process_v2(p_query_embedding,p_filters);
  elsif p_kind = 'flow' then
    return query select * from private.portal_projection_semantic_flow_v2(p_query_embedding,p_filters);
  else
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
end;
$$;
alter function private.portal_projection_semantic_candidates_v2(text,extensions.vector,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_semantic_candidates_v2(text,extensions.vector,jsonb) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_semantic_candidates_v2(text,extensions.vector,jsonb) to portal_public_executor;

create function private.portal_projection_hybrid_candidates_v2(
  p_kind text, p_query_terms text[], p_query_embedding extensions.vector, p_filters jsonb
) returns table(
  id uuid, version text, lexical_rank integer, semantic_rank integer,
  semantic_distance double precision, score numeric
)
language sql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '20s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $$
  with lexical_counts as materialized (
    select match.id, match.version, pg_catalog.count(distinct match.term_ordinal)::integer as hit_count
    from private.catalog_portal_hybrid_pattern_matches_v1(p_kind,p_query_terms) as match
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind and projection.id = match.id
        and projection.version = match.version
    where projection.state_code in (100,200)
      and (p_filters = '{}'::jsonb or private.portal_card_matches_filters_v2(projection.card,p_filters))
    group by match.id, match.version
  ), lexical_candidates as materialized (
    select * from lexical_counts
    where hit_count > 0
    order by hit_count desc, id, version desc
    limit 200
  ), lexical as materialized (
    select candidate.*,
      pg_catalog.row_number() over(order by hit_count desc,id,version desc)::integer as ordinal
    from lexical_candidates as candidate
  ), semantic as materialized (
    select candidate.*,
      pg_catalog.row_number() over(order by semantic_distance,id,version desc)::integer as ordinal
    from private.portal_projection_semantic_candidates_v2(p_kind,p_query_embedding,p_filters) as candidate
  )
  select coalesce(lexical.id,semantic.id), coalesce(lexical.version,semantic.version),
    lexical.ordinal, semantic.ordinal, semantic.semantic_distance,
    pg_catalog.round(least(1::numeric,greatest(0::numeric,(
      coalesce(0.5::numeric / (60 + lexical.ordinal),0::numeric)
      + coalesce(0.5::numeric / (60 + semantic.ordinal),0::numeric)
    ) * 61::numeric)),12)
  from lexical full outer join semantic
    on semantic.id = lexical.id and semantic.version = lexical.version;
$$;
alter function private.portal_projection_hybrid_candidates_v2(text,text[],extensions.vector,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_hybrid_candidates_v2(text,text[],extensions.vector,jsonb) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_hybrid_candidates_v2(text,text[],extensions.vector,jsonb) to portal_public_executor;

create function private.portal_projection_hybrid_search_v2_impl(
  p_kind text, p_query_terms text[], p_query_embedding extensions.vector,
  p_filters jsonb, p_limit integer, p_query_fingerprint text, p_cursor jsonb
) returns jsonb
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '20s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $$
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
alter function private.portal_projection_hybrid_search_v2_impl(text,text[],extensions.vector,jsonb,integer,text,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_hybrid_search_v2_impl(text,text[],extensions.vector,jsonb,integer,text,jsonb) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_hybrid_search_v2_impl(text,text[],extensions.vector,jsonb,integer,text,jsonb) to portal_public_executor;

create function api.portal_hybrid_search_v2(
  p_kind text, p_query_terms text[], p_query_embedding text, p_filters jsonb,
  p_limit integer, p_cursor text default null
) returns jsonb
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '20s'
as $$
declare
  v_input jsonb;
  v_fingerprint text;
  v_cursor jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,p_query_terms,p_query_embedding,p_filters,p_limit);
  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-hybrid-rank-v2:' || (v_input ->> 'queryFingerprint'),'UTF8'),
    'sha256'),'hex');
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
      or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
      or not (v_cursor ?& array['v','fp','kind','rankKey','id','version'])
      or v_cursor ->> 'v' is distinct from '1'
      or v_cursor ->> 'fp' is distinct from v_fingerprint
      or v_cursor ->> 'kind' is distinct from p_kind
      or pg_catalog.jsonb_typeof(v_cursor -> 'rankKey') is distinct from 'string'
      or coalesce(v_cursor ->> 'rankKey','') !~ '^(0(\.\d{1,12})?|1(\.0{1,12})?)$'
      or coalesce(v_cursor ->> 'id','') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(v_cursor ->> 'version','') !~ '^\d{2}\.\d{2}\.\d{3}$'
      or private.portal_cursor_encode_v1(v_cursor) is distinct from p_cursor then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;
  v_page := private.portal_decorate_card_context_v1(private.portal_lcia_decorate_item_page_v1(
    private.portal_projection_hybrid_search_v2_impl(
      p_kind,
      array(select term.value from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
        with ordinality as term(value,ordinality) order by term.ordinality),
      (v_input ->> 'queryEmbedding')::extensions.vector(1024),
      v_input -> 'filters',p_limit,v_fingerprint,v_cursor
    )
  ));
  v_page := pg_catalog.jsonb_set(v_page,'{schemaVersion}','"portal.public-hybrid-candidate-page.v2"'::jsonb);
  v_page := (v_page - 'nextCursorPayload') || pg_catalog.jsonb_build_object(
    'nextCursor',case when nullif(v_page -> 'nextCursorPayload','null'::jsonb) is null then null
      else private.portal_cursor_encode_v1(v_page -> 'nextCursorPayload') end
  );
  if v_page is null or pg_catalog.octet_length(pg_catalog.convert_to(v_page::text,'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end;
$$;
alter function api.portal_hybrid_search_v2(text,text[],text,jsonb,integer,text) owner to portal_public_executor;
revoke all on function api.portal_hybrid_search_v2(text,text[],text,jsonb,integer,text) from public, anon, authenticated, service_role;
grant execute on function api.portal_hybrid_search_v2(text,text[],text,jsonb,integer,text) to anon, authenticated;


-- Version-aware lexical browse/search and facet consumers.
CREATE OR REPLACE FUNCTION "private"."catalog_portal_search_v2_impl"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $_$
declare
  v_items jsonb;
  v_next_cursor_payload jsonb;
  v_exact_id uuid;
  v_like_pattern text;
begin
  if p_query ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact_id := p_query::uuid;
  end if;
  if p_query <> '' then
    v_like_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          p_query,
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
  end if;
  -- Empty unfiltered browse pages do not require search facts for the whole
  -- catalog.  Order/cursor reduction happens before at most limit+1
  -- cards are hydrated.
  if p_query = ''
     and p_filters = '{}'::jsonb
     and p_sort in ('relevance', 'modified_desc', 'name_asc') then
    with portal_prefilter as materialized (
      select p_kind as dataset_kind,
        candidate.*,
        case when p_sort = 'name_asc' then case
          when nullif(candidate.card #>> '{names,0,value}', '') is not null
            and pg_catalog.length(
              candidate.card #>> '{names,0,value}'
            ) <= 500
            and pg_catalog.octet_length(
              candidate.card #>> '{names,0,value}'
            ) <= 2000
            and candidate.card #>> '{names,0,value}' !~ '[[:cntrl:]]'
            then candidate.card #>> '{names,0,value}'
          else '~unnamed:' || candidate.id::text
        end end as name_key
      from private.catalog_portal_candidate_rows_v2(
        p_kind,
        p_query,
        v_exact_id,
        v_like_pattern
      ) as candidate
    ), portal_after_cursor as materialized (
      select portal_prefilter.*
      from portal_prefilter
      where p_cursor_rank is null
        or case p_sort
          when 'relevance' then
            0::numeric < p_cursor_rank::numeric
            or (
              0::numeric = p_cursor_rank::numeric
              and (
                portal_prefilter.id > p_cursor_id
                or (
                  portal_prefilter.id = p_cursor_id
                  and portal_prefilter.version < p_cursor_version
                )
              )
            )
          when 'modified_desc' then
            portal_prefilter.modified_at < p_cursor_rank::timestamptz
            or (
              portal_prefilter.modified_at = p_cursor_rank::timestamptz
              and (
                portal_prefilter.id > p_cursor_id
                or (
                  portal_prefilter.id = p_cursor_id
                  and portal_prefilter.version < p_cursor_version
                )
              )
            )
          else
            pg_catalog.lower(portal_prefilter.name_key)
              > pg_catalog.lower(p_cursor_rank)
            or (
              pg_catalog.lower(portal_prefilter.name_key)
                = pg_catalog.lower(p_cursor_rank)
              and (
                portal_prefilter.id > p_cursor_id
                or (
                  portal_prefilter.id = p_cursor_id
                  and portal_prefilter.version < p_cursor_version
                )
              )
            )
        end
    ), portal_ordered as materialized (
      select portal_after_cursor.*,
        pg_catalog.row_number() over (
          order by
            case when p_sort = 'modified_desc'
              then portal_after_cursor.modified_at end desc,
            case when p_sort = 'name_asc'
              then pg_catalog.lower(portal_after_cursor.name_key) end asc,
            portal_after_cursor.id asc,
            portal_after_cursor.version desc
        ) as page_rank
      from portal_after_cursor
      order by
        case when p_sort = 'modified_desc'
          then portal_after_cursor.modified_at end desc,
        case when p_sort = 'name_asc'
          then pg_catalog.lower(portal_after_cursor.name_key) end asc,
        portal_after_cursor.id asc,
        portal_after_cursor.version desc
      limit p_limit + 1
    ), portal_decorated as materialized (
      select portal_ordered.*
      from portal_ordered
    )
    select
      coalesce(pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', p_kind,
            'id', portal_decorated.id::text,
            'version', portal_decorated.version
          ),
          'accessLevel', portal_decorated.card -> 'accessLevel',
          'capabilities', portal_decorated.card -> 'capabilities',
          'names', portal_decorated.card -> 'names',
          'summary', portal_decorated.card -> 'summary',
          'geography', portal_decorated.card -> 'geography',
          'referenceYear', portal_decorated.card -> 'referenceYear',
          'modifiedAt', pg_catalog.to_char(
            portal_decorated.modified_at at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          ),
          'match', pg_catalog.jsonb_build_object(
            'kind', 'lexical',
            'score', 0::numeric,
            'reasonCodes', '[]'::jsonb
          )
        ) order by portal_decorated.page_rank
      ) filter (where portal_decorated.page_rank <= p_limit), '[]'::jsonb),
      case when max(portal_decorated.page_rank) > p_limit then
        (pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'v', 1,
          'fp', p_query_fingerprint,
          'rankKey', case p_sort
            when 'relevance' then '0'
            when 'modified_desc' then pg_catalog.to_char(
              portal_decorated.modified_at at time zone 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            )
            else pg_catalog.lower(portal_decorated.name_key)
          end,
          'kind', p_kind,
          'id', portal_decorated.id::text,
          'version', portal_decorated.version
        ) order by portal_decorated.page_rank)
          filter (where portal_decorated.page_rank = p_limit)) -> 0
      else null end
    into v_items, v_next_cursor_payload
    from portal_decorated;

    return pg_catalog.jsonb_build_object(
      'items', v_items,
      'nextCursorPayload', v_next_cursor_payload
    );
  end if;


  with portal_prefilter as materialized (
    select p_kind as dataset_kind,
      candidate.*
    from private.catalog_portal_candidate_rows_v2(
      p_kind,
      p_query,
      v_exact_id,
      v_like_pattern
    ) as candidate
  ), portal_facts as materialized (
    select portal_prefilter.*,
      private.catalog_portal_card_facts_v1(
        portal_prefilter.card,
        p_filters,
        p_query
      ) as facts
    from portal_prefilter
  ), portal_scored as materialized (
    select portal_facts.*,
      case
        when nullif(portal_facts.facts ->> 'nameKey', '') is not null
          and pg_catalog.length(portal_facts.facts ->> 'nameKey') <= 500
          and pg_catalog.octet_length(portal_facts.facts ->> 'nameKey') <= 2000
          and portal_facts.facts ->> 'nameKey' !~ '[[:cntrl:]]'
          then portal_facts.facts ->> 'nameKey'
        else '~unnamed:' || portal_facts.id::text
      end as name_key,
      case
        when p_query = '' then 0::numeric
        when pg_catalog.lower(portal_facts.id::text) = p_query then 1::numeric
        when pg_catalog.lower(coalesce(portal_facts.facts ->> 'casNumber', '')) = p_query
          then 0.98::numeric
        when (portal_facts.facts ->> 'nameExact')::boolean then 0.95::numeric
        when (portal_facts.facts ->> 'classificationExact')::boolean
          then 0.92::numeric
        when p_query <> '' then 0.70::numeric
        else 0::numeric
      end as score,
      case
        when pg_catalog.lower(portal_facts.id::text) = p_query
          then pg_catalog.jsonb_build_array('exact_id')
        when pg_catalog.lower(coalesce(portal_facts.facts ->> 'casNumber', '')) = p_query
          then pg_catalog.jsonb_build_array('cas')
        when (portal_facts.facts ->> 'nameExact')::boolean
          or (portal_facts.facts ->> 'nameContains')::boolean
          then pg_catalog.jsonb_build_array('name')
        when (portal_facts.facts ->> 'classificationExact')::boolean
          or (portal_facts.facts ->> 'classificationContains')::boolean
          then pg_catalog.jsonb_build_array('classification')
        when p_query <> '' then pg_catalog.jsonb_build_array('full_text')
        else '[]'::jsonb
      end as reason_codes
    from portal_facts
  ), portal_filtered as materialized (
    select portal_scored.*,
      case p_sort
        when 'relevance' then portal_scored.score::text
        when 'modified_desc' then pg_catalog.to_char(
          portal_scored.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
        else pg_catalog.lower(portal_scored.name_key)
      end as rank_key
    from portal_scored
    where (p_query = '' or portal_scored.score > 0)
      and (
        not (p_filters ? 'accessLevel')
        or portal_scored.facts ->> 'accessLevel' = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_scored.facts ->> 'geographyCode',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or (portal_scored.facts ->> 'classificationFilterMatch')::boolean
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (portal_scored.facts ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (portal_scored.facts ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_scored.facts ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_scored.facts ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), portal_after_cursor as materialized (
    select portal_filtered.*
    from portal_filtered
    where p_cursor_rank is null
      or case p_sort
        when 'relevance' then
          portal_filtered.score < p_cursor_rank::numeric
          or (
            portal_filtered.score = p_cursor_rank::numeric
            and (
              portal_filtered.id > p_cursor_id
              or (
                portal_filtered.id = p_cursor_id
                and portal_filtered.version < p_cursor_version
              )
            )
          )
        when 'modified_desc' then
          portal_filtered.modified_at < p_cursor_rank::timestamptz
          or (
            portal_filtered.modified_at = p_cursor_rank::timestamptz
            and (
              portal_filtered.id > p_cursor_id
              or (
                portal_filtered.id = p_cursor_id
                and portal_filtered.version < p_cursor_version
              )
            )
          )
        else
          pg_catalog.lower(portal_filtered.name_key) > pg_catalog.lower(p_cursor_rank)
          or (
            pg_catalog.lower(portal_filtered.name_key) = pg_catalog.lower(p_cursor_rank)
            and (
              portal_filtered.id > p_cursor_id
              or (
                portal_filtered.id = p_cursor_id
                and portal_filtered.version < p_cursor_version
              )
            )
          )
      end
  ), portal_ordered as materialized (
    select portal_after_cursor.*,
      pg_catalog.row_number() over (
        order by
          case when p_sort = 'relevance' then portal_after_cursor.score end desc,
          case when p_sort = 'modified_desc' then portal_after_cursor.modified_at end desc,
          case when p_sort = 'name_asc'
            then pg_catalog.lower(portal_after_cursor.name_key) end asc,
          portal_after_cursor.id asc,
          portal_after_cursor.version desc
      ) as page_rank
    from portal_after_cursor
    order by
      case when p_sort = 'relevance' then portal_after_cursor.score end desc,
      case when p_sort = 'modified_desc' then portal_after_cursor.modified_at end desc,
      case when p_sort = 'name_asc'
        then pg_catalog.lower(portal_after_cursor.name_key) end asc,
      portal_after_cursor.id asc,
      portal_after_cursor.version desc
    limit p_limit + 1
  ), portal_hydrated as materialized (
    select portal_ordered.*
    from portal_ordered
  )
  select
    coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', portal_hydrated.id::text,
          'version', portal_hydrated.version
        ),
        'accessLevel', portal_hydrated.card -> 'accessLevel',
        'capabilities', portal_hydrated.card -> 'capabilities',
        'names', portal_hydrated.card -> 'names',
        'summary', portal_hydrated.card -> 'summary',
        'geography', portal_hydrated.card -> 'geography',
        'referenceYear', portal_hydrated.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          portal_hydrated.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', case when portal_hydrated.reason_codes
            ?| array['exact_id', 'cas', 'classification']
            then 'identifier' else 'lexical' end,
          'score', portal_hydrated.score,
          'reasonCodes', portal_hydrated.reason_codes
        )
      ) order by portal_hydrated.page_rank
    ) filter (where portal_hydrated.page_rank <= p_limit), '[]'::jsonb),
    case when max(portal_hydrated.page_rank) > p_limit then
      (pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'v', 1,
        'fp', p_query_fingerprint,
        'rankKey', portal_hydrated.rank_key,
        'kind', p_kind,
        'id', portal_hydrated.id::text,
        'version', portal_hydrated.version
      ) order by portal_hydrated.page_rank)
        filter (where portal_hydrated.page_rank = p_limit)) -> 0
    else null end
  into v_items, v_next_cursor_payload
  from portal_hydrated;

  return pg_catalog.jsonb_build_object(
    'items', v_items,
    'nextCursorPayload', v_next_cursor_payload
  );
end
$_$;

ALTER FUNCTION "private"."catalog_portal_search_v2_impl"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_search_v2_impl"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."catalog_portal_search_v2_impl"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") TO "portal_public_executor";


CREATE OR REPLACE FUNCTION "private"."portal_search_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_query text;
  v_filters jsonb;
  v_sort text;
  v_limit integer := coalesce(p_limit, 20);
  v_fingerprint text;
  v_cursor jsonb;
  v_cursor_rank text;
  v_cursor_id uuid;
  v_cursor_version text;
  v_kernel jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();

  perform private.portal_validate_search_v1(
    p_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    coalesce(p_sort, 'relevance'),
    v_limit
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_sort := pg_catalog.lower(pg_catalog.btrim(coalesce(p_sort, 'relevance')));
  v_fingerprint := private.portal_query_fingerprint_v1(
    p_kind,
    v_query,
    v_filters,
    v_sort
  );
  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-search-versions-v2:' || v_fingerprint,'UTF8'),'sha256'),'hex');
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'fp' <> v_fingerprint
       or v_cursor ->> 'kind' <> p_kind
       or coalesce(v_cursor ->> 'rankKey', '') = ''
       or coalesce(v_cursor ->> 'id', '')
         !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_rank := v_cursor ->> 'rankKey';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
    v_cursor_version := v_cursor ->> 'version';
    if v_sort = 'relevance'
       and v_cursor_rank !~ '^(0(\.\d+)?|1(\.0+)?)$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    elsif v_sort = 'modified_desc'
       and private.portal_datetime_v1(v_cursor_rank) is null then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  v_kernel := private.catalog_portal_search_v2_impl(
    p_kind,v_query,v_filters,v_sort,v_cursor_rank,v_cursor_id,v_cursor_version,v_limit,v_fingerprint
  );

  v_next_cursor_payload := nullif(
    v_kernel -> 'nextCursorPayload',
    'null'::jsonb
  );

  return pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-search-page.v1',
    'kind', p_kind,
    'queryFingerprint', v_fingerprint,
    'items', coalesce(v_kernel -> 'items', '[]'::jsonb),
    'nextCursor', case when v_next_cursor_payload is null then null
      else private.portal_cursor_encode_v1(v_next_cursor_payload)
    end
  );
end
$_$;

ALTER FUNCTION "private"."portal_search_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_search_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;


CREATE OR REPLACE FUNCTION "api"."portal_search_processes_v2"("p_query" "text", "p_filters" "jsonb" DEFAULT '{}'::"jsonb", "p_sort" "text" DEFAULT 'relevance'::"text", "p_cursor" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 20) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $$
begin
  return pg_catalog.jsonb_set(private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_search_v2(
        'process', p_query, p_filters, p_sort, p_cursor, p_limit
      )
    )
  ), '{schemaVersion}', '"portal.public-search-page.v2"'::jsonb);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$$;

ALTER FUNCTION "api"."portal_search_processes_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_search_processes_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_search_processes_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."portal_search_processes_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) TO "authenticated";


CREATE OR REPLACE FUNCTION "api"."portal_search_flows_v2"("p_query" "text", "p_filters" "jsonb" DEFAULT '{}'::"jsonb", "p_sort" "text" DEFAULT 'relevance'::"text", "p_cursor" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 20) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $$
begin
  return pg_catalog.jsonb_set(private.portal_decorate_card_context_v1(
    private.portal_search_v2(
      'flow', p_query, p_filters, p_sort, p_cursor, p_limit
    )
  ), '{schemaVersion}', '"portal.public-search-page.v2"'::jsonb);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$$;

ALTER FUNCTION "api"."portal_search_flows_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_search_flows_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_search_flows_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."portal_search_flows_v2"("p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) TO "authenticated";


CREATE OR REPLACE FUNCTION "private"."catalog_portal_facet_candidate_rows_v2"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") RETURNS TABLE("dataset_kind" "text", "id" "uuid", "version" "text", "card" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
begin
  if p_kind = 'process' then
    return query
    select 'process'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'process', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  elsif p_kind = 'flow' then
    return query
    select 'flow'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'flow', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  elsif p_kind = 'all' then
    return query
    select 'process'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'process', p_query, p_exact_id, p_like_pattern
    ) as candidate;
    return query
    select 'flow'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'flow', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  end if;
end
$$;

ALTER FUNCTION "private"."catalog_portal_facet_candidate_rows_v2"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_facet_candidate_rows_v2"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") FROM PUBLIC;


CREATE OR REPLACE FUNCTION "private"."catalog_portal_facets_v2_impl"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text", "p_filters" "jsonb", "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
  with matched as materialized (
    select candidate.*
    from private.catalog_portal_facet_candidate_rows_v2(
      p_kind,
      p_query,
      p_exact_id,
      p_like_pattern
    ) as candidate
    where (
        not (p_filters ? 'accessLevel')
        or candidate.card ->> 'accessLevel' = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          candidate.card #>> '{geography,code}',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(
            candidate.card -> 'classifications'
          ) as classification(item)
          where pg_catalog.lower(pg_catalog.btrim(
            classification.item ->> 'code'
          )) = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (candidate.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (candidate.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          candidate.card ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          candidate.card ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), facet_values as materialized (
    select 'kind'::text as group_id,
      1 as group_order,
      matched.dataset_kind as value,
      matched.dataset_kind as label
    from matched
    union all
    select 'accessLevel',
      2,
      matched.card ->> 'accessLevel',
      matched.card ->> 'accessLevel'
    from matched
    union all
    select 'geography',
      3,
      pg_catalog.lower(pg_catalog.btrim(
        matched.card #>> '{geography,code}'
      )),
      matched.card #>> '{geography,code}'
    from matched
    union all
    select 'referenceYear',
      4,
      pg_catalog.btrim(matched.card ->> 'referenceYear'),
      pg_catalog.btrim(matched.card ->> 'referenceYear')
    from matched
    union all
    select 'processSubtype',
      5,
      pg_catalog.lower(pg_catalog.btrim(
        matched.card ->> 'processSubtype'
      )),
      matched.card ->> 'processSubtype'
    from matched
    where matched.dataset_kind = 'process'
    union all
    select 'source',
      6,
      pg_catalog.lower(pg_catalog.btrim(matched.card ->> 'source')),
      matched.card ->> 'source'
    from matched
  ), counts as materialized (
    select group_id,
      group_order,
      value,
      pg_catalog.min(value) as label,
      pg_catalog.count(*) as value_count
    from facet_values
    where nullif(pg_catalog.btrim(value), '') is not null
      and pg_catalog.length(value) <= 128
      and pg_catalog.octet_length(value) <= 512
    group by group_id, group_order, value
  ), ranked_counts as materialized (
    select counts.*,
      pg_catalog.row_number() over (
        partition by counts.group_id
        order by counts.value
      ) as value_rank
    from counts
  ), grouped as materialized (
    select ranked_counts.group_id,
      ranked_counts.group_order,
      pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'value', ranked_counts.value,
        'label', pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'language', 'und', 'value', ranked_counts.label
          )
        ),
        'count', ranked_counts.value_count
      ) order by ranked_counts.value)
        filter (where ranked_counts.value_rank <= 100) as values_json,
      pg_catalog.bool_or(ranked_counts.value_rank > 100) as has_more
    from ranked_counts
    group by ranked_counts.group_id, ranked_counts.group_order
  ), groups as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'id', grouped.group_id,
      'label', pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'language', 'en',
          'value', case grouped.group_id
            when 'kind' then 'Object type'
            when 'accessLevel' then 'Access level'
            when 'geography' then 'Geography'
            when 'referenceYear' then 'Reference year'
            when 'processSubtype' then 'Process subtype'
            else 'Source'
          end
        ),
        pg_catalog.jsonb_build_object(
          'language', 'zh-CN',
          'value', case grouped.group_id
            when 'kind' then '对象类型'
            when 'accessLevel' then '访问级别'
            when 'geography' then '地区'
            when 'referenceYear' then '参考年'
            when 'processSubtype' then '过程类型'
            else '数据源'
          end
        )
      ),
      'values', grouped.values_json,
      'hasMore', grouped.has_more
    ) order by grouped.group_order), '[]'::jsonb) as value
    from grouped
  )
  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-facets.v2',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'groups', groups.value
  )
  from groups
$$;

ALTER FUNCTION "private"."catalog_portal_facets_v2_impl"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text", "p_filters" "jsonb", "p_query_fingerprint" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_facets_v2_impl"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text", "p_filters" "jsonb", "p_query_fingerprint" "text") FROM PUBLIC;


CREATE OR REPLACE FUNCTION "private"."catalog_portal_facets_empty_v2_impl"("p_kind" "text", "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "work_mem" TO '32MB'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
  with visible_versions as materialized (
    select
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.facet_access_level,
      facet.facet_geography,
      facet.facet_reference_year,
      facet.facet_process_subtype,
      facet.facet_source
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.facet_contract_version = 1 and facet.state_code in (100,200)
      and (p_kind = 'all' or facet.dataset_kind = p_kind)
  ), facts as materialized (
    select visible_versions.dataset_kind,
      visible_versions.facet_access_level,
      visible_versions.facet_geography,
      visible_versions.facet_reference_year,
      case when visible_versions.dataset_kind = 'process' then
        visible_versions.facet_process_subtype
      else null::text end as facet_process_subtype,
      visible_versions.facet_source
    from visible_versions
  ), counts_raw as materialized (
    select case
        when grouping(facts.dataset_kind) = 0 then 'kind'
        when grouping(facts.facet_access_level) = 0 then 'accessLevel'
        when grouping(facts.facet_geography) = 0 then 'geography'
        when grouping(facts.facet_reference_year) = 0 then 'referenceYear'
        when grouping(facts.facet_process_subtype) = 0 then 'processSubtype'
        else 'source'
      end as group_id,
      case
        when grouping(facts.dataset_kind) = 0 then 1
        when grouping(facts.facet_access_level) = 0 then 2
        when grouping(facts.facet_geography) = 0 then 3
        when grouping(facts.facet_reference_year) = 0 then 4
        when grouping(facts.facet_process_subtype) = 0 then 5
        else 6
      end as group_order,
      case
        when grouping(facts.dataset_kind) = 0 then facts.dataset_kind
        when grouping(facts.facet_access_level) = 0 then
          facts.facet_access_level
        when grouping(facts.facet_geography) = 0 then facts.facet_geography
        when grouping(facts.facet_reference_year) = 0 then
          facts.facet_reference_year
        when grouping(facts.facet_process_subtype) = 0 then
          facts.facet_process_subtype
        else facts.facet_source
      end as value,
      pg_catalog.count(*) as value_count
    from facts
    group by grouping sets (
      (facts.dataset_kind),
      (facts.facet_access_level),
      (facts.facet_geography),
      (facts.facet_reference_year),
      (facts.facet_process_subtype),
      (facts.facet_source)
    )
  ), counts as materialized (
    select counts_raw.group_id,
      counts_raw.group_order,
      counts_raw.value,
      counts_raw.value as label,
      counts_raw.value_count
    from counts_raw
    where nullif(pg_catalog.btrim(counts_raw.value), '') is not null
      and pg_catalog.length(counts_raw.value) <= 128
      and pg_catalog.octet_length(counts_raw.value) <= 512
  ), ranked_counts as materialized (
    select counts.*,
      pg_catalog.row_number() over (
        partition by counts.group_id
        order by counts.value
      ) as value_rank
    from counts
  ), grouped as materialized (
    select ranked_counts.group_id,
      ranked_counts.group_order,
      pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'value', ranked_counts.value,
        'label', pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'language', 'und', 'value', ranked_counts.label
          )
        ),
        'count', ranked_counts.value_count
      ) order by ranked_counts.value)
        filter (where ranked_counts.value_rank <= 100) as values_json,
      pg_catalog.bool_or(ranked_counts.value_rank > 100) as has_more
    from ranked_counts
    group by ranked_counts.group_id, ranked_counts.group_order
  ), groups as (
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'id', grouped.group_id,
      'label', pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'language', 'en',
          'value', case grouped.group_id
            when 'kind' then 'Object type'
            when 'accessLevel' then 'Access level'
            when 'geography' then 'Geography'
            when 'referenceYear' then 'Reference year'
            when 'processSubtype' then 'Process subtype'
            else 'Source'
          end
        ),
        pg_catalog.jsonb_build_object(
          'language', 'zh-CN',
          'value', case grouped.group_id
            when 'kind' then '对象类型'
            when 'accessLevel' then '访问级别'
            when 'geography' then '地区'
            when 'referenceYear' then '参考年'
            when 'processSubtype' then '过程类型'
            else '数据源'
          end
        )
      ),
      'values', grouped.values_json,
      'hasMore', grouped.has_more
    ) order by grouped.group_order), '[]'::jsonb) as value
    from grouped
  )
  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-facets.v2',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'groups', groups.value
  )
  from groups
$$;

ALTER FUNCTION "private"."catalog_portal_facets_empty_v2_impl"("p_kind" "text", "p_query_fingerprint" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_facets_empty_v2_impl"("p_kind" "text", "p_query_fingerprint" "text") FROM PUBLIC;


CREATE OR REPLACE FUNCTION "api"."portal_facets_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $_$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_exact_id uuid;
  v_like_pattern text;
begin
  perform private.assert_portal_catalog_projection_contract_v1();

  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := pg_catalog.lower(pg_catalog.btrim(coalesce(p_kind, '')));
  perform private.portal_validate_search_v1(
    v_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    'relevance',
    1
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_fingerprint := private.portal_query_fingerprint_v1(
    v_kind,
    v_query,
    v_filters,
    'relevance'
  );

  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-search-versions-v2:' || v_fingerprint,'UTF8'),'sha256'),'hex');
  if v_query = '' and v_filters = '{}'::jsonb then
    perform private.assert_portal_catalog_facet_contract_v1();
    return private.catalog_portal_facets_empty_v2_impl(
      v_kind,
      v_fingerprint
    );
  end if;

  if v_query ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact_id := v_query::uuid;
  end if;
  if v_query <> '' then
    v_like_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          v_query,
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
  end if;

  return private.catalog_portal_facets_v2_impl(
    v_kind,
    v_query,
    v_exact_id,
    v_like_pattern,
    v_filters,
    v_fingerprint
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$_$;

ALTER FUNCTION "api"."portal_facets_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_facets_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_facets_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "api"."portal_facets_v2"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") TO "authenticated";



insert into private.api_capability_grants(
  routine_identity,capability_id,allow_anon,allow_authenticated,allow_service_role
) values
  ('api.portal_hybrid_search_v2(text, text[], text, jsonb, integer, text)','PORTAL-HYBRID-01',true,true,false),
  ('api.portal_search_processes_v2(text, jsonb, text, text, integer)','PORTAL-CATALOG-01',true,true,false),
  ('api.portal_search_flows_v2(text, jsonb, text, text, integer)','PORTAL-CATALOG-01',true,true,false),
  ('api.portal_facets_v2(text, text, jsonb)','PORTAL-CATALOG-01',true,true,false);

revoke create on schema private, api from portal_public_executor, api_internal_executor;
revoke portal_public_executor, api_internal_executor from postgres;
notify pgrst, 'reload schema';
commit;
