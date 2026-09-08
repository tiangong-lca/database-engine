-- Database #628: additive composite-name projection. Frozen v1 remains intact.
begin;
set local lock_timeout = '5s';
set local statement_timeout = '120s';
set local check_function_bodies = off;
select extensions.vector_dims('[1]'::extensions.vector);
grant portal_public_executor, api_internal_executor to postgres;
grant create on schema private to portal_public_executor, api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_projection_contract_cn1();
select private.assert_portal_catalog_character_contract_cn1();
select private.assert_portal_process_keyword_rank_contract_cn1();
select private.assert_portal_catalog_facet_contract_v1();
lock table public.processes in share row exclusive mode;
set local statement_timeout = '15s';
do $ready$ begin
  if (select count(*) from private.portal_names_backfill_v2) <> 4
     or exists (
       (select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v1 where dataset_kind='process'
        except select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v2)
       union all
       (select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v2
        except select dataset_kind,id,version,state_code,modified_at from private.portal_catalog_search_rows_v1)
     )
     or (select count(*) from private.portal_catalog_character_rows_v2) <>
        (select count(*) from private.portal_catalog_search_rows_v2)
     or (select count(*) from pg_catalog.pg_trigger where
         tgrelid in ('public.processes'::regclass)
         and tgname='portal_catalog_projection_content_sync_v2' and tgenabled='O'
         and tgfoid='private.sync_portal_catalog_search_row_v2()'::regprocedure) <> 1 then
    raise exception 'Portal composite-name projection is not ready for cutover';
  end if;
end $ready$;
do $indexes$ begin
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_process_document_v2_pgroonga') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_process_document_v2_pgroonga ON private.portal_catalog_search_rows_v2 USING pgroonga (document) WITH (tokenizer=''TokenBigram'', normalizer=''NormalizerAuto'') WHERE (dataset_kind = ''process''::text)') then
    raise exception 'Portal composite-name index portal_catalog_search_process_document_v2_pgroonga is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_process_exact_rank_v2_gin') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_process_exact_rank_v2_gin ON private.portal_catalog_search_rows_v2 USING gin (private.portal_process_rank_name_keys_v1(card), private.portal_process_rank_classification_keys_v1(card)) WHERE (dataset_kind = ''process''::text)') then
    raise exception 'Portal composite-name index portal_catalog_search_process_exact_rank_v2_gin is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_search_rows_latest_v2_idx') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_search_rows_latest_v2_idx ON private.portal_catalog_search_rows_v2 USING btree (dataset_kind, id, version DESC, modified_at DESC, state_code DESC)') then
    raise exception 'Portal composite-name index portal_catalog_search_rows_latest_v2_idx is invalid or drifted';
  end if;
  if not exists(select 1 from pg_catalog.pg_index i where
    i.indexrelid=pg_catalog.to_regclass('private.portal_catalog_summary_eligibility_v2_idx') and i.indisvalid and i.indisready and i.indislive
    and pg_catalog.pg_get_indexdef(i.indexrelid)='CREATE INDEX portal_catalog_summary_eligibility_v2_idx ON private.portal_catalog_search_rows_v2 USING btree (dataset_kind, id, version DESC, modified_at DESC, state_code DESC) WHERE (((dataset_kind = ''flow''::text) AND (jsonb_typeof((card -> ''casNumber''::text)) = ''string''::text) AND ((card ->> ''casNumber''::text) ~ ''^[0-9]{2,7}-[0-9]{2}-[0-9]$''::text)) OR ((jsonb_typeof((card -> ''classifications''::text)) = ''array''::text) AND (jsonb_array_length((card -> ''classifications''::text)) > 0)))') then
    raise exception 'Portal composite-name index portal_catalog_summary_eligibility_v2_idx is invalid or drifted';
  end if;
end $indexes$;
CREATE OR REPLACE FUNCTION private.catalog_portal_candidate_rows_v1(p_kind text, p_query text, p_exact_id uuid, p_like_pattern text)
 RETURNS TABLE(id uuid, version text, card jsonb, state_code integer, modified_at timestamp with time zone)
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
begin
  if p_kind = 'process' and p_query = '' then
    return query
    select distinct on (projection.id)
      projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from private.portal_catalog_search_current_v2 as projection
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc;
    return;
  end if;

  if p_kind = 'process' and p_exact_id is not null then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version,
        false as exact_id
      from private.catalog_portal_process_pattern_versions_v1(
        p_like_pattern
      ) as pattern
      union
      select projection.id,
        projection.version,
        true
      from private.portal_catalog_search_current_v2 as projection
      where projection.dataset_kind = 'process'
        and projection.id = p_exact_id
    ), candidate_ids as materialized (
      select matched.id,
        pg_catalog.bool_or(matched.exact_id) as exact_id
      from matched
      group by matched.id
    ), matched_versions as materialized (
      select distinct matched.id,
        matched.version
      from matched
    ), latest_keys as materialized (
      select latest.id,
        latest.version,
        candidate_ids.exact_id
      from candidate_ids
      cross join lateral (
        select projection.id,
          projection.version
        from private.portal_catalog_search_current_v2 as projection
        where projection.dataset_kind = 'process'
          and projection.id = candidate_ids.id
        order by projection.version desc,
          projection.modified_at desc,
          projection.state_code desc
        limit 1
      ) as latest
    ), eligible_keys as materialized (
      select latest.id,
        latest.version
      from latest_keys as latest
      left join matched_versions as latest_match
        on latest_match.id = latest.id
       and latest_match.version = latest.version
      where latest.exact_id
         or latest_match.id is not null
    )
    select projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from eligible_keys
    join private.portal_catalog_search_current_v2 as projection
      on projection.dataset_kind = 'process'
     and projection.id = eligible_keys.id
     and projection.version = eligible_keys.version;
    return;
  end if;

  if p_kind = 'process' then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version
      from private.catalog_portal_process_pattern_versions_v1(
        p_like_pattern
      ) as pattern
    ), candidate_ids as materialized (
      select distinct matched.id
      from matched
    ), matched_versions as materialized (
      select distinct matched.id,
        matched.version
      from matched
    ), latest_keys as materialized (
      select latest.id,
        latest.version
      from candidate_ids
      cross join lateral (
        select projection.id,
          projection.version
        from private.portal_catalog_search_current_v2 as projection
        where projection.dataset_kind = 'process'
          and projection.id = candidate_ids.id
        order by projection.version desc,
          projection.modified_at desc,
          projection.state_code desc
        limit 1
      ) as latest
    ), eligible_keys as materialized (
      select latest.id,
        latest.version
      from latest_keys as latest
      join matched_versions as latest_match
        on latest_match.id = latest.id
       and latest_match.version = latest.version
    )
    select projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from eligible_keys
    join private.portal_catalog_search_current_v2 as projection
      on projection.dataset_kind = 'process'
     and projection.id = eligible_keys.id
     and projection.version = eligible_keys.version;
    return;
  end if;

  if p_kind = 'flow' and p_query = '' then
    return query
    select distinct on (projection.id)
      projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from private.portal_catalog_search_current_v2 as projection
    where projection.dataset_kind = 'flow'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc;
    return;
  end if;

  if p_kind = 'flow'
     and private.portal_catalog_summary_valid_cas_v1(p_query) then
    return query
    with candidate_ids as materialized (
      select distinct projection.id
      from private.portal_catalog_search_current_v2 as projection
      where projection.dataset_kind = 'flow'
        and pg_catalog.jsonb_typeof(
          projection.card -> 'casNumber'
        ) = 'string'
        and projection.card ->> 'casNumber' ~
          '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
        and pg_catalog.length(
          projection.card ->> 'casNumber'
        ) between 7 and 12
        and projection.card ->> 'casNumber' = p_query
    ), latest_rows as materialized (
      select latest.id,
        latest.version,
        latest.card,
        latest.state_code,
        latest.modified_at
      from candidate_ids
      cross join lateral (
        select projection.id,
          projection.version,
          projection.card,
          projection.state_code,
          projection.modified_at
        from private.portal_catalog_search_current_v2 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = candidate_ids.id
        order by projection.version desc,
          projection.modified_at desc,
          projection.state_code desc
        limit 1
      ) as latest
      where pg_catalog.jsonb_typeof(
          latest.card -> 'casNumber'
        ) = 'string'
        and latest.card ->> 'casNumber' = p_query
    )
    select latest.id,
      latest.version,
      latest.card,
      latest.state_code,
      latest.modified_at
    from latest_rows as latest;
    return;
  end if;

  if p_kind = 'flow' and p_exact_id is not null then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version,
        false as exact_id
      from private.catalog_portal_flow_pattern_versions_v1(
        p_like_pattern
      ) as pattern
      union
      select projection.id,
        projection.version,
        true
      from private.portal_catalog_search_current_v2 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = p_exact_id
    ), candidate_ids as materialized (
      select matched.id,
        pg_catalog.bool_or(matched.exact_id) as exact_id
      from matched
      group by matched.id
    ), matched_versions as materialized (
      select distinct matched.id,
        matched.version
      from matched
    ), latest_keys as materialized (
      select latest.id,
        latest.version,
        candidate_ids.exact_id
      from candidate_ids
      cross join lateral (
        select projection.id,
          projection.version
        from private.portal_catalog_search_current_v2 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = candidate_ids.id
        order by projection.version desc,
          projection.modified_at desc,
          projection.state_code desc
        limit 1
      ) as latest
    ), eligible_keys as materialized (
      select latest.id,
        latest.version
      from latest_keys as latest
      left join matched_versions as latest_match
        on latest_match.id = latest.id
       and latest_match.version = latest.version
      where latest.exact_id
         or latest_match.id is not null
    )
    select projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from eligible_keys
    join private.portal_catalog_search_current_v2 as projection
      on projection.dataset_kind = 'flow'
     and projection.id = eligible_keys.id
     and projection.version = eligible_keys.version;
    return;
  end if;

  if p_kind = 'flow' then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version
      from private.catalog_portal_flow_pattern_versions_v1(
        p_like_pattern
      ) as pattern
    ), candidate_ids as materialized (
      select distinct matched.id
      from matched
    ), matched_versions as materialized (
      select distinct matched.id,
        matched.version
      from matched
    ), latest_keys as materialized (
      select latest.id,
        latest.version
      from candidate_ids
      cross join lateral (
        select projection.id,
          projection.version
        from private.portal_catalog_search_current_v2 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = candidate_ids.id
        order by projection.version desc,
          projection.modified_at desc,
          projection.state_code desc
        limit 1
      ) as latest
    ), eligible_keys as materialized (
      select latest.id,
        latest.version
      from latest_keys as latest
      join matched_versions as latest_match
        on latest_match.id = latest.id
       and latest_match.version = latest.version
    )
    select projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from eligible_keys
    join private.portal_catalog_search_current_v2 as projection
      on projection.dataset_kind = 'flow'
     and projection.id = eligible_keys.id
     and projection.version = eligible_keys.version;
  end if;
end
$function$
;
alter function private.catalog_portal_candidate_rows_v1(text,text,uuid,text) owner to portal_public_executor;
revoke all on function private.catalog_portal_candidate_rows_v1(text,text,uuid,text) from public;
grant execute on function private.catalog_portal_candidate_rows_v1(text,text,uuid,text) to api_internal_executor;

CREATE OR REPLACE FUNCTION private.catalog_portal_candidate_rows_v2(p_kind text, p_query text, p_exact_id uuid, p_like_pattern text)
 RETURNS TABLE(id uuid, version text, card jsonb, state_code integer, modified_at timestamp with time zone)
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
begin
  if p_kind not in ('process','flow') or p_kind is null then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_query = '' then
    return query
    select p.id, p.version, p.card, p.state_code, p.modified_at
    from private.portal_catalog_search_current_v2 as p
    where p.dataset_kind = p_kind and p.state_code in (100,200);
    return;
  end if;
  if p_kind = 'flow' and private.portal_catalog_summary_valid_cas_v1(p_query) then
    return query
    select p.id, p.version, p.card, p.state_code, p.modified_at
    from private.portal_catalog_search_current_v2 as p
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
    from private.portal_catalog_search_current_v2 as p
    where p.dataset_kind = p_kind and p.id = p_exact_id and p.state_code in (100,200)
  )
  select p.id, p.version, p.card, p.state_code, p.modified_at
  from matched
  join private.portal_catalog_search_current_v2 as p
    on p.dataset_kind = p_kind and p.id = matched.id and p.version = matched.version
  where p.state_code in (100,200);
end;
$function$
;
alter function private.catalog_portal_candidate_rows_v2(text,text,uuid,text) owner to portal_public_executor;
revoke all on function private.catalog_portal_candidate_rows_v2(text,text,uuid,text) from public;
grant execute on function private.catalog_portal_candidate_rows_v2(text,text,uuid,text) to api_internal_executor;

CREATE OR REPLACE FUNCTION private.catalog_portal_process_pattern_versions_v1(p_like_pattern text)
 RETURNS TABLE(id uuid, version text)
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
declare
  v_literal text;
begin
  if pg_catalog.char_length(p_like_pattern) = 3
     and pg_catalog.left(p_like_pattern, 1) = '%'
     and pg_catalog.right(p_like_pattern, 1) = '%' then
    v_literal := pg_catalog.substr(p_like_pattern, 2, 1);
    return query
    select candidate.id,
      candidate.version
    from private.catalog_portal_process_single_character_versions_v1(
      v_literal
    ) as candidate;
    return;
  end if;

  return query execute pg_catalog.format($sql$
    select projection.id,
      projection.version
    from private.portal_catalog_search_current_v2 as projection
    where projection.dataset_kind = 'process'
      and projection.document like %L escape E'\\'
  $sql$, p_like_pattern);
end
$function$
;
alter function private.catalog_portal_process_pattern_versions_v1(text) owner to portal_public_executor;
revoke all on function private.catalog_portal_process_pattern_versions_v1(text) from public;

CREATE OR REPLACE FUNCTION private.catalog_portal_process_single_character_versions_v1(p_literal text)
 RETURNS TABLE(id uuid, version text)
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET enable_indexscan TO 'off'
 SET enable_indexonlyscan TO 'off'
 SET enable_bitmapscan TO 'off'
 SET max_parallel_workers_per_gather TO '4'
 SET min_parallel_table_scan_size TO '0'
 SET parallel_setup_cost TO '0'
 SET parallel_tuple_cost TO '0'
 SET row_security TO 'on'
AS $function$
  select projection.id,
    projection.version
  from private.portal_catalog_search_current_v2 as projection
  where projection.dataset_kind = 'process'
    and pg_catalog.strpos(projection.document, p_literal) > 0
$function$
;
alter function private.catalog_portal_process_single_character_versions_v1(text) owner to portal_public_executor;
revoke all on function private.catalog_portal_process_single_character_versions_v1(text) from public;

CREATE OR REPLACE FUNCTION private.catalog_portal_search_v1_impl(p_kind text, p_query text, p_filters jsonb, p_sort text, p_cursor_rank text, p_cursor_id uuid, p_cursor_version text, p_limit integer, p_query_fingerprint text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
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
  -- catalog.  Order/latest/cursor reduction happens before at most limit+1
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
      from private.catalog_portal_candidate_rows_v1(
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

  -- Geography-only Flow browse can use the synchronized narrow facet child
  -- for latest/filter/order/limit, then hydrate only limit+1 stored cards.
  -- This preserves latest-version and cursor semantics without evaluating
  -- the wide card-facts helper over the full Flow card set.
  if p_kind = 'flow'
     and p_query = ''
     and p_sort = 'relevance'
     and p_filters ? 'geography'
     and (select count(*) from pg_catalog.jsonb_object_keys(p_filters)) = 1 then
    perform private.assert_portal_catalog_facet_contract_v1();

    with portal_latest_facts as materialized (
      select distinct on (facet.id)
        facet.id,
        facet.version,
        facet.state_code,
        facet.modified_at,
        facet.facet_geography
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'flow'
      order by facet.id,
        facet.version desc,
        facet.modified_at desc,
        facet.state_code desc
    ), portal_filtered_keys as materialized (
      select portal_latest_facts.*
      from portal_latest_facts
      where portal_latest_facts.facet_geography =
          p_filters ->> 'geography'
        and (
          p_cursor_rank is null
          or 0::numeric < p_cursor_rank::numeric
          or (
            0::numeric = p_cursor_rank::numeric
            and (
              portal_latest_facts.id > p_cursor_id
              or (
                portal_latest_facts.id = p_cursor_id
                and portal_latest_facts.version < p_cursor_version
              )
            )
          )
        )
    ), portal_ordered_keys as materialized (
      select portal_filtered_keys.*,
        pg_catalog.row_number() over (
          order by portal_filtered_keys.id,
            portal_filtered_keys.version desc
        ) as page_rank
      from portal_filtered_keys
      order by portal_filtered_keys.id,
        portal_filtered_keys.version desc
      limit p_limit + 1
    ), portal_hydrated as materialized (
      select portal_ordered_keys.*,
        projection.card
      from portal_ordered_keys
      join private.portal_catalog_search_current_v2 as projection
        on projection.dataset_kind = 'flow'
       and projection.id = portal_ordered_keys.id
       and projection.version = portal_ordered_keys.version
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
            'kind', 'lexical',
            'score', 0::numeric,
            'reasonCodes', '[]'::jsonb
          )
        ) order by portal_hydrated.page_rank
      ) filter (where portal_hydrated.page_rank <= p_limit), '[]'::jsonb),
      case when max(portal_hydrated.page_rank) > p_limit then
        (pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'v', 1,
          'fp', p_query_fingerprint,
          'rankKey', '0',
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
  end if;

  with portal_prefilter as materialized (
    select p_kind as dataset_kind,
      candidate.*
    from private.catalog_portal_candidate_rows_v1(
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
$function$
;
alter function private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text) owner to api_internal_executor;
revoke all on function private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text) from public;
grant execute on function private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text) to portal_public_executor;

CREATE OR REPLACE FUNCTION private.catalog_portal_single_character_search_v1_impl(p_kind text, p_query text, p_cursor_rank text, p_cursor_id uuid, p_cursor_version text, p_limit integer, p_query_fingerprint text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
 SET work_mem TO '32MB'
 SET plan_cache_mode TO 'force_custom_plan'
 SET jit TO 'off'
 SET row_security TO 'on'
AS $function$
declare
  v_items jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.assert_portal_catalog_character_contract_cn1();

  if p_kind not in ('process', 'flow')
     or pg_catalog.char_length(p_query) <> 1
     or p_limit not between 1 and 50 then
    raise exception 'invalid Portal character Search'
      using errcode = '22023';
  end if;

  with latest as materialized (
    select distinct on (character_row.id)
      character_row.id,
      character_row.version,
      character_row.state_code,
      character_row.modified_at,
      character_row.document_characters,
      character_row.name_characters,
      character_row.name_exact_characters,
      character_row.classification_characters,
      character_row.classification_exact_characters
    from private.portal_catalog_character_current_v2 as character_row
    where character_row.dataset_kind = p_kind
    order by character_row.id,
      character_row.version desc,
      character_row.modified_at desc,
      character_row.state_code desc
  ), scored as materialized (
    select latest.*,
      case
        when pg_catalog.strpos(
          latest.name_exact_characters, p_query
        ) > 0 then 0.95::numeric
        when pg_catalog.strpos(
          latest.classification_exact_characters, p_query
        ) > 0 then 0.92::numeric
        else 0.70::numeric
      end as score,
      case
        when pg_catalog.strpos(latest.name_characters, p_query) > 0
          then pg_catalog.jsonb_build_array('name')
        when pg_catalog.strpos(
          latest.classification_characters, p_query
        ) > 0 then pg_catalog.jsonb_build_array('classification')
        else pg_catalog.jsonb_build_array('full_text')
      end as reason_codes
    from latest
    where pg_catalog.strpos(latest.document_characters, p_query) > 0
  ), after_cursor as materialized (
    select scored.*
    from scored
    where p_cursor_rank is null
      or scored.score < p_cursor_rank::numeric
      or (
        scored.score = p_cursor_rank::numeric
        and (
          scored.id > p_cursor_id
          or (
            scored.id = p_cursor_id
            and scored.version < p_cursor_version
          )
        )
      )
  ), ordered as materialized (
    select after_cursor.*,
      pg_catalog.row_number() over (
        order by after_cursor.score desc,
          after_cursor.id asc,
          after_cursor.version desc
      ) as page_rank
    from after_cursor
    order by after_cursor.score desc,
      after_cursor.id asc,
      after_cursor.version desc
    limit p_limit + 1
  ), hydrated as materialized (
    select ordered.*,
      projection.card
    from ordered
    join private.portal_catalog_search_current_v2 as projection
      on projection.dataset_kind = p_kind
     and projection.id = ordered.id
     and projection.version = ordered.version
  )
  select coalesce(
      pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', p_kind,
            'id', hydrated.id::text,
            'version', hydrated.version
          ),
          'accessLevel', hydrated.card -> 'accessLevel',
          'capabilities', hydrated.card -> 'capabilities',
          'names', hydrated.card -> 'names',
          'summary', hydrated.card -> 'summary',
          'geography', hydrated.card -> 'geography',
          'referenceYear', hydrated.card -> 'referenceYear',
          'modifiedAt', pg_catalog.to_char(
            hydrated.modified_at at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          ),
          'match', pg_catalog.jsonb_build_object(
            'kind', 'lexical',
            'score', hydrated.score,
            'reasonCodes', hydrated.reason_codes
          )
        )
        order by hydrated.page_rank
      ) filter (where hydrated.page_rank <= p_limit),
      '[]'::jsonb
    ),
    case
      when pg_catalog.max(hydrated.page_rank) > p_limit then
        (
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'v', 1,
              'fp', p_query_fingerprint,
              'rankKey', hydrated.score::text,
              'kind', p_kind,
              'id', hydrated.id::text,
              'version', hydrated.version
            )
            order by hydrated.page_rank
          ) filter (where hydrated.page_rank = p_limit)
        ) -> 0
      else null
    end
  into v_items, v_next_cursor_payload
  from hydrated;

  return pg_catalog.jsonb_build_object(
    'items', v_items,
    'nextCursorPayload', v_next_cursor_payload
  );
end
$function$
;
alter function private.catalog_portal_single_character_search_v1_impl(text,text,text,uuid,text,integer,text) owner to portal_public_executor;
revoke all on function private.catalog_portal_single_character_search_v1_impl(text,text,text,uuid,text,integer,text) from public;

CREATE OR REPLACE FUNCTION private.portal_dataset_metadata_v1(p_kind text, p_state_code integer, p_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED
 SET search_path TO ''
AS $function$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_location_code text;
  v_cas text;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information #> '{geography,locationOfOperationSupplyOrProduction}';
    v_location_code := nullif(
      private.portal_scalar_text_v1(v_location -> '@location'),
      ''
    );
    return jsonb_build_object(
      'kind', 'process',
      'names', private.portal_process_names_v1(p_json),
      'generalComment', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:generalComment}'),
      'referenceProduct', private.portal_process_reference_product_v1(p_json),
      'functionalUnit', private.portal_process_functional_unit_v1(p_state_code, p_json),
      'classifications', private.portal_classifications_v1(v_information #> '{dataSetInformation,classificationInformation}'),
      'geography', jsonb_build_object(
        'code', v_location_code,
        'label', private.portal_localized_text_v1(v_location -> 'descriptionOfRestrictions'),
        'precision', private.portal_geography_precision_v1(v_location_code)
      ),
      'referenceYear', private.portal_safe_year_v1(v_information #>> '{time,common:referenceYear}'),
      'validUntilYear', private.portal_safe_year_v1(v_information #>> '{time,common:dataSetValidUntil}'),
      'technology',
        private.portal_localized_text_v1(
          v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
        ) || private.portal_localized_text_v1(
          v_information #> '{technology,technologicalApplicability}'
        ),
      'dataSetType', nullif(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
      ), ''),
      'allocationAndModeling',
        private.portal_localized_text_v1(
          v_modelling #> '{LCIMethodAndAllocation,deviationsFromLCIMethodPrinciple}'
        ) || private.portal_localized_text_v1(
          v_modelling #> '{LCIMethodAndAllocation,deviationsFromModellingConstants}'
        ),
      'cutoffRules', private.portal_localized_text_v1(
        v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,deviationsFromCutOffAndCompletenessPrinciples}'
      ),
      'quality', jsonb_build_object(
        'reviewStatus', (
          select nullif(private.portal_scalar_text_v1(review_item -> '@type'), '')
          from private.portal_json_items_v1(v_modelling #> '{validation,review}') as review_item
          limit 1
        ),
        'timeRepresentativeness', private.portal_first_text_v1(
          v_information #> '{time,common:timeRepresentativenessDescription}'
        ),
        'geographyRepresentativeness', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,geographicalRepresentativenessDescription}'
        ),
        'technologyRepresentativeness', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,technologicalRepresentativenessDescription}'
        ),
        'completeness', private.portal_first_text_v1(
          v_modelling #> '{completeness,completenessOtherProblemField}'
        ),
        'uncertainty', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,uncertaintyAdjustments}'
        )
      ),
      'source', private.portal_source_v1('process', p_json),
      'compliance', private.portal_compliance_v1('process', p_json),
      'administration', private.portal_administration_v1('process', p_json)
    );
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_modelling := p_json #> '{flowDataSet,modellingAndValidation}';
    v_location := v_information -> 'geography';
    v_location_code := case jsonb_typeof(v_location -> 'locationOfSupply')
      when 'string' then nullif(
        private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
        ''
      )
      when 'object' then nullif(
        private.portal_scalar_text_v1(v_location #> '{locationOfSupply,@location}'),
        ''
      )
      else null
    end;
    v_cas := nullif(btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    return jsonb_build_object(
      'kind', 'flow',
      'names', private.portal_localized_text_v1(v_information #> '{dataSetInformation,name,baseName}'),
      'synonyms', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:synonyms}'),
      'generalComment', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:generalComment}'),
      'casNumber', v_cas,
      'flowType', private.portal_flow_kind_v1(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethod,typeOfDataSet}'
      )),
      'classifications', private.portal_classifications_v1(v_information #> '{dataSetInformation,classificationInformation}'),
      'locationOfSupply', jsonb_build_object(
        'code', v_location_code,
        'label', private.portal_localized_text_v1(v_location #> '{locationOfSupply,descriptionOfRestrictions}')
      ),
      'referenceFlowProperty', private.portal_reference_flowproperty_v1(p_json),
      'source', private.portal_source_v1('flow', p_json),
      'compliance', private.portal_compliance_v1('flow', p_json),
      'administration', private.portal_administration_v1('flow', p_json)
    );
  end if;
  return null;
end
$function$
;
alter function private.portal_dataset_metadata_v1(text,integer,jsonb) owner to portal_public_executor;
revoke all on function private.portal_dataset_metadata_v1(text,integer,jsonb) from public;

CREATE OR REPLACE FUNCTION private.portal_projection_hybrid_candidates_v2(p_kind text, p_query_terms text[], p_query_embedding vector, p_filters jsonb)
 RETURNS TABLE(id uuid, version text, lexical_rank integer, semantic_rank integer, semantic_distance double precision, score numeric)
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
  with lexical_counts as materialized (
    select match.id, match.version, pg_catalog.count(distinct match.term_ordinal)::integer as hit_count
    from private.catalog_portal_hybrid_pattern_matches_v1(p_kind,p_query_terms) as match
    join private.portal_catalog_search_current_v2 as projection
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
$function$
;
alter function private.portal_projection_hybrid_candidates_v2(text,text[],vector,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_hybrid_candidates_v2(text,text[],vector,jsonb) from public;
grant execute on function private.portal_projection_hybrid_candidates_v2(text,text[],vector,jsonb) to portal_public_executor;

CREATE OR REPLACE FUNCTION private.portal_projection_hybrid_search_v1_impl(p_kind text, p_query_terms text[], p_query_embedding vector, p_filters jsonb, p_limit integer, p_query_fingerprint text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET "hnsw.iterative_scan" TO 'strict_order'
 SET row_security TO 'on'
AS $function$
declare
  v_items jsonb;
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();

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
    from private.portal_catalog_search_current_v2 as projection
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
    join private.portal_catalog_search_current_v2 as projection
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
$function$
;
alter function private.portal_projection_hybrid_search_v1_impl(text,text[],vector,jsonb,integer,text) owner to api_internal_executor;
revoke all on function private.portal_projection_hybrid_search_v1_impl(text,text[],vector,jsonb,integer,text) from public;
grant execute on function private.portal_projection_hybrid_search_v1_impl(text,text[],vector,jsonb,integer,text) to portal_public_executor;

CREATE OR REPLACE FUNCTION private.portal_projection_hybrid_search_v2_impl(p_kind text, p_query_terms text[], p_query_embedding vector, p_filters jsonb, p_limit integer, p_query_fingerprint text, p_cursor jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
declare
  v_items jsonb;
  v_next jsonb;
  v_count integer;
  v_dataset_count integer;
  v_groups jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();
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
    join private.portal_catalog_search_current_v2 as projection
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
$function$
;
alter function private.portal_projection_hybrid_search_v2_impl(text,text[],vector,jsonb,integer,text,jsonb) owner to api_internal_executor;
revoke all on function private.portal_projection_hybrid_search_v2_impl(text,text[],vector,jsonb,integer,text,jsonb) from public;
grant execute on function private.portal_projection_hybrid_search_v2_impl(text,text[],vector,jsonb,integer,text,jsonb) to portal_public_executor;

CREATE OR REPLACE FUNCTION private.portal_projection_semantic_process_v1(p_query_embedding vector)
 RETURNS TABLE(id uuid, version text, semantic_distance double precision)
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET "hnsw.iterative_scan" TO 'relaxed_order'
 SET "hnsw.ef_search" TO '1000'
 SET "hnsw.max_scan_tuples" TO '200000'
 SET "hnsw.scan_mem_multiplier" TO '4'
 SET enable_sort TO 'off'
 SET jit TO 'off'
 SET row_security TO 'on'
AS $function$
declare
  v_ids uuid[];
  v_versions text[];
  v_distances double precision[];
  v_source_ids uuid[];
  v_source_versions text[];
  v_source_distances double precision[];
  v_source_rows integer;
begin
  if p_query_embedding is null then
    raise exception using
      errcode = '22023',
      message = 'invalid portal semantic query';
  end if;

  select pg_catalog.array_agg(
      candidate.id
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.version
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.semantic_distance
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    )
  into v_ids, v_versions, v_distances
  from (
    select approximate.id,
      approximate.version,
      approximate.semantic_distance
    from (
      select process.id,
        process.version::text as version,
        process.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from public.processes as process
      where process.state_code in (100, 200)
        and process.embedding_ft is not null
        and exists (
          select 1
          from private.portal_catalog_search_current_v2 as projection
          where projection.dataset_kind = 'process'
            and projection.id = process.id
            and projection.version = process.version::text
            and not exists (
              select 1
              from private.portal_catalog_search_current_v2 as newer
              where newer.dataset_kind = projection.dataset_kind
                and newer.id = projection.id
                and (
                  newer.version > projection.version
                  or (
                    newer.version = projection.version
                    and newer.modified_at > projection.modified_at
                  )
                  or (
                    newer.version = projection.version
                    and newer.modified_at = projection.modified_at
                    and newer.state_code > projection.state_code
                  )
                )
            )
        )
      order by process.embedding_ft
        operator(extensions.<=>) p_query_embedding
      limit 5000
    ) as approximate
    where approximate.semantic_distance is not null
      and approximate.semantic_distance >= 0::double precision
    order by approximate.semantic_distance + 0::double precision,
      approximate.id,
      approximate.version desc
    limit 200
  ) as candidate;

  if coalesce(pg_catalog.cardinality(v_ids), 0) >= 200 then
    return query
    select v_ids[candidate.ordinal],
      v_versions[candidate.ordinal],
      v_distances[candidate.ordinal]
    from pg_catalog.generate_subscripts(v_ids, 1)
      as candidate(ordinal)
    where v_distances[candidate.ordinal] <= 0.5::double precision
    order by candidate.ordinal;
    return;
  end if;

  select pg_catalog.array_agg(
      bounded_source.id order by bounded_source.id, bounded_source.version desc
    ),
    pg_catalog.array_agg(
      bounded_source.version
      order by bounded_source.id, bounded_source.version desc
    ),
    pg_catalog.array_agg(
      bounded_source.semantic_distance
      order by bounded_source.id, bounded_source.version desc
    )
  into v_source_ids, v_source_versions, v_source_distances
  from (
    select process.id,
      process.version::text as version,
      process.embedding_ft operator(extensions.<=>) p_query_embedding
        as semantic_distance
    from public.processes as process
    where process.state_code in (100, 200)
      and process.embedding_ft is not null
    limit 200
  ) as bounded_source;

  v_source_rows := coalesce(pg_catalog.cardinality(v_source_ids), 0);

  if v_source_rows < 200 then
    return query
    select v_source_ids[source.ordinal],
      v_source_versions[source.ordinal],
      v_source_distances[source.ordinal]
    from pg_catalog.generate_subscripts(v_source_ids, 1)
      as source(ordinal)
    where v_source_distances[source.ordinal] is not null
      and v_source_distances[source.ordinal] >= 0::double precision
      and v_source_distances[source.ordinal] <= 0.5::double precision
      and exists (
        select 1
        from private.portal_catalog_search_current_v2 as projection
        where projection.dataset_kind = 'process'
          and projection.id = v_source_ids[source.ordinal]
          and projection.version = v_source_versions[source.ordinal]
          and not exists (
            select 1
            from private.portal_catalog_search_current_v2 as newer
            where newer.dataset_kind = projection.dataset_kind
              and newer.id = projection.id
              and (
                newer.version > projection.version
                or (
                  newer.version = projection.version
                  and newer.modified_at > projection.modified_at
                )
                or (
                  newer.version = projection.version
                  and newer.modified_at = projection.modified_at
                  and newer.state_code > projection.state_code
                )
              )
          )
        offset 0
      )
    order by v_source_distances[source.ordinal],
      v_source_ids[source.ordinal],
      v_source_versions[source.ordinal] desc;
    return;
  end if;

  return query
  select exact.*
  from private.portal_projection_semantic_process_exact_cn1(
    p_query_embedding
  ) as exact;
  return;
end
$function$
;
alter function private.portal_projection_semantic_process_v1(vector) owner to api_internal_executor;
revoke all on function private.portal_projection_semantic_process_v1(vector) from public;

CREATE OR REPLACE FUNCTION private.portal_projection_semantic_process_v2(p_query_embedding vector, p_filters jsonb)
 RETURNS TABLE(id uuid, version text, semantic_distance double precision)
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET "hnsw.iterative_scan" TO 'strict_order'
 SET "hnsw.ef_search" TO '200'
 SET "hnsw.max_scan_tuples" TO '20000'
 SET "hnsw.scan_mem_multiplier" TO '2'
 SET jit TO 'off'
 SET row_security TO 'on'
AS $function$
declare
  v_exact_cutover constant integer := 2000;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer;
  v_indexed_probe boolean := false;
begin
  if p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024 then
    raise exception using
      errcode = '22023',
      message = 'invalid portal request';
  end if;

  -- Geography and access level are exact, normalized facts in the
  -- transactionally synchronized facet child.  Additional filters remain a
  -- final canonical card recheck, so this key set is a safe candidate
  -- superset for combined filter requests.
  if (p_filters ? 'geography') and (p_filters ? 'accessLevel') then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_geography = p_filters ->> 'geography'
        and facet.facet_access_level = p_filters ->> 'accessLevel'
      limit v_exact_cutover + 1
    ) as candidate;
  elsif p_filters ? 'geography' then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_geography = p_filters ->> 'geography'
      limit v_exact_cutover + 1
    ) as candidate;
  elsif p_filters ? 'accessLevel' then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_access_level = p_filters ->> 'accessLevel'
      limit v_exact_cutover + 1
    ) as candidate;
  end if;

  v_candidate_count := coalesce(
    pg_catalog.cardinality(v_candidate_ids),
    0
  );

  if v_indexed_probe
     and v_candidate_count <= v_exact_cutover then
    return query
    with candidate_keys as materialized (
      select
        v_candidate_ids[key.ordinal] as id,
        v_candidate_versions[key.ordinal] as version
      from pg_catalog.generate_subscripts(
        v_candidate_ids,
        1
      ) as key(ordinal)
    ), nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) p_query_embedding
          as distance
      from candidate_keys as candidate
      join public.processes as source
        on source.id = candidate.id
       and source.version::text = candidate.version
      join private.portal_catalog_search_current_v2 as projection
        on projection.dataset_kind = 'process'
       and projection.id = candidate.id
       and projection.version = candidate.version
      where source.state_code in (100, 200)
        and source.embedding_ft is not null
        and projection.state_code in (100, 200)
        and private.portal_card_matches_filters_v2(
          projection.card,
          p_filters
        )
      -- The no-op addition deliberately prevents the global HNSW index from
      -- satisfying this ORDER BY.  Only the bounded exact-key rows are scored.
      order by
        (
          source.embedding_ft operator(extensions.<=>) p_query_embedding
        ) + 0::double precision,
        source.id,
        source.version::text desc
      limit 200
    )
    select nearest.id, nearest.version, nearest.distance
    from nearest
    where nearest.distance >= 0::double precision
      and nearest.distance <= 0.5::double precision
    order by
      nearest.distance + 0::double precision,
      nearest.id,
      nearest.version desc;
    return;
  end if;

  -- Unfiltered, unsupported-filter-only, and broad indexed-filter requests
  -- retain the predecessor HNSW path byte-for-byte.
  return query
  with nearest as materialized (
    select source.id, source.version::text as version,
      source.embedding_ft operator(extensions.<=>) p_query_embedding as distance
    from public.processes as source
    where source.state_code in (100,200)
      and source.embedding_ft is not null
      and exists (
        select 1 from private.portal_catalog_search_current_v2 as projection
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
$function$
;
alter function private.portal_projection_semantic_process_v2(vector,jsonb) owner to portal_public_executor;
revoke all on function private.portal_projection_semantic_process_v2(vector,jsonb) from public;
grant execute on function private.portal_projection_semantic_process_v2(vector,jsonb) to api_internal_executor;

CREATE OR REPLACE FUNCTION private.portal_public_hybrid_card_v1(p_kind text, p_state_code integer, p_json jsonb)
 RETURNS jsonb
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
AS $function$
  select private.portal_catalog_card_cn1(p_kind, p_state_code, p_json)
$function$
;
alter function private.portal_public_hybrid_card_v1(text,integer,jsonb) owner to portal_public_executor;
revoke all on function private.portal_public_hybrid_card_v1(text,integer,jsonb) from public;
grant execute on function private.portal_public_hybrid_card_v1(text,integer,jsonb) to api_internal_executor;

CREATE OR REPLACE FUNCTION private.portal_search_v1(p_kind text, p_query text, p_filters jsonb, p_sort text, p_cursor text, p_limit integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED
 SET search_path TO ''
AS $function$
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
  perform private.assert_portal_catalog_projection_contract_cn1();

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
  if p_kind = 'process' then
  v_fingerprint := pg_catalog.encode(extensions.digest(pg_catalog.convert_to('composite-names-v2:' || v_fingerprint, 'UTF8'), 'sha256'), 'hex');
  end if;
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

  if pg_catalog.char_length(v_query) = 1
     and v_filters = '{}'::jsonb
     and v_sort = 'relevance' then
    v_kernel := private.catalog_portal_single_character_search_v1_impl(
      p_kind,
      v_query,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  elsif p_kind = 'process'
     and pg_catalog.char_length(v_query) > 1
     and v_query !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     and v_filters = '{}'::jsonb
     and v_sort = 'relevance' then
    perform private.assert_portal_process_keyword_rank_contract_cn1();
    v_kernel := private.catalog_portal_process_keyword_relevance_cn1_impl(
      v_query,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  else
    v_kernel := private.catalog_portal_search_v1_impl(
      p_kind,
      v_query,
      v_filters,
      v_sort,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  end if;

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
$function$
;
alter function private.portal_search_v1(text,text,jsonb,text,text,integer) owner to portal_public_executor;
revoke all on function private.portal_search_v1(text,text,jsonb,text,text,integer) from public;

CREATE OR REPLACE FUNCTION private.portal_search_v2(p_kind text, p_query text, p_filters jsonb, p_sort text, p_cursor text, p_limit integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED
 SET search_path TO ''
AS $function$
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
  perform private.assert_portal_catalog_projection_contract_cn1();

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
  if p_kind = 'process' then
  v_fingerprint := pg_catalog.encode(extensions.digest(pg_catalog.convert_to('composite-names-v2:' || v_fingerprint, 'UTF8'), 'sha256'), 'hex');
  end if;
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
$function$
;
alter function private.portal_search_v2(text,text,jsonb,text,text,integer) owner to portal_public_executor;
revoke all on function private.portal_search_v2(text,text,jsonb,text,text,integer) from public;

CREATE OR REPLACE FUNCTION api.portal_catalog_summary_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '2s'
 SET work_mem TO '32MB'
 SET plan_cache_mode TO 'force_custom_plan'
 SET max_parallel_workers_per_gather TO '0'
 SET jit TO 'off'
 SET row_security TO 'on'
AS $function$
declare
  v_counts jsonb;
  v_latest_modified_at text;
  v_uuid_example jsonb;
  v_cas_example jsonb;
  v_classification_example jsonb;
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();
  perform private.assert_portal_catalog_facet_contract_v1();

  with latest as materialized (
    select distinct on (facet.dataset_kind, facet.id)
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.modified_at,
      facet.state_code
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.facet_contract_version = 1
    order by facet.dataset_kind,
      facet.id,
      facet.version desc,
      facet.modified_at desc,
      facet.state_code desc
  ), counts as (
    select pg_catalog.jsonb_build_object(
        'process', pg_catalog.count(*) filter (
          where latest.dataset_kind = 'process'
        ),
        'flow', pg_catalog.count(*) filter (
          where latest.dataset_kind = 'flow'
        ),
        'total', pg_catalog.count(*)
      ) as value,
      private.portal_timestamp_v1(
        pg_catalog.max(latest.modified_at)
      ) as latest_modified_at
    from latest
  ), uuid_candidates as materialized (
    (
      select 0 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_current_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      where candidate.dataset_kind = 'process'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc
      limit 1
    )
    union all
    (
      select 1 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_current_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      where candidate.dataset_kind = 'flow'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc
      limit 1
    )
  ), uuid_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'uuid',
      'datasetKind', candidate.dataset_kind,
      'query', candidate.id::text,
      'label', candidate.label
    ) as value
    from uuid_candidates as candidate
    order by candidate.preference
    limit 1
  ), cas_unique_values as materialized (
    select candidate.card ->> 'casNumber' as cas_number,
      pg_catalog.min(candidate.id::text)::uuid as id
    from private.portal_catalog_search_current_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and pg_catalog.jsonb_typeof(candidate.card -> 'casNumber') = 'string'
      and candidate.card ->> 'casNumber' ~
        '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
      and pg_catalog.length(
        candidate.card ->> 'casNumber'
      ) between 7 and 12
      and private.portal_catalog_summary_valid_cas_v1(
        candidate.card ->> 'casNumber'
      )
    group by candidate.card ->> 'casNumber'
    having pg_catalog.count(*) = 1
    order by candidate.card ->> 'casNumber'
    limit 64
  ), cas_candidates as materialized (
    select candidate.dataset_kind,
      candidate.id,
      candidate.version,
      candidate.modified_at,
      candidate.state_code,
      unique_cas.cas_number,
      private.portal_catalog_summary_label_v1(candidate.card) as label
    from cas_unique_values as unique_cas
    join private.portal_catalog_search_current_v2 as candidate
      on candidate.dataset_kind = 'flow'
     and candidate.id = unique_cas.id
     and candidate.card ->> 'casNumber' = unique_cas.cas_number
    join latest
      on latest.dataset_kind = candidate.dataset_kind
     and latest.id = candidate.id
     and latest.version = candidate.version
    where pg_catalog.jsonb_array_length(
      private.portal_catalog_summary_label_v1(candidate.card)
    ) > 0
    order by unique_cas.cas_number,
      candidate.id,
      candidate.version desc,
      candidate.modified_at desc,
      candidate.state_code desc
    limit 1
  ), cas_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'cas',
      'datasetKind', 'flow',
      'query', candidate.cas_number,
      'label', candidate.label
    ) as value
    from cas_candidates as candidate
    order by candidate.id,
      candidate.version desc,
      candidate.modified_at desc,
      candidate.state_code desc
    limit 1
  ), classification_candidates as materialized (
    (
      select 0 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        candidate.modified_at,
        candidate.state_code,
        classification.ordinality,
        pg_catalog.btrim(classification.value ->> 'code') as code,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_current_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      cross join lateral pg_catalog.jsonb_array_elements(
        candidate.card -> 'classifications'
      ) with ordinality as classification(value, ordinality)
      where candidate.dataset_kind = 'process'
        and pg_catalog.jsonb_typeof(
          candidate.card -> 'classifications'
        ) = 'array'
        and pg_catalog.jsonb_array_length(
          candidate.card -> 'classifications'
        ) > 0
        and pg_catalog.jsonb_typeof(classification.value) = 'object'
        and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) between 4 and 128
        and pg_catalog.octet_length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 512
        and pg_catalog.btrim(
          classification.value ->> 'code'
        ) !~ '[[:cntrl:]]'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc,
        classification.ordinality,
        pg_catalog.btrim(
          classification.value ->> 'code'
        ) collate pg_catalog."C"
      limit 1
    )
    union all
    (
      select 1 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        candidate.modified_at,
        candidate.state_code,
        classification.ordinality,
        pg_catalog.btrim(classification.value ->> 'code') as code,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_current_v2 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      cross join lateral pg_catalog.jsonb_array_elements(
        candidate.card -> 'classifications'
      ) with ordinality as classification(value, ordinality)
      where candidate.dataset_kind = 'flow'
        and pg_catalog.jsonb_typeof(
          candidate.card -> 'classifications'
        ) = 'array'
        and pg_catalog.jsonb_array_length(
          candidate.card -> 'classifications'
        ) > 0
        and pg_catalog.jsonb_typeof(classification.value) = 'object'
        and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) between 4 and 128
        and pg_catalog.octet_length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 512
        and pg_catalog.btrim(
          classification.value ->> 'code'
        ) !~ '[[:cntrl:]]'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc,
        classification.ordinality,
        pg_catalog.btrim(
          classification.value ->> 'code'
        ) collate pg_catalog."C"
      limit 1
    )
  ), classification_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'classification',
      'datasetKind', candidate.dataset_kind,
      'query', candidate.code,
      'label', candidate.label
    ) as value
    from classification_candidates as candidate
    order by candidate.preference
    limit 1
  )
  select counts.value,
    counts.latest_modified_at,
    uuid_example.value,
    cas_example.value,
    classification_example.value
  into v_counts,
    v_latest_modified_at,
    v_uuid_example,
    v_cas_example,
    v_classification_example
  from counts
  left join uuid_example on true
  left join cas_example on true
  left join classification_example on true;

  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-catalog-summary.v1',
    'counts', v_counts,
    'latestModifiedAt', v_latest_modified_at,
    'examples', coalesce(pg_catalog.jsonb_agg(
      example.value order by example.ordinality
    ) filter (where example.value is not null), '[]'::jsonb)
  )
  into v_result
  from (values
    (1, v_uuid_example),
    (2, v_cas_example),
    (3, v_classification_example)
  ) as example(ordinality, value);

  if pg_catalog.octet_length(v_result::text) > 16384 then
    raise exception using
      errcode = '54000',
      message = 'Portal catalog summary exceeded its response budget';
  end if;

  return v_result;
exception
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_catalog_summary_v1() owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_get_dataset_v1(p_kind text, p_id uuid, p_version text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_version is null
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return private.portal_lcia_decorate_dataset_v1(
    private.portal_dataset_projection_v1(p_kind, p_id, p_version)
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_get_dataset_v1(text,uuid,text) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_processes_v1(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  return private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_search_v1(
        'process', p_query, p_filters, p_sort, p_cursor, p_limit
      )
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_search_processes_v1(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_flows_v1(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
begin
  return private.portal_decorate_card_context_v1(
    private.portal_search_v1(
      'flow', p_query, p_filters, p_sort, p_cursor, p_limit
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$
;
alter function api.portal_search_flows_v1(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_facets_v1(p_kind text, p_query text, p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_exact_id uuid;
  v_like_pattern text;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();

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

  if v_query = '' and v_filters = '{}'::jsonb then
    perform private.assert_portal_catalog_facet_contract_v1();
    return private.catalog_portal_facets_empty_v1_impl(
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

  return private.catalog_portal_facets_v1_impl(
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
$function$
;
alter function api.portal_facets_v1(text,text,jsonb) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_hybrid_search_v1(p_kind text, p_query_terms text[], p_query_embedding text, p_filters jsonb, p_limit integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
AS $function$
declare
  v_input jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,
    p_query_terms,
    p_query_embedding,
    p_filters,
    p_limit
  );
  v_page := private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_projection_hybrid_search_v1_impl(
        v_input ->> 'kind',
        array(
          select term.value
          from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
            with ordinality as term(value, ordinality)
          order by term.ordinality
        ),
        (v_input ->> 'queryEmbedding')::extensions.vector(1024),
        v_input -> 'filters',
        (v_input ->> 'limit')::integer,
        v_input ->> 'queryFingerprint'
      )
    )
  );
  if v_page is null
     or pg_catalog.octet_length(
       pg_catalog.convert_to(v_page::text, 'UTF8')
     ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end
$function$
;
alter function api.portal_hybrid_search_v1(text,text[],text,jsonb,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_hybrid_search_v2(p_kind text, p_query_terms text[], p_query_embedding text, p_filters jsonb, p_limit integer, p_cursor text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
AS $function$
declare
  v_input jsonb;
  v_fingerprint text;
  v_cursor jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,p_query_terms,p_query_embedding,p_filters,p_limit);
  v_fingerprint := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to('portal-hybrid-rank-v2:' || case when v_input ->> 'kind' = 'process' then 'composite-names-v2:' else '' end || (v_input ->> 'queryFingerprint'),'UTF8'),
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
$function$
;
alter function api.portal_hybrid_search_v2(text,text[],text,jsonb,integer,text) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_processes_v2(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
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
$function$
;
alter function api.portal_search_processes_v2(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_search_flows_v2(p_query text, p_filters jsonb DEFAULT '{}'::jsonb, p_sort text DEFAULT 'relevance'::text, p_cursor text DEFAULT NULL::text, p_limit integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
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
$function$
;
alter function api.portal_search_flows_v2(text,jsonb,text,text,integer) owner to portal_public_executor;

CREATE OR REPLACE FUNCTION api.portal_facets_v2(p_kind text, p_query text, p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
AS $function$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_exact_id uuid;
  v_like_pattern text;
begin
  perform private.assert_portal_catalog_projection_contract_cn1();

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
$function$
;
alter function api.portal_facets_v2(text,text,jsonb) owner to portal_public_executor;


revoke create on schema private from portal_public_executor, api_internal_executor;
revoke portal_public_executor, api_internal_executor from postgres;
commit;
