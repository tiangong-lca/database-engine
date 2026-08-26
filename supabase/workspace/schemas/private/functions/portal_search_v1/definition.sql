CREATE OR REPLACE FUNCTION "private"."portal_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) RETURNS "jsonb"
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
  v_items jsonb;
  v_next_cursor text;
begin
  perform private.portal_validate_search_v1(
    p_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    coalesce(p_sort, 'relevance'),
    v_limit
  );
  v_query := lower(btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_sort := lower(btrim(coalesce(p_sort, 'relevance')));
  v_fingerprint := private.portal_query_fingerprint_v1(p_kind, v_query, v_filters, v_sort);
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'fp' <> v_fingerprint
       or v_cursor ->> 'kind' <> p_kind
       or coalesce(v_cursor ->> 'rankKey', '') = ''
       or coalesce(v_cursor ->> 'id', '') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_rank := v_cursor ->> 'rankKey';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
    v_cursor_version := v_cursor ->> 'version';
    if v_sort = 'relevance'
       and (v_cursor_rank !~ '^(0(\.\d+)?|1(\.0+)?)$') then
      raise exception using errcode = '22023', message = 'invalid portal request';
    elsif v_sort = 'modified_desc'
       and private.portal_datetime_v1(v_cursor_rank) is null then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  with latest as materialized (
    select candidate.*
    from (
      select source.*,
        row_number() over (partition by source.id order by source.version desc) as version_rank
      from private.portal_catalog_rows_v1(p_kind) as source
    ) as candidate
    where candidate.version_rank = 1
  ), decorated as materialized (
    select latest.*,
      private.portal_catalog_card_v1(p_kind, latest.state_code, latest.json_data) as card
    from latest
  ), scored as materialized (
    select decorated.*,
      case
        when nullif(decorated.card #>> '{names,0,value}', '') is not null
          and length(decorated.card #>> '{names,0,value}') <= 500
          and pg_catalog.octet_length(decorated.card #>> '{names,0,value}') <= 2000
          and decorated.card #>> '{names,0,value}' !~ '[[:cntrl:]]'
          then decorated.card #>> '{names,0,value}'
        else '~unnamed:' || decorated.id::text
      end as name_key,
      case
        when v_query = '' then 0::numeric
        when lower(decorated.id::text) = v_query then 1::numeric
        when lower(coalesce(decorated.card ->> 'casNumber', '')) = v_query then 0.98::numeric
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'names') as name(item)
          where lower(btrim(item ->> 'value')) = v_query
        ) then 0.95::numeric
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'classifications') as classification(item)
          where lower(btrim(item ->> 'code')) = v_query
        ) then 0.92::numeric
        when strpos(lower(concat_ws(' ', decorated.card ->> 'document', decorated.lexical_text)), v_query) > 0
          then 0.70::numeric
        else 0::numeric
      end as score,
      case
        when lower(decorated.id::text) = v_query then jsonb_build_array('exact_id')
        when lower(coalesce(decorated.card ->> 'casNumber', '')) = v_query then jsonb_build_array('cas')
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'names') as name(item)
          where lower(btrim(item ->> 'value')) = v_query
             or strpos(lower(item ->> 'value'), v_query) > 0
        ) then jsonb_build_array('name')
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'classifications') as classification(item)
          where lower(btrim(item ->> 'code')) = v_query
             or strpos(lower(item ->> 'code'), v_query) > 0
        ) then jsonb_build_array('classification')
        when v_query <> '' then jsonb_build_array('full_text')
        else '[]'::jsonb
      end as reason_codes
    from decorated
  ), filtered as materialized (
    select scored.*,
      case v_sort
        when 'relevance' then scored.score::text
        when 'modified_desc' then private.portal_timestamp_v1(scored.modified_at)
        else lower(scored.name_key)
      end as rank_key
    from scored
    where (v_query = '' or scored.score > 0)
      and (not (v_filters ? 'accessLevel') or scored.card ->> 'accessLevel' = v_filters ->> 'accessLevel')
      and (not (v_filters ? 'geography') or lower(btrim(coalesce(scored.card #>> '{geography,code}', ''))) = v_filters ->> 'geography')
      and (not (v_filters ? 'classification') or exists (
        select 1 from jsonb_array_elements(scored.card -> 'classifications') as classification(item)
        where lower(btrim(item ->> 'code')) = v_filters ->> 'classification'
      ))
      and (not (v_filters ? 'referenceYearFrom') or (scored.card ->> 'referenceYear')::integer >= (v_filters ->> 'referenceYearFrom')::integer)
      and (not (v_filters ? 'referenceYearTo') or (scored.card ->> 'referenceYear')::integer <= (v_filters ->> 'referenceYearTo')::integer)
      and (not (v_filters ? 'processSubtype') or lower(btrim(coalesce(scored.card ->> 'processSubtype', ''))) = v_filters ->> 'processSubtype')
      and (not (v_filters ? 'source') or lower(btrim(coalesce(scored.card ->> 'source', ''))) = v_filters ->> 'source')
  ), after_cursor as materialized (
    select filtered.*
    from filtered
    where v_cursor is null
      or case v_sort
        when 'relevance' then
          filtered.score < v_cursor_rank::numeric
          or (
            filtered.score = v_cursor_rank::numeric
            and (
              filtered.id > v_cursor_id
              or (filtered.id = v_cursor_id and filtered.version < v_cursor_version)
            )
          )
        when 'modified_desc' then
          filtered.modified_at < v_cursor_rank::timestamptz
          or (
            filtered.modified_at = v_cursor_rank::timestamptz
            and (
              filtered.id > v_cursor_id
              or (filtered.id = v_cursor_id and filtered.version < v_cursor_version)
            )
          )
        else
          lower(filtered.name_key) > lower(v_cursor_rank)
          or (
            lower(filtered.name_key) = lower(v_cursor_rank)
            and (
              filtered.id > v_cursor_id
              or (filtered.id = v_cursor_id and filtered.version < v_cursor_version)
            )
          )
      end
  ), ordered as materialized (
    select after_cursor.*,
      row_number() over (
        order by
          case when v_sort = 'relevance' then after_cursor.score end desc,
          case when v_sort = 'modified_desc' then after_cursor.modified_at end desc,
          case when v_sort = 'name_asc' then lower(after_cursor.name_key) end asc,
          after_cursor.id asc,
          after_cursor.version desc
      ) as page_rank
    from after_cursor
    order by
      case when v_sort = 'relevance' then after_cursor.score end desc,
      case when v_sort = 'modified_desc' then after_cursor.modified_at end desc,
      case when v_sort = 'name_asc' then lower(after_cursor.name_key) end asc,
      after_cursor.id asc,
      after_cursor.version desc
    limit v_limit + 1
  )
  select
    coalesce(jsonb_agg(
      jsonb_build_object(
        'key', jsonb_build_object('kind', p_kind, 'id', ordered.id::text, 'version', ordered.version),
        'accessLevel', ordered.card -> 'accessLevel',
        'capabilities', ordered.card -> 'capabilities',
        'names', ordered.card -> 'names',
        'summary', ordered.card -> 'summary',
        'geography', ordered.card -> 'geography',
        'referenceYear', ordered.card -> 'referenceYear',
        'modifiedAt', private.portal_timestamp_v1(ordered.modified_at),
        'match', jsonb_build_object(
          'kind', case when ordered.reason_codes ?| array['exact_id', 'cas', 'classification'] then 'identifier' else 'lexical' end,
          'score', ordered.score,
          'reasonCodes', ordered.reason_codes
        )
      ) order by ordered.page_rank
    ) filter (where ordered.page_rank <= v_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > v_limit then private.portal_cursor_encode_v1(
      (jsonb_agg(jsonb_build_object(
        'v', 1,
        'fp', v_fingerprint,
        'rankKey', ordered.rank_key,
        'kind', p_kind,
        'id', ordered.id::text,
        'version', ordered.version
      ) order by ordered.page_rank) filter (where ordered.page_rank = v_limit)) -> 0
    ) else null end
  into v_items, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-search-page.v1',
    'kind', p_kind,
    'queryFingerprint', v_fingerprint,
    'items', v_items,
    'nextCursor', v_next_cursor
  );
end
$_$;

ALTER FUNCTION "private"."portal_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;
