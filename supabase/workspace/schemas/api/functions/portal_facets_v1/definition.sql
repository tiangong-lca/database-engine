CREATE OR REPLACE FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_groups jsonb;
begin
  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := lower(btrim(coalesce(p_kind, '')));
  perform private.portal_validate_search_v1(
    v_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    'relevance',
    1
  );
  v_query := lower(btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_fingerprint := private.portal_query_fingerprint_v1(v_kind, v_query, v_filters, 'relevance');

  with source_rows as materialized (
    select kinds.kind as dataset_kind, source.*
    from (values ('process'::text), ('flow'::text)) as kinds(kind)
    cross join lateral private.portal_catalog_rows_v1(kinds.kind) as source
    where v_kind = 'all' or kinds.kind = v_kind
  ), latest as materialized (
    select candidate.*
    from (
      select source_rows.*,
        row_number() over (
          partition by source_rows.dataset_kind, source_rows.id
          order by source_rows.version desc
        ) as version_rank
      from source_rows
    ) as candidate
    where candidate.version_rank = 1
  ), decorated as materialized (
    select latest.*,
      private.portal_catalog_card_v1(latest.dataset_kind, latest.state_code, latest.json_data) as card
    from latest
  ), matched as materialized (
    select decorated.*
    from decorated
    where (
      v_query = ''
      or lower(decorated.id::text) = v_query
      or lower(coalesce(decorated.card ->> 'casNumber', '')) = v_query
      or strpos(lower(concat_ws(' ', decorated.card ->> 'document', decorated.lexical_text)), v_query) > 0
    )
      and (not (v_filters ? 'accessLevel') or decorated.card ->> 'accessLevel' = v_filters ->> 'accessLevel')
      and (not (v_filters ? 'geography') or lower(btrim(coalesce(decorated.card #>> '{geography,code}', ''))) = v_filters ->> 'geography')
      and (not (v_filters ? 'classification') or exists (
        select 1 from jsonb_array_elements(decorated.card -> 'classifications') as classification(item)
        where lower(btrim(item ->> 'code')) = v_filters ->> 'classification'
      ))
      and (not (v_filters ? 'referenceYearFrom') or (decorated.card ->> 'referenceYear')::integer >= (v_filters ->> 'referenceYearFrom')::integer)
      and (not (v_filters ? 'referenceYearTo') or (decorated.card ->> 'referenceYear')::integer <= (v_filters ->> 'referenceYearTo')::integer)
      and (not (v_filters ? 'processSubtype') or lower(btrim(coalesce(decorated.card ->> 'processSubtype', ''))) = v_filters ->> 'processSubtype')
      and (not (v_filters ? 'source') or lower(btrim(coalesce(decorated.card ->> 'source', ''))) = v_filters ->> 'source')
  ), facet_values as materialized (
    select 'kind'::text as group_id, 1 as group_order,
      matched.dataset_kind as value, matched.dataset_kind as label
    from matched
    union all
    select 'accessLevel', 2,
      matched.card ->> 'accessLevel' as value, matched.card ->> 'accessLevel' as label
    from matched
    union all
    select 'geography', 3,
      lower(btrim(matched.card #>> '{geography,code}')),
      matched.card #>> '{geography,code}'
    from matched
    union all
    select 'referenceYear', 4,
      btrim(matched.card ->> 'referenceYear'),
      btrim(matched.card ->> 'referenceYear')
    from matched
    union all
    select 'processSubtype', 5,
      lower(btrim(matched.card ->> 'processSubtype')),
      matched.card ->> 'processSubtype'
    from matched where matched.dataset_kind = 'process'
    union all
    select 'source', 6,
      lower(btrim(matched.card ->> 'source')),
      matched.card ->> 'source'
    from matched
  ), counts as materialized (
    select group_id, group_order, value, min(value) as label, count(*) as value_count
    from facet_values
    where nullif(btrim(value), '') is not null
      and length(value) <= 128
      and pg_catalog.octet_length(value) <= 512
    group by group_id, group_order, value
  ), ranked_counts as materialized (
    select counts.*,
      row_number() over (
        partition by counts.group_id
        order by counts.value
      ) as value_rank
    from counts
  ), grouped as materialized (
    select ranked_counts.group_id, ranked_counts.group_order,
      jsonb_agg(jsonb_build_object(
        'value', ranked_counts.value,
        'label', jsonb_build_array(jsonb_build_object('language', 'und', 'value', ranked_counts.label)),
        'count', ranked_counts.value_count
      ) order by ranked_counts.value) filter (where ranked_counts.value_rank <= 100) as values_json,
      bool_or(ranked_counts.value_rank > 100) as has_more
    from ranked_counts
    group by ranked_counts.group_id, ranked_counts.group_order
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'id', grouped.group_id,
    'label', jsonb_build_array(
      jsonb_build_object('language', 'en', 'value', case grouped.group_id
        when 'kind' then 'Object type'
        when 'accessLevel' then 'Access level'
        when 'geography' then 'Geography'
        when 'referenceYear' then 'Reference year'
        when 'processSubtype' then 'Process subtype'
        else 'Source'
      end),
      jsonb_build_object('language', 'zh-CN', 'value', case grouped.group_id
        when 'kind' then '对象类型'
        when 'accessLevel' then '访问级别'
        when 'geography' then '地区'
        when 'referenceYear' then '参考年'
        when 'processSubtype' then '过程类型'
        else '数据源'
      end)
    ),
    'values', grouped.values_json,
    'hasMore', grouped.has_more
  ) order by grouped.group_order), '[]'::jsonb)
  into v_groups
  from grouped;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-facets.v1',
    'kind', v_kind,
    'queryFingerprint', v_fingerprint,
    'groups', v_groups
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$$;

ALTER FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "api"."portal_facets_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb") TO "authenticated";
