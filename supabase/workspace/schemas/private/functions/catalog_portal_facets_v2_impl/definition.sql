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
