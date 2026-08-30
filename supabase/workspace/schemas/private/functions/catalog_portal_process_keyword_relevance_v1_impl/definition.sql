CREATE OR REPLACE FUNCTION "private"."catalog_portal_process_keyword_relevance_v1_impl"("p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
  with selected_keys as materialized (
    select selected.id, selected.version, selected.score,
      pg_catalog.row_number() over (
        order by selected.score desc, selected.id, selected.version desc
      ) as page_rank
    from private.catalog_portal_process_keyword_keys_v1(
      p_query,
      p_cursor_rank,
      p_cursor_id,
      p_cursor_version,
      p_limit
    ) as selected
  ), hydrated as materialized (
    select selected.page_rank,
      projection.id,
      projection.version,
      projection.modified_at,
      projection.card,
      selected.score,
      private.catalog_portal_card_facts_v1(
        projection.card,
        '{}'::jsonb,
        p_query
      ) as facts
    from selected_keys as selected
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = 'process'
     and projection.id = selected.id
     and projection.version = selected.version
  ), result as (
    select coalesce(
      pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', 'process',
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
            'kind', case
              when (hydrated.facts ->> 'nameExact')::boolean
                or (hydrated.facts ->> 'nameContains')::boolean
                then 'lexical'
              when (hydrated.facts ->> 'classificationExact')::boolean
                or (hydrated.facts ->> 'classificationContains')::boolean
                then 'identifier'
              else 'lexical'
            end,
            'score', hydrated.score,
            'reasonCodes', case
              when (hydrated.facts ->> 'nameExact')::boolean
                or (hydrated.facts ->> 'nameContains')::boolean
                then pg_catalog.jsonb_build_array('name')
              when (hydrated.facts ->> 'classificationExact')::boolean
                or (hydrated.facts ->> 'classificationContains')::boolean
                then pg_catalog.jsonb_build_array('classification')
              else pg_catalog.jsonb_build_array('full_text')
            end
          )
        ) order by hydrated.page_rank
      ) filter (where hydrated.page_rank <= p_limit),
      '[]'::jsonb
    ) as items,
    case when pg_catalog.max(hydrated.page_rank) > p_limit then
      (
        pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'v', 1,
            'fp', p_query_fingerprint,
            'rankKey', hydrated.score::text,
            'kind', 'process',
            'id', hydrated.id::text,
            'version', hydrated.version
          ) order by hydrated.page_rank
        ) filter (where hydrated.page_rank = p_limit)
      ) -> 0
    else null end as next_cursor_payload
    from hydrated
  )
  select pg_catalog.jsonb_build_object(
    'items', result.items,
    'nextCursorPayload', result.next_cursor_payload
  )
  from result
$$;

ALTER FUNCTION "private"."catalog_portal_process_keyword_relevance_v1_impl"("p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_process_keyword_relevance_v1_impl"("p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") FROM PUBLIC;
