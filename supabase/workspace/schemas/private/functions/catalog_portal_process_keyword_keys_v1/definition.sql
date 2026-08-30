CREATE OR REPLACE FUNCTION "private"."catalog_portal_process_keyword_keys_v1"("p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer) RETURNS TABLE("id" "uuid", "version" "text", "score" numeric)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
declare
  v_like_pattern text;
begin
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

  return query
  with matched_versions as materialized (
    select matched.id, matched.version
    from private.catalog_portal_process_pattern_versions_v1(
      v_like_pattern
    ) as matched
  ), candidate_ids as materialized (
    select distinct matched.id
    from matched_versions as matched
  ), latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    join candidate_ids using (id)
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), eligible_keys as materialized (
    select latest.id, latest.version
    from latest_keys as latest
    join matched_versions as matched
      on matched.id = latest.id
     and matched.version = latest.version
  ), exact_source as materialized (
    select projection.id,
      projection.version,
      case
        when private.portal_process_rank_name_keys_v1(projection.card)
          @> array[p_query] then 0.95::numeric
        else 0.92::numeric
      end as score
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and (
        private.portal_process_rank_name_keys_v1(projection.card)
          @> array[p_query]
        or private.portal_process_rank_classification_keys_v1(
          projection.card
        ) @> array[p_query]
      )
  ), exact_keys as materialized (
    select exact_source.*
    from exact_source
    join eligible_keys using (id, version)
    where p_cursor_rank is null
      or exact_source.score < p_cursor_rank::numeric
      or (
        exact_source.score = p_cursor_rank::numeric
        and (
          exact_source.id > p_cursor_id
          or (
            exact_source.id = p_cursor_id
            and exact_source.version < p_cursor_version
          )
        )
      )
  ), general_keys as materialized (
    select eligible.id, eligible.version, 0.70::numeric as score
    from eligible_keys as eligible
    left join exact_source using (id, version)
    where exact_source.id is null
      and (
        p_cursor_rank is null
        or 0.70::numeric < p_cursor_rank::numeric
        or (
          0.70::numeric = p_cursor_rank::numeric
          and (
            eligible.id > p_cursor_id
            or (
              eligible.id = p_cursor_id
              and eligible.version < p_cursor_version
            )
          )
        )
      )
    order by eligible.id, eligible.version desc
    limit p_limit + 1
  ), combined as (
    select exact_keys.* from exact_keys
    union all
    select general_keys.* from general_keys
  )
  select combined.id, combined.version, combined.score
  from combined
  order by combined.score desc, combined.id, combined.version desc
  limit p_limit + 1;
end
$$;

ALTER FUNCTION "private"."catalog_portal_process_keyword_keys_v1"("p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_process_keyword_keys_v1"("p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer) FROM PUBLIC;
