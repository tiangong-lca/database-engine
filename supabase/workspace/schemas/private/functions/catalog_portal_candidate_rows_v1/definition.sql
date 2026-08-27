CREATE OR REPLACE FUNCTION "private"."catalog_portal_candidate_rows_v1"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") RETURNS TABLE("id" "uuid", "version" "text", "card" "jsonb", "state_code" integer, "modified_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $_$
begin
  if p_kind = 'process' and p_query = '' then
    return query
    select distinct on (projection.id)
      projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from private.portal_catalog_search_rows_v1 as projection
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
      from private.portal_catalog_search_rows_v1 as projection
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
        from private.portal_catalog_search_rows_v1 as projection
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
    join private.portal_catalog_search_rows_v1 as projection
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
        from private.portal_catalog_search_rows_v1 as projection
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
    join private.portal_catalog_search_rows_v1 as projection
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
    from private.portal_catalog_search_rows_v1 as projection
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
      from private.portal_catalog_search_rows_v1 as projection
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
        from private.portal_catalog_search_rows_v1 as projection
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
      from private.portal_catalog_search_rows_v1 as projection
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
        from private.portal_catalog_search_rows_v1 as projection
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
    join private.portal_catalog_search_rows_v1 as projection
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
        from private.portal_catalog_search_rows_v1 as projection
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
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = 'flow'
     and projection.id = eligible_keys.id
     and projection.version = eligible_keys.version;
  end if;
end
$_$;

ALTER FUNCTION "private"."catalog_portal_candidate_rows_v1"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_candidate_rows_v1"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."catalog_portal_candidate_rows_v1"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") TO "api_internal_executor";
