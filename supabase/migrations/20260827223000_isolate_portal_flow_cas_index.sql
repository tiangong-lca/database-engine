-- Issue #543 forward repair: the exact-CAS index fixed public Search but its
-- leading CAS expression was also eligible for the summary's unconstrained
-- CAS-example selector. On cold persistent Dev that changed the selector plan
-- enough to exceed its fixed two-second timeout. Add an explicit length-bound
-- discriminator to the exact path/index so the summary retains its original
-- id-ordered eligibility index while exact CAS remains sub-millisecond.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_flow_cas_index_isolation_prerequisite$
declare
  v_definition_sha256 text;
  v_candidate regprocedure :=
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)'::regprocedure;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(v_candidate),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_definition_sha256;

  if v_definition_sha256 <>
       '93d0e2f3d419f4408c02ea4e5d08decfc5d7af802fad8309b0deb28d17750316'
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_summary_valid_cas_v1(text)'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_rows_latest_v1_idx'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_flow_cas_v1_idx'
     ) is null
     or (
       select index_record.indisvalid is not true
         or index_record.indisready is not true
         or pg_catalog.pg_get_expr(
           index_record.indpred,
           index_record.indrelid,
           true
         ) ~ 'length'
         or pg_catalog.pg_get_indexdef(index_record.indexrelid) !~
           'casNumber.*id.*version DESC.*modified_at DESC.*state_code DESC'
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid =
         'private.portal_catalog_search_flow_cas_v1_idx'::regclass
     )
     or (
       select pg_catalog.pg_get_userbyid(routine.proowner) <>
           'portal_public_executor'
         or not routine.prosecdef
         or routine.provolatile <> 's'
         or routine.proparallel <> 'r'
         or pg_catalog.pg_get_function_result(routine.oid) <>
           'TABLE(id uuid, version text, card jsonb, state_code integer, modified_at timestamp with time zone)'
         or coalesce(routine.proconfig, '{}'::text[]) <> array[
           'search_path=""',
           'statement_timeout=8s',
           'plan_cache_mode=force_custom_plan',
           'row_security=on'
         ]::text[]
         or coalesce(routine.proacl::text, '') <>
           '{portal_public_executor=X/portal_public_executor,api_internal_executor=X/portal_public_executor}'
       from pg_catalog.pg_proc as routine
       where routine.oid = v_candidate
     ) then
    raise exception 'Portal Flow CAS index-isolation prerequisite drifted'
      using errcode = '55000';
  end if;
end
$portal_flow_cas_index_isolation_prerequisite$;

drop index private.portal_catalog_search_flow_cas_v1_idx;

create index portal_catalog_search_flow_cas_v1_idx
  on private.portal_catalog_search_rows_v1 (
    ((card ->> 'casNumber')),
    id,
    version desc,
    modified_at desc,
    state_code desc
  )
  where dataset_kind = 'flow'
    and pg_catalog.jsonb_typeof(card -> 'casNumber') = 'string'
    and card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
    and pg_catalog.length(card ->> 'casNumber') between 7 and 12;

comment on index private.portal_catalog_search_flow_cas_v1_idx is
  'Exact public Flow CAS candidate keys with an explicit 7..12 length discriminator that isolates the index from unconstrained summary selection.';

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

CREATE OR REPLACE FUNCTION "private"."catalog_portal_candidate_rows_v1"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") RETURNS TABLE("id" "uuid", "version" "text", "card" "jsonb", "state_code" integer, "modified_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
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
$$;

comment on function private.catalog_portal_candidate_rows_v1(
  text, text, uuid, text
) is
  'Seven static kind/query branches: valid Flow CAS uses length-isolated exact partial-index keys with latest-version recheck; all empty, UUID, and ordinary lexical paths retain their existing behavior.';

reset role;
revoke create on schema private from portal_public_executor;
set role portal_public_executor;

do $verify_portal_flow_cas_search$
declare
  v_candidate regprocedure :=
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)'::regprocedure;
  v_index regclass :=
    'private.portal_catalog_search_flow_cas_v1_idx'::regclass;
  v_summary jsonb;
  v_cas_example jsonb;
  v_search jsonb;
begin
  if (
       select pg_catalog.pg_get_userbyid(routine.proowner) <>
           'portal_public_executor'
         or not routine.prosecdef
         or routine.provolatile <> 's'
         or routine.proparallel <> 'r'
         or coalesce(routine.proconfig, '{}'::text[]) <> array[
           'search_path=""',
           'statement_timeout=8s',
           'plan_cache_mode=force_custom_plan',
           'row_security=on'
         ]::text[]
         or coalesce(routine.proacl::text, '') <>
           '{portal_public_executor=X/portal_public_executor,api_internal_executor=X/portal_public_executor}'
         or routine.prosrc !~ 'portal_catalog_summary_valid_cas_v1'
         or routine.prosrc !~ 'portal_catalog_search_rows_v1'
         or routine.prosrc !~ 'latest_rows as materialized'
         or routine.prosrc !~ 'between 7 and 12'
       from pg_catalog.pg_proc as routine
       where routine.oid = v_candidate
     )
     or (
       select index_record.indisvalid is not true
         or index_record.indisready is not true
         or index_record.indisunique is true
         or index_record.indisprimary is true
         or pg_catalog.pg_get_expr(
           index_record.indpred,
           index_record.indrelid,
           true
         ) !~ 'dataset_kind = ''flow'''
         or pg_catalog.pg_get_expr(
           index_record.indpred,
           index_record.indrelid,
           true
         ) !~ 'length.*casNumber.*7.*12'
         or pg_catalog.pg_get_indexdef(v_index) !~
           'casNumber.*id.*version DESC.*modified_at DESC.*state_code DESC'
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid = v_index
     ) then
    raise exception 'Portal Flow CAS Search cutover drifted'
      using errcode = '55000';
  end if;

  v_summary := api.portal_catalog_summary_v1();
  select example.value
  into v_cas_example
  from pg_catalog.jsonb_array_elements(v_summary -> 'examples') as example(value)
  where example.value ->> 'queryKind' = 'cas';

  if v_cas_example is not null then
    v_search := private.portal_search_v1(
      'flow',
      v_cas_example ->> 'query', '{}'::jsonb, 'relevance', null, 20
    );
    if pg_catalog.jsonb_array_length(v_search -> 'items') = 0 then
      raise exception 'Portal summary CAS example is not executable'
        using errcode = '55000';
    end if;
  end if;
end
$verify_portal_flow_cas_search$;

reset role;
revoke portal_public_executor from postgres;

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();
reset role;
revoke api_internal_executor from postgres;

commit;
