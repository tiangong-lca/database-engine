-- Issue #531: atomically route only normalized empty-query/empty-filter
-- Facets calls to the reconciled narrow projection. Every query or filter
-- request retains the established card-based implementation.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

create temporary table portal_facets_before_v1 (
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null,
  result_type oid not null,
  fallback_definition text not null
) on commit drop;

insert into portal_facets_before_v1 (
  owner_name,
  security_definer,
  proconfig,
  acl_text,
  result_type,
  fallback_definition
)
select
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, ''),
  routine.prorettype,
  pg_catalog.pg_get_functiondef(
    'private.catalog_portal_facets_v1_impl(text,text,uuid,text,jsonb,text)'::regprocedure
  )
from pg_catalog.pg_proc as routine
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner
where routine.oid = 'api.portal_facets_v1(text,text,jsonb)'::regprocedure;

do $portal_facet_cutover_prerequisite_guard$
begin
  if (select count(*) from portal_facets_before_v1) <> 1
     or not exists (
       select 1
       from pg_catalog.pg_roles
       where rolname = 'portal_public_executor'
         and not rolcanlogin
         and not rolbypassrls
         and not rolsuper
         and not rolreplication
     ) then
    raise exception 'Portal facet cutover prerequisite is unsafe'
      using errcode = '42501';
  end if;
end
$portal_facet_cutover_prerequisite_guard$;

grant api_internal_executor to postgres;
set role api_internal_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

do $portal_facet_cutover_parity_guard$
begin
  if (
    select count(*)
    from private.portal_catalog_facet_rows_v1
    where dataset_kind = 'process'
  ) <> (
    select count(*)
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
  ) or (
    select count(*)
    from private.portal_catalog_facet_rows_v1
    where dataset_kind = 'flow'
  ) <> (
    select count(*)
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'flow'
  ) then
    raise exception 'Portal facet cutover parity guard failed'
      using errcode = '55000';
  end if;
end
$portal_facet_cutover_parity_guard$;

reset role;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set role portal_public_executor;

create function private.catalog_portal_facets_empty_v1_impl(
  p_kind text,
  p_query_fingerprint text
)
returns jsonb
language sql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set work_mem = '32MB'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
  with latest as materialized (
    select distinct on (facet.dataset_kind, facet.id)
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.facet_access_level,
      facet.facet_geography,
      facet.facet_reference_year,
      facet.facet_process_subtype,
      facet.facet_source
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.facet_contract_version = 1
      and (p_kind = 'all' or facet.dataset_kind = p_kind)
    order by facet.dataset_kind,
      facet.id,
      facet.version desc,
      facet.modified_at desc,
      facet.state_code desc
  ), facts as materialized (
    select latest.dataset_kind,
      latest.facet_access_level,
      latest.facet_geography,
      latest.facet_reference_year,
      case when latest.dataset_kind = 'process' then
        latest.facet_process_subtype
      else null::text end as facet_process_subtype,
      latest.facet_source
    from latest
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
    'schemaVersion', 'portal.public-facets.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'groups', groups.value
  )
  from groups
$function$;

comment on function private.catalog_portal_facets_empty_v1_impl(text, text) is
  'Empty-query, empty-filter Portal facets over the narrow latest-visible fact projection.';

revoke all on function private.catalog_portal_facets_empty_v1_impl(text, text)
from public, anon, authenticated, service_role, api_internal_executor;

create or replace function api.portal_facets_v1(
  p_kind text,
  p_query text,
  p_filters jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
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
$function$;

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

do $verify_portal_facet_cutover$
declare
  v_api regprocedure := 'api.portal_facets_v1(text,text,jsonb)'::regprocedure;
  v_fast regprocedure :=
    'private.catalog_portal_facets_empty_v1_impl(text,text)'::regprocedure;
  v_fallback regprocedure :=
    'private.catalog_portal_facets_v1_impl(text,text,uuid,text,jsonb,text)'::regprocedure;
begin
  if exists (
    select 1
    from portal_facets_before_v1 as before
    join pg_catalog.pg_proc as routine
      on routine.oid = v_api
    join pg_catalog.pg_roles as owner_role
      on owner_role.oid = routine.proowner
    where before.owner_name <> owner_role.rolname
       or before.security_definer <> routine.prosecdef
       or before.proconfig <> coalesce(routine.proconfig, '{}'::text[])
       or before.acl_text <> coalesce(routine.proacl::text, '')
       or before.result_type <> routine.prorettype
       or before.fallback_definition <>
         pg_catalog.pg_get_functiondef(v_fallback)
  ) or exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid = v_fast
      and not (
        routine.proowner = 'portal_public_executor'::regrole
        and routine.prosecdef
        and routine.provolatile = 's'
        and routine.proparallel = 'r'
        and coalesce(routine.proconfig, '{}'::text[]) @> array[
          'search_path=""',
          'statement_timeout=8s',
          'work_mem=32MB',
          'plan_cache_mode=force_custom_plan',
          'row_security=on'
        ]::text[]
        and coalesce(routine.proacl::text, '') =
          '{portal_public_executor=X/portal_public_executor}'
      )
  ) or pg_catalog.has_function_privilege(
    'anon', v_fast, 'EXECUTE'
  ) or pg_catalog.has_function_privilege(
    'authenticated', v_fast, 'EXECUTE'
  ) or pg_catalog.has_function_privilege(
    'service_role', v_fast, 'EXECUTE'
  ) or pg_catalog.has_function_privilege(
    'api_internal_executor', v_fast, 'EXECUTE'
  ) or (
    select routine.prosrc !~ 'portal_catalog_facet_rows_v1'
      or routine.prosrc ~
        'portal_catalog_search_rows_v1|\.card|public\.processes|public\.flows'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_fast
  ) or (
    select routine.prosrc !~
        $$v_query = '' and v_filters = '{}'::jsonb$$
      or routine.prosrc !~ 'catalog_portal_facets_empty_v1_impl'
      or routine.prosrc !~ 'catalog_portal_facets_v1_impl'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_api
  ) then
    raise exception 'Portal facet cutover contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_cutover$;

commit;
