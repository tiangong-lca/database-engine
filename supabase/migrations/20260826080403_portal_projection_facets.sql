-- Issue #531: keep the public facets signature, DTO, metadata, and error
-- contract compatible while moving full-result aggregation onto the
-- synchronized public-safe projection.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '15min';

create temporary table portal_projection_facets_before (
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null,
  result_type oid not null
) on commit drop;

insert into portal_projection_facets_before (
  owner_name,
  security_definer,
  proconfig,
  acl_text,
  result_type
)
select
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, ''),
  routine.prorettype
from pg_catalog.pg_proc as routine
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner
where routine.oid = 'api.portal_facets_v1(text,text,jsonb)'::regprocedure;

do $portal_projection_facets_role_guard$
begin
  if (select count(*) from portal_projection_facets_before) <> 1
     or not exists (
       select 1
       from pg_catalog.pg_roles
       where rolname = 'portal_public_executor'
         and not rolcanlogin
         and not rolbypassrls
         and not rolsuper
         and not rolreplication
     ) then
    raise exception 'Portal facets prerequisite is missing or unsafe'
      using errcode = '42501';
  end if;
end
$portal_projection_facets_role_guard$;

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
reset role;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set role portal_public_executor;

create or replace function private.catalog_portal_facet_candidate_rows_v1(
  p_kind text,
  p_query text,
  p_exact_id uuid,
  p_like_pattern text
)
returns table(
  dataset_kind text,
  id uuid,
  version text,
  card jsonb
)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
begin
  if p_kind = 'process' then
    return query
    select 'process'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v1(
      'process', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  elsif p_kind = 'flow' then
    return query
    select 'flow'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v1(
      'flow', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  elsif p_kind = 'all' then
    return query
    select 'process'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v1(
      'process', p_query, p_exact_id, p_like_pattern
    ) as candidate;
    return query
    select 'flow'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v1(
      'flow', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  end if;
end
$function$;

comment on function private.catalog_portal_facet_candidate_rows_v1(
  text, text, uuid, text
) is
  'Exact Process/Flow/all latest-visible facet candidates over the synchronized public-safe projection.';

revoke all on function private.catalog_portal_facet_candidate_rows_v1(
  text, text, uuid, text
) from public, anon, authenticated, service_role, api_internal_executor;

create or replace function private.catalog_portal_facets_v1_impl(
  p_kind text,
  p_query text,
  p_exact_id uuid,
  p_like_pattern text,
  p_filters jsonb,
  p_query_fingerprint text
)
returns jsonb
language sql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
  with matched as materialized (
    select candidate.*
    from private.catalog_portal_facet_candidate_rows_v1(
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
    'schemaVersion', 'portal.public-facets.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'groups', groups.value
  )
  from groups
$function$;

comment on function private.catalog_portal_facets_v1_impl(
  text, text, uuid, text, jsonb, text
) is
  'Full-result Portal facet aggregation over exact latest synchronized projection candidates.';

revoke all on function private.catalog_portal_facets_v1_impl(
  text, text, uuid, text, jsonb, text
) from public, anon, authenticated, service_role, api_internal_executor;

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

do $verify_portal_projection_facets$
declare
  v_api regprocedure := 'api.portal_facets_v1(text,text,jsonb)'::regprocedure;
  v_candidates regprocedure :=
    'private.catalog_portal_facet_candidate_rows_v1(text,text,uuid,text)'::regprocedure;
  v_impl regprocedure :=
    'private.catalog_portal_facets_v1_impl(text,text,uuid,text,jsonb,text)'::regprocedure;
begin
  if exists (
    select 1
    from portal_projection_facets_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = v_api
    join pg_catalog.pg_roles as owner_role
      on owner_role.oid = routine.proowner
    where before.owner_name <> owner_role.rolname
       or before.security_definer <> routine.prosecdef
       or before.proconfig <> coalesce(routine.proconfig, '{}'::text[])
       or before.acl_text <> coalesce(routine.proacl::text, '')
       or before.result_type <> routine.prorettype
  ) then
    raise exception 'Portal facets external metadata drifted';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid = any (array[v_candidates::oid, v_impl::oid])
      and not (
        routine.proowner = 'portal_public_executor'::regrole
        and routine.prosecdef
        and coalesce(routine.proconfig, '{}'::text[]) @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan',
          'row_security=on'
        ]::text[]
        and coalesce(routine.proacl::text, '')
          = '{portal_public_executor=X/portal_public_executor}'
      )
  ) then
    raise exception 'Portal facets private owner/config/ACL mismatch';
  end if;

  if (
    select routine.prosrc !~ 'catalog_portal_facets_v1_impl'
      or routine.prosrc ~ 'public\.processes|public\.flows|portal_catalog_rows_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_api
  ) or (
    select routine.prosrc !~ 'catalog_portal_candidate_rows_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_candidates
  ) or (
    select routine.prosrc !~ 'catalog_portal_facet_candidate_rows_v1'
      or routine.prosrc ~ 'public\.processes|public\.flows|portal_catalog_rows_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_impl
  ) then
    raise exception 'Portal facets cutover does not use projection candidates';
  end if;
end
$verify_portal_projection_facets$;

do $verify_portal_projection_memberships$
begin
  if exists (
    with expected(role_name, member_name, admin_option, inherit_option, set_option) as (
      values
        (
          'api_internal_executor'::text,
          'postgres'::text,
          true,
          false,
          false
        ),
        (
          'authenticated',
          'api_internal_executor',
          false,
          true,
          true
        ),
        (
          'portal_public_executor',
          'postgres',
          true,
          false,
          false
        )
    ), actual as (
      select role.rolname as role_name,
        member.rolname as member_name,
        membership.admin_option,
        membership.inherit_option,
        membership.set_option
      from pg_catalog.pg_auth_members as membership
      join pg_catalog.pg_roles as role
        on role.oid = membership.roleid
      join pg_catalog.pg_roles as member
        on member.oid = membership.member
      where membership.roleid in (
          'api_internal_executor'::regrole,
          'portal_public_executor'::regrole
        )
         or membership.member in (
           'api_internal_executor'::regrole,
           'portal_public_executor'::regrole
         )
    )
    select 1
    from (
      (select * from actual except select * from expected)
      union all
      (select * from expected except select * from actual)
    ) as difference
  ) then
    raise exception 'Portal projection executor membership model drifted'
      using errcode = '42501';
  end if;
end
$verify_portal_projection_memberships$;

commit;
