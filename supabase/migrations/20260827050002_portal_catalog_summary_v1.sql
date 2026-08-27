-- Issue #533: expose one bounded anonymous catalog summary from the
-- synchronized Portal projections and the guarded narrow eligibility index.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_catalog_summary_prerequisite_guard$
begin
  if pg_catalog.to_regprocedure('api.portal_catalog_summary_v1()') is not null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_summary_label_v1(jsonb)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_summary_valid_cas_v1(text)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_projection_contract_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_facet_contract_v1()'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_summary_eligibility_v1_idx'
     ) is null
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'portal_public_executor'
         and not role.rolcanlogin
         and not role.rolinherit
         and not role.rolbypassrls
         and not role.rolsuper
     ) then
    raise exception 'Portal catalog summary prerequisites are unsafe'
      using errcode = '55000';
  end if;
end
$portal_catalog_summary_prerequisite_guard$;

grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set role portal_public_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

create function private.portal_catalog_summary_label_v1(p_card jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'language', label_item.language,
    'value', label_item.label_value
  ) order by label_item.preference,
      pg_catalog.lower(label_item.language) collate pg_catalog."C",
      label_item.label_value collate pg_catalog."C",
      label_item.ordinality), '[]'::jsonb)
  from (
    select
      pg_catalog.btrim(item.value ->> 'language') as language,
      pg_catalog.btrim(item.value ->> 'value') as label_value,
      item.ordinality,
      case pg_catalog.lower(pg_catalog.btrim(item.value ->> 'language'))
        when 'zh-cn' then 0
        when 'en' then 1
        else 2
      end as preference
    from pg_catalog.jsonb_array_elements(
      case pg_catalog.jsonb_typeof(p_card -> 'names')
        when 'array' then p_card -> 'names'
        else '[]'::jsonb
      end
    ) with ordinality as item(value, ordinality)
    where pg_catalog.jsonb_typeof(item.value) = 'object'
      and pg_catalog.jsonb_typeof(item.value -> 'language') = 'string'
      and pg_catalog.jsonb_typeof(item.value -> 'value') = 'string'
      and pg_catalog.btrim(item.value ->> 'language') ~
        '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
      and pg_catalog.length(
        pg_catalog.btrim(item.value ->> 'language')
      ) <= 35
      and nullif(pg_catalog.btrim(item.value ->> 'value'), '') is not null
      and pg_catalog.length(
        pg_catalog.btrim(item.value ->> 'value')
      ) <= 160
      and pg_catalog.octet_length(
        pg_catalog.btrim(item.value ->> 'value')
      ) <= 640
    order by preference,
      pg_catalog.lower(
        pg_catalog.btrim(item.value ->> 'language')
      ) collate pg_catalog."C",
      pg_catalog.btrim(item.value ->> 'value') collate pg_catalog."C",
      item.ordinality
    limit 2
  ) as label_item
$function$;

create function private.portal_catalog_summary_valid_cas_v1(p_value text)
returns boolean
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case
    when p_value ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      pg_catalog.right(p_value, 1)::integer = (
        select pg_catalog.mod(
          pg_catalog.sum(
            digit.value::integer * digit.ordinality::integer
          ),
          10
        )
        from pg_catalog.regexp_split_to_table(
          pg_catalog.reverse(pg_catalog.replace(
            pg_catalog.left(p_value, pg_catalog.length(p_value) - 2),
            '-',
            ''
          )),
          ''
        ) with ordinality as digit(value, ordinality)
      )
    else false
  end
$function$;

revoke all on function private.portal_catalog_summary_label_v1(jsonb)
  from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.portal_catalog_summary_valid_cas_v1(text)
  from public, anon, authenticated, service_role, api_internal_executor;

create function api.portal_catalog_summary_v1()
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '2s'
set work_mem = '32MB'
set plan_cache_mode = 'force_custom_plan'
set max_parallel_workers_per_gather = '0'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_counts jsonb;
  v_latest_modified_at text;
  v_uuid_example jsonb;
  v_cas_example jsonb;
  v_classification_example jsonb;
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
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
      from private.portal_catalog_search_rows_v1 as candidate
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
      from private.portal_catalog_search_rows_v1 as candidate
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
  ), cas_candidates as materialized (
    select candidate.dataset_kind,
      candidate.id,
      candidate.version,
      candidate.modified_at,
      candidate.state_code,
      candidate.card ->> 'casNumber' as cas_number,
      private.portal_catalog_summary_label_v1(candidate.card) as label
    from private.portal_catalog_search_rows_v1 as candidate
    join latest
      on latest.dataset_kind = candidate.dataset_kind
     and latest.id = candidate.id
     and latest.version = candidate.version
    where candidate.dataset_kind = 'flow'
      and pg_catalog.jsonb_typeof(candidate.card -> 'casNumber') = 'string'
      and candidate.card ->> 'casNumber' ~
        '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
      and private.portal_catalog_summary_valid_cas_v1(
        candidate.card ->> 'casNumber'
      )
      and pg_catalog.jsonb_array_length(
        private.portal_catalog_summary_label_v1(candidate.card)
      ) > 0
    order by candidate.id,
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
      from private.portal_catalog_search_rows_v1 as candidate
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
        and nullif(
          pg_catalog.btrim(classification.value ->> 'code'), ''
        ) is not null
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 128
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
      from private.portal_catalog_search_rows_v1 as candidate
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
        and nullif(
          pg_catalog.btrim(classification.value ->> 'code'), ''
        ) is not null
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 128
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
$function$;

comment on function api.portal_catalog_summary_v1() is
  'Bounded latest-visible public Process/Flow counts, timestamp, and deterministic executable R1 examples from synchronized Portal projections.';

revoke all on function api.portal_catalog_summary_v1()
  from public, anon, authenticated, service_role;
grant execute on function api.portal_catalog_summary_v1()
  to anon, authenticated;

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values (
  'api.portal_catalog_summary_v1()',
  'PORTAL-CATALOG-01',
  true,
  true,
  false
);

do $verify_portal_catalog_summary_v1$
declare
  v_routine regprocedure := 'api.portal_catalog_summary_v1()'::regprocedure;
  v_label regprocedure :=
    'private.portal_catalog_summary_label_v1(jsonb)'::regprocedure;
  v_valid_cas regprocedure :=
    'private.portal_catalog_summary_valid_cas_v1(text)'::regprocedure;
begin
  if (
    select not (
      routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 's'
      and routine.proparallel = 'r'
      and routine.prorettype = 'pg_catalog.jsonb'::regtype
      and coalesce(routine.proconfig, '{}'::text[]) @> array[
        'search_path=""',
        'statement_timeout=2s',
        'work_mem=32MB',
        'plan_cache_mode=force_custom_plan',
        'max_parallel_workers_per_gather=0',
        'jit=off',
        'row_security=on'
      ]::text[]
    )
    from pg_catalog.pg_proc as routine
    where routine.oid = v_routine
  ) is not false
  or exists (
    select 1
    from pg_catalog.pg_proc as helper
    where helper.oid in (v_label::oid, v_valid_cas::oid)
      and not (
        helper.proowner = 'portal_public_executor'::regrole
        and not helper.prosecdef
        and helper.provolatile = 'i'
        and helper.proparallel = 's'
        and coalesce(helper.proconfig, '{}'::text[]) @>
          array['search_path=""']::text[]
        and coalesce(helper.proacl::text, '') =
          '{portal_public_executor=X/portal_public_executor}'
      )
  )
  or (
    select count(*)
    from private.api_capability_grants as manifest
    where manifest.routine_identity = 'api.portal_catalog_summary_v1()'
      and manifest.capability_id = 'PORTAL-CATALOG-01'
      and manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ) <> 1
  or not pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
  or not pg_catalog.has_function_privilege(
    'authenticated', v_routine, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('service_role', v_routine, 'EXECUTE')
  or pg_catalog.has_function_privilege('anon', v_label, 'EXECUTE')
  or pg_catalog.has_function_privilege('authenticated', v_label, 'EXECUTE')
  or pg_catalog.has_function_privilege('service_role', v_label, 'EXECUTE')
  or pg_catalog.has_function_privilege(
    'api_internal_executor', v_label, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('anon', v_valid_cas, 'EXECUTE')
  or pg_catalog.has_function_privilege(
    'authenticated', v_valid_cas, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('service_role', v_valid_cas, 'EXECUTE')
  or pg_catalog.has_function_privilege(
    'api_internal_executor', v_valid_cas, 'EXECUTE'
  )
  or exists (
    select 1
    from pg_catalog.aclexplode(
      coalesce(
        (select routine.proacl from pg_catalog.pg_proc as routine
         where routine.oid = v_routine),
        pg_catalog.acldefault(
          'f',
          (select routine.proowner from pg_catalog.pg_proc as routine
           where routine.oid = v_routine)
        )
      )
    ) as acl
    where acl.grantee = 0
      and acl.privilege_type = 'EXECUTE'
  )
  or (
    select routine.prosrc !~ 'portal_catalog_facet_rows_v1'
      or routine.prosrc !~ 'portal_catalog_search_rows_v1'
      or routine.prosrc ~
        'public\.(processes|flows)|search_text|extracted_md|embedding_ft|team_id|user_id|review_id|privateLocator|objectLocator'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_routine
  ) then
    raise exception 'Portal catalog summary contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_catalog_summary_v1$;

commit;
